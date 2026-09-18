//! Shared search service for HTML and JSON. Rankings are short-lived snapshots;
//! each page rechecks live source eligibility before displaying story fields.
use crate::{home_page_story_from_row, search_page, AppState, HomePageStoryRow};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    Json,
};
use diesel::{
    sql_query,
    sql_types::{Array, BigInt, Double, Nullable, Text},
    QueryableByName,
};
use diesel_async::{AsyncConnection, RunQueryDsl};
use hn_core::embeddings::{EmbeddingClient, Workload};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

const PAGE_SIZE: usize = 20;
const TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SearchQuery {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub sort: Sort,
    #[serde(default = "first_page")]
    pub page: usize,
    pub session: Option<u64>,
    #[serde(default)]
    pub freshness: u8,
    #[serde(default)]
    pub votes: u8,
}
impl SearchQuery {
    pub fn tuning(&self) -> crate::search_tuning::Tuning {
        crate::search_tuning::Tuning {
            freshness: self.freshness,
            votes: self.votes,
        }
    }
}
fn first_page() -> usize {
    1
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Sort {
    #[default]
    Relevance,
    Score,
    Date,
}
impl Sort {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Relevance => "relevance",
            Self::Score => "score",
            Self::Date => "date",
        }
    }
}

#[derive(Clone)]
struct Ranking {
    query: String,
    sort: Sort,
    ids: Vec<i64>,
    ranks: HashMap<i64, RankDetails>,
    mode: &'static str,
    created: Instant,
}

/// Bounded process-local snapshots. Locks protect only bookkeeping, never HTTP/SQL.
#[derive(Default)]
pub struct SearchCache {
    next_id: u64,
    entries: HashMap<u64, Ranking>,
}

#[derive(Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub sort: Sort,
    pub page: usize,
    pub session: u64,
    pub tuning: crate::search_tuning::Tuning,
    pub retrieval_mode: &'static str,
    pub has_more: bool,
    pub results: Vec<crate::home_page::HomePageStory>,
    pub ranks: HashMap<i64, RankDetails>,
}

#[derive(Debug)]
pub struct SearchError(pub StatusCode, pub &'static str);
impl From<diesel::result::Error> for SearchError {
    fn from(error: diesel::result::Error) -> Self {
        tracing::error!(%error, "search database request failed");
        Self(
            StatusCode::SERVICE_UNAVAILABLE,
            "Search is temporarily unavailable. Please try again.",
        )
    }
}

/// HTML uses the same retrieval service as API clients, with a complete GET page
/// as the baseline so query URLs, refresh, and browser history work without scripts.
pub async fn html(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SearchQuery>,
) -> (StatusCode, Html<String>) {
    if query.q.trim().is_empty() {
        return (
            StatusCode::OK,
            Html(search_page::render(&query, None, None)),
        );
    }
    match run(&state, &query).await {
        Ok(result) => (
            StatusCode::OK,
            Html(search_page::render(&query, Some(&result), None)),
        ),
        Err(error) => (
            error.0,
            Html(search_page::render(&query, None, Some(error.1))),
        ),
    }
}
pub async fn json(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, (StatusCode, Json<crate::ErrorResponse>)> {
    run(&state, &query)
        .await
        .map(Json)
        .map_err(|e| (e.0, Json(crate::ErrorResponse { error: e.1.into() })))
}

/// Create a ranking once, or retrieve its explicitly named snapshot. Expiration
/// never silently recomputes page two: that could duplicate or skip stories.
pub async fn run(
    state: &Arc<AppState>,
    query: &SearchQuery,
) -> Result<SearchResponse, SearchError> {
    let text = query.q.trim();
    if text.is_empty()
        || text.len() > 2048
        || !(1..=10).contains(&query.page)
        || !query.tuning().valid()
    {
        return Err(SearchError(
            StatusCode::BAD_REQUEST,
            "Use a query of 1–2048 bytes, page 1–10, and boost values 0–100.",
        ));
    }
    let cached = {
        let mut cache = state.search_cache.lock().map_err(|_| {
            SearchError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Search is temporarily unavailable.",
            )
        })?;
        cache.entries.retain(|_, r| r.created.elapsed() < TTL);
        query
            .session
            .and_then(|id| cache.entries.get(&id).cloned().map(|r| (id, r)))
    };
    let (session, mut ranking) = if let Some((id, ranking)) = cached {
        if ranking.query != text || ranking.sort != query.sort {
            return Err(SearchError(
                StatusCode::BAD_REQUEST,
                "Search settings changed. Start a new search.",
            ));
        }
        (id, ranking)
    } else {
        if query.session.is_some() || query.page != 1 {
            return Err(SearchError(
                StatusCode::GONE,
                "This search has expired. Search again to refresh the results.",
            ));
        }
        let ranking = rank(state, text, query.sort).await?;
        let mut cache = state.search_cache.lock().map_err(|_| {
            SearchError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Search is temporarily unavailable.",
            )
        })?;
        if cache.entries.len() >= 128 {
            if let Some(oldest) = cache
                .entries
                .iter()
                .min_by_key(|(_, r)| r.created)
                .map(|(id, _)| *id)
            {
                cache.entries.remove(&oldest);
            }
        }
        cache.next_id += 1;
        let id = cache.next_id;
        cache.entries.insert(id, ranking.clone());
        (id, ranking)
    };
    for detail in ranking.ranks.values_mut() {
        detail.tuned = query
            .tuning()
            .score(detail.rrf, detail.freshness, detail.popularity);
    }
    if query.sort == Sort::Relevance {
        ranking.ids.sort_by(|a, b| {
            ranking.ranks[b]
                .tuned
                .total_cmp(&ranking.ranks[a].tuned)
                .then_with(|| a.cmp(b))
        });
    }
    let offset = (query.page - 1) * PAGE_SIZE;
    let ids: Vec<_> = ranking
        .ids
        .iter()
        .skip(offset)
        .take(PAGE_SIZE)
        .copied()
        .collect();
    let results = if ids.is_empty() {
        Vec::new()
    } else {
        let mut conn = state.pool.get().await.map_err(|_| {
            SearchError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Database connection unavailable.",
            )
        })?;
        sql_query("SELECT i.id,i.title,i.url,i.domain,i.by,i.time,i.score,i.descendants FROM unnest($1::bigint[]) WITH ORDINALITY r(id,ord) JOIN items i ON i.id=r.id WHERE public.story_search_eligible(i) ORDER BY r.ord")
            .bind::<Array<BigInt>, _>(ids).load::<HomePageStoryRow>(&mut conn).await?
            .into_iter().map(home_page_story_from_row).collect()
    };
    Ok(SearchResponse {
        query: text.into(),
        sort: query.sort,
        page: query.page,
        session,
        tuning: query.tuning(),
        retrieval_mode: ranking.mode,
        has_more: ranking.ids.len() > offset + PAGE_SIZE,
        ranks: results
            .iter()
            .map(|story| (story.id, ranking.ranks[&story.id].clone()))
            .collect(),
        results,
    })
}

#[derive(QueryableByName)]
struct Recipe {
    #[diesel(sql_type = Nullable<Text>)]
    recipe: Option<String>,
}
#[derive(QueryableByName)]
struct Hit {
    #[diesel(sql_type = BigInt)]
    story_id: i64,
}

/// Embedding HTTP runs before acquiring the ranking connection. A validated
/// proxy failure yields an explicitly keyword-only snapshot for all its pages.
async fn rank(state: &Arc<AppState>, query: &str, sort: Sort) -> Result<Ranking, SearchError> {
    let vector = if let Some(endpoint) = &state.embedding_base_url {
        let recipe = {
            let mut conn = state.pool.get().await.map_err(|_| {
                SearchError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Database connection unavailable.",
                )
            })?;
            sql_query(
                "SELECT obj_description('public.story_search'::regclass,'pg_class') AS recipe",
            )
            .get_result::<Recipe>(&mut conn)
            .await?
            .recipe
        };
        let recipe = recipe.ok_or(SearchError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Search index configuration is unavailable.",
        ))?;
        match EmbeddingClient::new(endpoint, recipe, Duration::from_secs(10)) {
            Ok(client) => match client
                .embed(&[query.to_owned()], Workload::Interactive)
                .await
            {
                Ok(vectors) => Some(vectors[0].to_pg_text()),
                Err(error) => {
                    tracing::warn!(%error, "keyword-only fallback");
                    None
                }
            },
            Err(error) => {
                tracing::warn!(%error, "keyword-only fallback");
                None
            }
        }
    } else {
        None
    };
    let mode = if vector.is_some() {
        "hybrid"
    } else {
        "keyword-only"
    };
    let mut conn = state.pool.get().await.map_err(|_| {
        SearchError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Database connection unavailable.",
        )
    })?;
    let query = query.to_owned();
    let ranked_query = query.clone();
    let (ids, ranks) = conn.transaction::<(Vec<i64>, HashMap<i64, RankDetails>), diesel::result::Error, _>(|conn| Box::pin(async move {
        sql_query("SET LOCAL statement_timeout = '15s'").execute(conn).await?;
        sql_query("SET LOCAL hnsw.ef_search = 1000").execute(conn).await?;
        let dense = if let Some(vector) = &vector {
            sql_query("WITH candidates AS MATERIALIZED (SELECT story_id,embedding <=> $1::halfvec(1024) AS distance FROM story_search WHERE embedding IS NOT NULL ORDER BY embedding <=> $1::halfvec(1024) LIMIT 100) SELECT c.story_id FROM candidates c JOIN items i ON i.id=c.story_id WHERE public.story_search_eligible(i) ORDER BY c.distance,c.story_id")
                .bind::<Text, _>(vector).load::<Hit>(conn).await?
        } else { Vec::new() };
        let lexical = sql_query("WITH candidates AS MATERIALIZED (SELECT s.story_id,s.title <@> to_bm25query($1,'story_search_title_bm25') AS distance FROM story_search s JOIN items i ON i.id=s.story_id WHERE public.story_search_eligible(i) ORDER BY s.title <@> to_bm25query($1,'story_search_title_bm25') LIMIT 100) SELECT story_id FROM candidates WHERE distance < 0 ORDER BY distance,story_id")
            .bind::<Text, _>(ranked_query).load::<Hit>(conn).await?;
        let (ids, mut ranks) = fuse(&dense.iter().map(|h| h.story_id).collect::<Vec<_>>(), &lexical.iter().map(|h| h.story_id).collect::<Vec<_>>());
        // Score only the bounded fused candidate set, including BM25-only hits.
        // This is cosine similarity (1 - distance), not a rank-derived estimate.
        if let Some(vector) = &vector {
            if !ids.is_empty() {
                let similarities = sql_query("SELECT story_id, 1 - (embedding <=> $1::halfvec(1024)) AS cosine FROM story_search WHERE story_id=ANY($2)")
                    .bind::<Text, _>(vector).bind::<Array<BigInt>, _>(&ids).load::<Similarity>(conn).await?;
                for hit in similarities { ranks.get_mut(&hit.story_id).expect("candidate has RRF details").cosine = hit.cosine; }
            }
        }
        if !ids.is_empty() {
            let signals = sql_query("SELECT id AS story_id,time,score FROM items WHERE id=ANY($1)")
                .bind::<Array<BigInt>, _>(&ids).load::<StorySignals>(conn).await?;
            let now = crate::home_page::unix_now_seconds();
            for story in signals {
                let detail = ranks.get_mut(&story.story_id).expect("candidate has rank");
                (detail.freshness, detail.popularity) = crate::search_tuning::signals(story.time, story.score, now);
            }
        }
        if ids.is_empty() || sort == Sort::Relevance { return Ok((ids, ranks)); }
        let order = if sort == Sort::Score { "i.score DESC NULLS LAST,i.id DESC" } else { "i.time DESC NULLS LAST,i.id DESC" };
        let ids = sql_query(format!("SELECT i.id AS story_id FROM items i WHERE i.id=ANY($1) ORDER BY {order}"))
            .bind::<Array<BigInt>, _>(ids).load::<Hit>(conn).await?.into_iter().map(|h| h.story_id).collect();
        Ok((ids, ranks))
    })).await?;
    Ok(Ranking {
        query,
        sort,
        ids,
        ranks,
        mode,
        created: Instant::now(),
    })
}

/// Branch positions used by RRF, retained with the snapshot even when display
/// order changes to points/date. Missing branches contribute zero, not rank zero.
#[derive(Clone, Default, Serialize)]
pub struct RankDetails {
    pub vector_rank: Option<usize>,
    pub bm25_rank: Option<usize>,
    pub rrf: f64,
    pub cosine: Option<f64>,
    pub freshness: f64,
    pub popularity: f64,
    pub tuned: f64,
}
#[derive(QueryableByName)]
struct StorySignals {
    #[diesel(sql_type = BigInt)]
    story_id: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    time: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    score: Option<i64>,
}
#[derive(QueryableByName)]
struct Similarity {
    #[diesel(sql_type = BigInt)]
    story_id: i64,
    #[diesel(sql_type = Nullable<Double>)]
    cosine: Option<f64>,
}
impl RankDetails {
    pub fn inline_label(&self) -> String {
        let cosine = self
            .cosine
            .map_or_else(|| "—".into(), |value| format!("{value:.3}"));
        format!(
            " | Cosine {cosine} ・RRF {:.1} ・Tuned {:.1}",
            self.rrf * 1000.0,
            self.tuned * 1000.0
        )
    }
}

/// Paper's weighted RRF: absent branches contribute zero; ties use story ID.
fn fuse(dense: &[i64], lexical: &[i64]) -> (Vec<i64>, HashMap<i64, RankDetails>) {
    let mut scores = HashMap::<i64, RankDetails>::new();
    for (ids, weight, vector) in [(dense, 1.0, true), (lexical, 0.125, false)] {
        for (rank, id) in ids.iter().enumerate() {
            let detail = scores.entry(*id).or_default();
            if vector {
                detail.vector_rank = Some(rank + 1);
            } else {
                detail.bm25_rank = Some(rank + 1);
            }
            detail.rrf += weight / (61 + rank) as f64;
        }
    }
    let mut ranked: Vec<_> = scores.keys().copied().collect();
    ranked.sort_by(|a, b| {
        scores[b]
            .rrf
            .total_cmp(&scores[a].rrf)
            .then_with(|| a.cmp(b))
    });
    (ranked, scores)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tuning_query_uses_numeric_url_parameters() {
        let uri = "/api/search?q=iphone&freshness=50&votes=25"
            .parse()
            .unwrap();
        let Query(query) = Query::<SearchQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(query.freshness, 50);
        assert_eq!(query.votes, 25);
        assert!(query.tuning().valid());
    }
    #[test]
    fn fusion_respects_weights_and_deduplicates() {
        let (ids, ranks) = fuse(&[2, 1], &[1, 3]);
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(ranks[&1].vector_rank, Some(2));
        assert_eq!(ranks[&1].bm25_rank, Some(1));
        assert!((ranks[&1].rrf - (1.0 / 62.0 + 0.125 / 61.0)).abs() < 1e-12);
        assert_eq!(ranks[&3].vector_rank, None);
        assert_eq!(fuse(&[], &[3, 2]).0, vec![3, 2]);
        assert!(fuse(&[], &[]).0.is_empty());
    }
}
