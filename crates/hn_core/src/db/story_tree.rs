use std::collections::{HashMap, HashSet};
use std::fmt;

use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Nullable, Text};
use diesel::QueryableByName;
use serde::Serialize;

/// Tuning knobs for story-thread retrieval.
///
/// These options are intentionally small at first; they represent behavior that affects
/// tree shape and caller-visible output semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoryTreeOptions {
    pub include_deleted: bool,
    pub include_dead: bool,
}

impl Default for StoryTreeOptions {
    fn default() -> Self {
        Self {
            include_deleted: true,
            include_dead: true,
        }
    }
}

/// Lightweight row shape used by thread-retrieval code.
///
/// This is intentionally decoupled from Diesel model derives so the tree code can be tested
/// as pure data transformations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoryTreeItem {
    pub id: i64,
    pub parent: Option<i64>,
    pub type_: Option<String>,
    pub by: Option<String>,
    pub time: Option<i64>,
    pub text: Option<String>,
    pub title: Option<String>,
    pub deleted: Option<bool>,
    pub dead: Option<bool>,
    pub story_id: Option<i64>,
}

/// Parent->child edge with explicit sibling order from the `kids` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KidEdge {
    pub item: i64,
    pub kid: i64,
    pub display_order: Option<i64>,
}

/// Complete in-memory snapshot needed to build one story tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoryThreadSnapshot {
    pub story: Option<StoryTreeItem>,
    pub comments: Vec<StoryTreeItem>,
    pub kid_edges: Vec<KidEdge>,
}

/// A single comment node in the final nested story tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommentTreeNode {
    pub item: StoryTreeItem,
    pub children: Vec<CommentTreeNode>,
}

/// Describes a non-fatal graph issue found while constructing the tree.
///
/// The retrieval path should prefer returning partial but well-formed trees plus explicit
/// break records, instead of hard-failing on every data inconsistency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphBreak {
    pub reason: GraphBreakReason,
    pub parent_id: Option<i64>,
    pub child_id: Option<i64>,
}

/// High-level categories for edge/data inconsistencies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum GraphBreakReason {
    MissingParent,
    MissingChild,
    CycleDetected,
    EdgePointsOutsideStory,
}

/// Final story-tree payload consumed by API/HTML render paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoryCommentTree {
    pub story: StoryTreeItem,
    pub roots: Vec<CommentTreeNode>,
    pub graph_breaks: Vec<GraphBreak>,
}

/// Dependency type for retrieval failures that originate outside tree assembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendDependency {
    Database,
    Network,
}

/// Retryability classification for backend failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendFailureClass {
    Transient,
    Permanent,
}

/// Backend I/O failure envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoryTreeBackendError {
    pub dependency: BackendDependency,
    pub class: BackendFailureClass,
    pub message: String,
}

impl StoryTreeBackendError {
    pub fn transient(dependency: BackendDependency, message: impl Into<String>) -> Self {
        Self {
            dependency,
            class: BackendFailureClass::Transient,
            message: message.into(),
        }
    }

    pub fn permanent(dependency: BackendDependency, message: impl Into<String>) -> Self {
        Self {
            dependency,
            class: BackendFailureClass::Permanent,
            message: message.into(),
        }
    }
}

/// Top-level retrieval errors visible to callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetrieveStoryTreeError {
    StoryNotFound {
        story_id: i64,
    },
    NotAStory {
        requested_story_id: i64,
        actual_type: Option<String>,
    },
    Backend(StoryTreeBackendError),
}

impl fmt::Display for RetrieveStoryTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoryNotFound { story_id } => write!(f, "story {story_id} was not found"),
            Self::NotAStory {
                requested_story_id,
                actual_type,
            } => write!(
                f,
                "item {requested_story_id} is not a story (type: {})",
                actual_type.as_deref().unwrap_or("<null>")
            ),
            Self::Backend(err) => write!(
                f,
                "backend failure ({:?}, {:?}): {}",
                err.dependency, err.class, err.message
            ),
        }
    }
}

impl std::error::Error for RetrieveStoryTreeError {}

/// Backend interface used by retrieval orchestration.
///
/// This indirection keeps tree-assembly logic testable and lets callers inject alternate
/// stores for testing and operational fallbacks.
pub trait StoryTreeBackend {
    fn load_story_snapshot(
        &mut self,
        story_id: i64,
        options: &StoryTreeOptions,
    ) -> Result<StoryThreadSnapshot, StoryTreeBackendError>;
}

/// Postgres-backed implementation for story snapshot loading.
pub struct PgStoryTreeBackend<'a> {
    conn: &'a mut PgConnection,
}

impl<'a> PgStoryTreeBackend<'a> {
    pub fn new(conn: &'a mut PgConnection) -> Self {
        Self { conn }
    }
}

impl StoryTreeBackend for PgStoryTreeBackend<'_> {
    fn load_story_snapshot(
        &mut self,
        story_id: i64,
        _options: &StoryTreeOptions,
    ) -> Result<StoryThreadSnapshot, StoryTreeBackendError> {
        let story = sql_query(
            r#"
            SELECT
                id,
                parent,
                type AS type_,
                by,
                time,
                text,
                title,
                deleted,
                dead,
                story_id
            FROM items
            WHERE id = $1
            "#,
        )
        .bind::<BigInt, _>(story_id)
        .load::<StoryTreeItemRow>(self.conn)
        .map_err(database_backend_error)?
        .into_iter()
        .next()
        .map(StoryTreeItemRow::into_item);

        if !should_load_descendants(story.as_ref()) {
            return Ok(StoryThreadSnapshot {
                story,
                comments: Vec::new(),
                kid_edges: Vec::new(),
            });
        }

        let comments = sql_query(
            r#"
            SELECT
                id,
                parent,
                type AS type_,
                by,
                time,
                text,
                title,
                deleted,
                dead,
                story_id
            FROM items
            WHERE story_id = $1
              AND type = 'comment'
            ORDER BY id
            "#,
        )
        .bind::<BigInt, _>(story_id)
        .load::<StoryTreeItemRow>(self.conn)
        .map_err(database_backend_error)?
        .into_iter()
        .map(StoryTreeItemRow::into_item)
        .collect::<Vec<_>>();

        let kid_edges = sql_query(
            r#"
            WITH parents AS (
                SELECT $1::bigint AS item
                UNION ALL
                SELECT i.id
                FROM items i
                WHERE i.story_id = $1
                  AND i.type = 'comment'
            )
            SELECT
                k.item,
                k.kid,
                k.display_order
            FROM kids k
            JOIN parents p ON p.item = k.item
            ORDER BY k.item, k.display_order NULLS LAST, k.kid
            "#,
        )
        .bind::<BigInt, _>(story_id)
        .load::<KidEdgeRow>(self.conn)
        .map_err(database_backend_error)?
        .into_iter()
        .map(KidEdgeRow::to_edge)
        .collect::<Vec<_>>();

        Ok(StoryThreadSnapshot {
            story,
            comments,
            kid_edges,
        })
    }
}

/// Async-Postgres helper for snapshot retrieval tuned for index usage.
///
/// Query stages:
/// 1. Root row by PK (`items.id`).
/// 2. Story-scoped comments via `story_id` partial index (`type='comment'`).
/// 3. Parent-child edges by joining `kids` against the parent id set.
pub async fn load_story_snapshot_async_pg(
    conn: &mut diesel_async::AsyncPgConnection,
    story_id: i64,
    _options: &StoryTreeOptions,
) -> Result<StoryThreadSnapshot, StoryTreeBackendError> {
    let story = diesel_async::RunQueryDsl::load(
        sql_query(
            r#"
            SELECT
                id,
                parent,
                type AS type_,
                by,
                time,
                text,
                title,
                deleted,
                dead,
                story_id
            FROM items
            WHERE id = $1
            "#,
        )
        .bind::<BigInt, _>(story_id),
        conn,
    )
    .await
    .map_err(database_backend_error)?
    .into_iter()
    .next()
    .map(StoryTreeItemRow::into_item);

    if !should_load_descendants(story.as_ref()) {
        return Ok(StoryThreadSnapshot {
            story,
            comments: Vec::new(),
            kid_edges: Vec::new(),
        });
    }

    let comments = diesel_async::RunQueryDsl::load(
        sql_query(
            r#"
            SELECT
                id,
                parent,
                type AS type_,
                by,
                time,
                text,
                title,
                deleted,
                dead,
                story_id
            FROM items
            WHERE story_id = $1
              AND type = 'comment'
            ORDER BY id
            "#,
        )
        .bind::<BigInt, _>(story_id),
        conn,
    )
    .await
    .map_err(database_backend_error)?
    .into_iter()
    .map(StoryTreeItemRow::into_item)
    .collect::<Vec<_>>();

    let kid_edges = diesel_async::RunQueryDsl::load(
        sql_query(
            r#"
            WITH parents AS (
                SELECT $1::bigint AS item
                UNION ALL
                SELECT i.id
                FROM items i
                WHERE i.story_id = $1
                  AND i.type = 'comment'
            )
            SELECT
                k.item,
                k.kid,
                k.display_order
            FROM kids k
            JOIN parents p ON p.item = k.item
            ORDER BY k.item, k.display_order NULLS LAST, k.kid
            "#,
        )
        .bind::<BigInt, _>(story_id),
        conn,
    )
    .await
    .map_err(database_backend_error)?
    .into_iter()
    .map(KidEdgeRow::to_edge)
    .collect::<Vec<_>>();

    Ok(StoryThreadSnapshot {
        story,
        comments,
        kid_edges,
    })
}

#[cfg(any(test, feature = "sqlite-tests"))]
use diesel::sqlite::SqliteConnection;

/// SQLite-backed implementation that mirrors the index-friendly retrieval shape.
#[cfg(any(test, feature = "sqlite-tests"))]
pub struct SqliteStoryTreeBackend<'a> {
    conn: &'a mut SqliteConnection,
}

#[cfg(any(test, feature = "sqlite-tests"))]
impl<'a> SqliteStoryTreeBackend<'a> {
    pub fn new(conn: &'a mut SqliteConnection) -> Self {
        Self { conn }
    }
}

#[cfg(any(test, feature = "sqlite-tests"))]
impl StoryTreeBackend for SqliteStoryTreeBackend<'_> {
    fn load_story_snapshot(
        &mut self,
        story_id: i64,
        _options: &StoryTreeOptions,
    ) -> Result<StoryThreadSnapshot, StoryTreeBackendError> {
        let story = sql_query(
            r#"
            SELECT
                id,
                parent,
                type AS type_,
                by,
                time,
                text,
                title,
                deleted,
                dead,
                story_id
            FROM items
            WHERE id = ?
            "#,
        )
        .bind::<BigInt, _>(story_id)
        .load::<StoryTreeItemRow>(self.conn)
        .map_err(database_backend_error)?
        .into_iter()
        .next()
        .map(StoryTreeItemRow::into_item);

        if !should_load_descendants(story.as_ref()) {
            return Ok(StoryThreadSnapshot {
                story,
                comments: Vec::new(),
                kid_edges: Vec::new(),
            });
        }

        let comments = sql_query(
            r#"
            SELECT
                id,
                parent,
                type AS type_,
                by,
                time,
                text,
                title,
                deleted,
                dead,
                story_id
            FROM items
            WHERE story_id = ?
              AND type = 'comment'
            ORDER BY id
            "#,
        )
        .bind::<BigInt, _>(story_id)
        .load::<StoryTreeItemRow>(self.conn)
        .map_err(database_backend_error)?
        .into_iter()
        .map(StoryTreeItemRow::into_item)
        .collect::<Vec<_>>();

        let kid_edges = sql_query(
            r#"
            WITH parents AS (
                SELECT ? AS item
                UNION ALL
                SELECT i.id
                FROM items i
                WHERE i.story_id = ?
                  AND i.type = 'comment'
            )
            SELECT
                k.item,
                k.kid,
                k.display_order
            FROM kids k
            JOIN parents p ON p.item = k.item
            ORDER BY k.item, k.display_order, k.kid
            "#,
        )
        .bind::<BigInt, _>(story_id)
        .bind::<BigInt, _>(story_id)
        .load::<KidEdgeRow>(self.conn)
        .map_err(database_backend_error)?
        .into_iter()
        .map(KidEdgeRow::to_edge)
        .collect::<Vec<_>>();

        Ok(StoryThreadSnapshot {
            story,
            comments,
            kid_edges,
        })
    }
}

fn should_load_descendants(story: Option<&StoryTreeItem>) -> bool {
    matches!(story, Some(item) if item.type_.as_deref() == Some("story"))
}

/// Retrieves one story's comment tree using a backend snapshot provider.
///
/// This function performs backend retrieval and delegates pure graph construction to
/// [`assemble_story_comment_tree`].
pub fn retrieve_story_comments_as_tree(
    backend: &mut dyn StoryTreeBackend,
    story_id: i64,
    options: StoryTreeOptions,
) -> Result<StoryCommentTree, RetrieveStoryTreeError> {
    let snapshot = backend
        .load_story_snapshot(story_id, &options)
        .map_err(RetrieveStoryTreeError::Backend)?;
    assemble_story_comment_tree(snapshot, story_id, options)
}

/// Async convenience wrapper for Axum/HTTP runtimes using `diesel_async`.
pub async fn retrieve_story_comments_as_tree_async_pg(
    conn: &mut diesel_async::AsyncPgConnection,
    story_id: i64,
    options: StoryTreeOptions,
) -> Result<StoryCommentTree, RetrieveStoryTreeError> {
    let snapshot = load_story_snapshot_async_pg(conn, story_id, &options)
        .await
        .map_err(RetrieveStoryTreeError::Backend)?;
    assemble_story_comment_tree(snapshot, story_id, options)
}

/// Assembles a nested comment tree from one preloaded snapshot.
///
/// Behavior:
/// - Validates root story existence and type.
/// - Preserves explicit `kids.display_order` whenever available.
/// - Falls back to `items.parent` edges when ordered edges are missing.
/// - Returns graph-break metadata for non-fatal data inconsistencies.
pub fn assemble_story_comment_tree(
    snapshot: StoryThreadSnapshot,
    story_id: i64,
    options: StoryTreeOptions,
) -> Result<StoryCommentTree, RetrieveStoryTreeError> {
    let story = snapshot
        .story
        .ok_or(RetrieveStoryTreeError::StoryNotFound { story_id })?;

    if story.type_.as_deref() != Some("story") {
        return Err(RetrieveStoryTreeError::NotAStory {
            requested_story_id: story_id,
            actual_type: story.type_.clone(),
        });
    }

    let mut graph_breaks = Vec::new();
    let mut visible_comments = HashMap::<i64, StoryTreeItem>::new();
    let mut filtered_out_ids = HashSet::<i64>::new();
    let mut outside_story_ids = HashSet::<i64>::new();

    for item in snapshot.comments {
        if item.id == story_id || item.type_.as_deref() != Some("comment") {
            continue;
        }

        if item.story_id != Some(story_id) {
            outside_story_ids.insert(item.id);
            graph_breaks.push(GraphBreak {
                reason: GraphBreakReason::EdgePointsOutsideStory,
                parent_id: item.parent,
                child_id: Some(item.id),
            });
            continue;
        }

        if (!options.include_deleted && item.deleted.unwrap_or(false))
            || (!options.include_dead && item.dead.unwrap_or(false))
        {
            filtered_out_ids.insert(item.id);
            continue;
        }

        visible_comments.insert(item.id, item);
    }

    let mut edge_seen = HashSet::<(i64, i64)>::new();
    let mut edges_by_parent = HashMap::<i64, Vec<OrderedChild>>::new();

    for edge in snapshot.kid_edges {
        let parent_visible = edge.item == story_id || visible_comments.contains_key(&edge.item);
        let child_visible = visible_comments.contains_key(&edge.kid);

        if !parent_visible {
            if filtered_out_ids.contains(&edge.item) {
                continue;
            }
            let reason = if outside_story_ids.contains(&edge.item) {
                GraphBreakReason::EdgePointsOutsideStory
            } else {
                GraphBreakReason::MissingParent
            };
            graph_breaks.push(GraphBreak {
                reason,
                parent_id: Some(edge.item),
                child_id: Some(edge.kid),
            });
            continue;
        }

        if !child_visible {
            if filtered_out_ids.contains(&edge.kid) {
                continue;
            }
            let reason = if outside_story_ids.contains(&edge.kid) {
                GraphBreakReason::EdgePointsOutsideStory
            } else {
                GraphBreakReason::MissingChild
            };
            graph_breaks.push(GraphBreak {
                reason,
                parent_id: Some(edge.item),
                child_id: Some(edge.kid),
            });
            continue;
        }

        if !edge_seen.insert((edge.item, edge.kid)) {
            continue;
        }

        edges_by_parent
            .entry(edge.item)
            .or_default()
            .push(OrderedChild::explicit(edge.kid, edge.display_order));
    }

    for (item_id, item) in &visible_comments {
        let Some(parent_id) = item.parent else {
            graph_breaks.push(GraphBreak {
                reason: GraphBreakReason::MissingParent,
                parent_id: None,
                child_id: Some(*item_id),
            });
            continue;
        };

        if parent_id != story_id && !visible_comments.contains_key(&parent_id) {
            if filtered_out_ids.contains(&parent_id) {
                continue;
            }
            let reason = if outside_story_ids.contains(&parent_id) {
                GraphBreakReason::EdgePointsOutsideStory
            } else {
                GraphBreakReason::MissingParent
            };
            graph_breaks.push(GraphBreak {
                reason,
                parent_id: Some(parent_id),
                child_id: Some(*item_id),
            });
            continue;
        }

        if edge_seen.insert((parent_id, *item_id)) {
            edges_by_parent
                .entry(parent_id)
                .or_default()
                .push(OrderedChild::fallback(*item_id));
        }
    }

    for children in edges_by_parent.values_mut() {
        children.sort_unstable();
    }

    let mut roots = Vec::new();
    let mut attached = HashSet::<i64>::new();
    let mut path = Vec::<i64>::new();
    let mut path_set = HashSet::<i64>::new();

    if let Some(top_level) = edges_by_parent.get(&story_id).cloned() {
        for edge in top_level {
            if let Some(node) = build_tree_node(
                edge.kid,
                &visible_comments,
                &edges_by_parent,
                &mut attached,
                &mut path,
                &mut path_set,
                &mut graph_breaks,
            ) {
                roots.push(node);
            }
        }
    }

    let mut orphans = visible_comments
        .keys()
        .copied()
        .filter(|id| !attached.contains(id))
        .collect::<Vec<_>>();
    orphans.sort_unstable();
    for orphan_id in orphans {
        if let Some(orphan) = visible_comments.get(&orphan_id) {
            if orphan.parent.is_some() && orphan.parent != Some(story_id) {
                graph_breaks.push(GraphBreak {
                    reason: GraphBreakReason::MissingParent,
                    parent_id: orphan.parent,
                    child_id: Some(orphan.id),
                });
            }
        }

        if let Some(node) = build_tree_node(
            orphan_id,
            &visible_comments,
            &edges_by_parent,
            &mut attached,
            &mut path,
            &mut path_set,
            &mut graph_breaks,
        ) {
            roots.push(node);
        }
    }

    Ok(StoryCommentTree {
        story,
        roots,
        graph_breaks,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OrderedChild {
    source_priority: i8,
    display_order: i64,
    kid: i64,
}

impl OrderedChild {
    fn explicit(kid: i64, display_order: Option<i64>) -> Self {
        Self {
            source_priority: 0,
            display_order: display_order.unwrap_or(i64::MAX),
            kid,
        }
    }

    fn fallback(kid: i64) -> Self {
        Self {
            source_priority: 1,
            display_order: i64::MAX,
            kid,
        }
    }
}

fn build_tree_node(
    node_id: i64,
    comments: &HashMap<i64, StoryTreeItem>,
    edges_by_parent: &HashMap<i64, Vec<OrderedChild>>,
    attached: &mut HashSet<i64>,
    path: &mut Vec<i64>,
    path_set: &mut HashSet<i64>,
    graph_breaks: &mut Vec<GraphBreak>,
) -> Option<CommentTreeNode> {
    if attached.contains(&node_id) {
        return None;
    }

    let item = comments.get(&node_id)?.clone();

    if !path_set.insert(node_id) {
        graph_breaks.push(GraphBreak {
            reason: GraphBreakReason::CycleDetected,
            parent_id: path.last().copied(),
            child_id: Some(node_id),
        });
        return None;
    }
    path.push(node_id);

    let mut children_nodes = Vec::new();
    if let Some(children) = edges_by_parent.get(&node_id) {
        for child in children {
            if path_set.contains(&child.kid) {
                graph_breaks.push(GraphBreak {
                    reason: GraphBreakReason::CycleDetected,
                    parent_id: Some(node_id),
                    child_id: Some(child.kid),
                });
                continue;
            }
            if let Some(child_node) = build_tree_node(
                child.kid,
                comments,
                edges_by_parent,
                attached,
                path,
                path_set,
                graph_breaks,
            ) {
                children_nodes.push(child_node);
            }
        }
    }

    path.pop();
    path_set.remove(&node_id);
    attached.insert(node_id);

    Some(CommentTreeNode {
        item,
        children: children_nodes,
    })
}

fn database_backend_error(err: diesel::result::Error) -> StoryTreeBackendError {
    let message = err.to_string();
    let lower = message.to_ascii_lowercase();
    let transient = lower.contains("timeout")
        || lower.contains("temporar")
        || lower.contains("deadlock")
        || lower.contains("connection reset")
        || lower.contains("broken pipe")
        || lower.contains("connection");

    if transient {
        StoryTreeBackendError::transient(BackendDependency::Database, message)
    } else {
        StoryTreeBackendError::permanent(BackendDependency::Database, message)
    }
}

#[derive(QueryableByName)]
struct StoryTreeItemRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    parent: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    type_: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    by: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    time: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    text: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    title: Option<String>,
    #[diesel(sql_type = Nullable<Bool>)]
    deleted: Option<bool>,
    #[diesel(sql_type = Nullable<Bool>)]
    dead: Option<bool>,
    #[diesel(sql_type = Nullable<BigInt>)]
    story_id: Option<i64>,
}

impl StoryTreeItemRow {
    fn into_item(self) -> StoryTreeItem {
        StoryTreeItem {
            id: self.id,
            parent: self.parent,
            type_: self.type_,
            by: self.by,
            time: self.time,
            text: self.text,
            title: self.title,
            deleted: self.deleted,
            dead: self.dead,
            story_id: self.story_id,
        }
    }
}

#[derive(QueryableByName)]
struct KidEdgeRow {
    #[diesel(sql_type = BigInt)]
    item: i64,
    #[diesel(sql_type = BigInt)]
    kid: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    display_order: Option<i64>,
}

impl KidEdgeRow {
    fn to_edge(self) -> KidEdge {
        KidEdge {
            item: self.item,
            kid: self.kid,
            display_order: self.display_order,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use diesel::sql_types::Integer;

    use crate::db::sqlite_test::setup_in_memory_sqlite;

    #[derive(Clone)]
    struct FakeBackend {
        result: Result<StoryThreadSnapshot, StoryTreeBackendError>,
    }

    impl StoryTreeBackend for FakeBackend {
        fn load_story_snapshot(
            &mut self,
            _story_id: i64,
            _options: &StoryTreeOptions,
        ) -> Result<StoryThreadSnapshot, StoryTreeBackendError> {
            self.result.clone()
        }
    }

    fn story_item(id: i64) -> StoryTreeItem {
        StoryTreeItem {
            id,
            parent: None,
            type_: Some("story".to_string()),
            by: Some("author".to_string()),
            time: Some(1_700_000_000),
            text: None,
            title: Some(format!("story-{id}")),
            deleted: Some(false),
            dead: Some(false),
            story_id: Some(id),
        }
    }

    fn comment_item(id: i64, parent: i64, story_id: i64) -> StoryTreeItem {
        StoryTreeItem {
            id,
            parent: Some(parent),
            type_: Some("comment".to_string()),
            by: Some("commenter".to_string()),
            time: Some(1_700_000_100),
            text: Some(format!("comment-{id}")),
            title: None,
            deleted: Some(false),
            dead: Some(false),
            story_id: Some(story_id),
        }
    }

    fn flatten_depth_first_ids(nodes: &[CommentTreeNode]) -> Vec<i64> {
        fn walk(node: &CommentTreeNode, out: &mut Vec<i64>) {
            out.push(node.item.id);
            for child in &node.children {
                walk(child, out);
            }
        }

        let mut out = Vec::new();
        for node in nodes {
            walk(node, &mut out);
        }
        out
    }

    #[test]
    fn retrieves_via_backend_and_assembles_tree() {
        let mut backend = FakeBackend {
            result: Ok(StoryThreadSnapshot {
                story: Some(story_item(100)),
                comments: vec![
                    comment_item(200, 100, 100),
                    comment_item(201, 100, 100),
                    comment_item(300, 200, 100),
                ],
                kid_edges: vec![
                    KidEdge {
                        item: 100,
                        kid: 200,
                        display_order: Some(0),
                    },
                    KidEdge {
                        item: 100,
                        kid: 201,
                        display_order: Some(1),
                    },
                    KidEdge {
                        item: 200,
                        kid: 300,
                        display_order: Some(0),
                    },
                ],
            }),
        };

        let tree = retrieve_story_comments_as_tree(&mut backend, 100, StoryTreeOptions::default())
            .expect("retrieval should succeed");
        assert_eq!(flatten_depth_first_ids(&tree.roots), vec![200, 300, 201]);
    }

    #[test]
    fn assembles_depth_first_tree_in_display_order() {
        let snapshot = StoryThreadSnapshot {
            story: Some(story_item(100)),
            comments: vec![
                comment_item(200, 100, 100),
                comment_item(201, 100, 100),
                comment_item(300, 200, 100),
                comment_item(301, 200, 100),
                comment_item(400, 201, 100),
            ],
            kid_edges: vec![
                KidEdge {
                    item: 100,
                    kid: 201,
                    display_order: Some(1),
                },
                KidEdge {
                    item: 100,
                    kid: 200,
                    display_order: Some(0),
                },
                KidEdge {
                    item: 200,
                    kid: 301,
                    display_order: Some(1),
                },
                KidEdge {
                    item: 200,
                    kid: 300,
                    display_order: Some(0),
                },
                KidEdge {
                    item: 201,
                    kid: 400,
                    display_order: Some(0),
                },
            ],
        };

        let tree = assemble_story_comment_tree(snapshot, 100, StoryTreeOptions::default())
            .expect("assembly should succeed");

        let root_ids: Vec<i64> = tree.roots.iter().map(|node| node.item.id).collect();
        assert_eq!(
            root_ids,
            vec![200, 201],
            "top-level ordering should match kids"
        );

        let flattened = flatten_depth_first_ids(&tree.roots);
        assert_eq!(
            flattened,
            vec![200, 300, 301, 201, 400],
            "depth-first order should preserve sibling display order"
        );
    }

    #[test]
    fn reports_graph_breaks_without_failing_whole_request() {
        let snapshot = StoryThreadSnapshot {
            story: Some(story_item(100)),
            comments: vec![
                comment_item(200, 100, 100),
                comment_item(300, 200, 100),
                comment_item(301, 300, 100),
            ],
            kid_edges: vec![
                KidEdge {
                    item: 100,
                    kid: 200,
                    display_order: Some(0),
                },
                KidEdge {
                    item: 200,
                    kid: 999,
                    display_order: Some(0),
                },
                KidEdge {
                    item: 300,
                    kid: 301,
                    display_order: Some(0),
                },
                KidEdge {
                    item: 301,
                    kid: 300,
                    display_order: Some(0),
                },
            ],
        };

        let tree = assemble_story_comment_tree(snapshot, 100, StoryTreeOptions::default())
            .expect("assembly should return partial result with break metadata");

        assert!(
            tree.graph_breaks
                .iter()
                .any(|b| b.reason == GraphBreakReason::MissingChild && b.child_id == Some(999)),
            "missing child edge should be reported"
        );
        assert!(
            tree.graph_breaks
                .iter()
                .any(|b| b.reason == GraphBreakReason::CycleDetected),
            "cycles should be reported"
        );
    }

    #[test]
    fn returns_explicit_errors_for_missing_or_non_story_roots() {
        let missing_story = StoryThreadSnapshot {
            story: None,
            comments: Vec::new(),
            kid_edges: Vec::new(),
        };

        let err = assemble_story_comment_tree(missing_story, 42, StoryTreeOptions::default())
            .expect_err("missing story row should be an explicit error");
        assert_eq!(err, RetrieveStoryTreeError::StoryNotFound { story_id: 42 });

        let non_story = StoryThreadSnapshot {
            story: Some(StoryTreeItem {
                id: 42,
                parent: None,
                type_: Some("comment".to_string()),
                by: None,
                time: None,
                text: None,
                title: None,
                deleted: None,
                dead: None,
                story_id: Some(42),
            }),
            comments: Vec::new(),
            kid_edges: Vec::new(),
        };

        let err = assemble_story_comment_tree(non_story, 42, StoryTreeOptions::default())
            .expect_err("non-story root should return NotAStory");
        assert_eq!(
            err,
            RetrieveStoryTreeError::NotAStory {
                requested_story_id: 42,
                actual_type: Some("comment".to_string()),
            }
        );
    }

    #[test]
    fn surfaces_transient_backend_failures_for_db_and_network_issues() {
        let mut db_backend = FakeBackend {
            result: Err(StoryTreeBackendError::transient(
                BackendDependency::Database,
                "temporary connection timeout",
            )),
        };
        let err =
            retrieve_story_comments_as_tree(&mut db_backend, 100, StoryTreeOptions::default())
                .expect_err("transient db failures should be returned to caller");
        assert_eq!(
            err,
            RetrieveStoryTreeError::Backend(StoryTreeBackendError::transient(
                BackendDependency::Database,
                "temporary connection timeout",
            ))
        );

        let mut network_backend = FakeBackend {
            result: Err(StoryTreeBackendError::transient(
                BackendDependency::Network,
                "upstream API 503",
            )),
        };
        let err =
            retrieve_story_comments_as_tree(&mut network_backend, 100, StoryTreeOptions::default())
                .expect_err("transient network failures should be returned to caller");
        assert_eq!(
            err,
            RetrieveStoryTreeError::Backend(StoryTreeBackendError::transient(
                BackendDependency::Network,
                "upstream API 503",
            ))
        );
    }

    #[test]
    fn sqlite_backend_retrieves_ordered_tree_with_two_call_strategy() {
        let mut conn = setup_in_memory_sqlite();
        seed_story(&mut conn, 10, "story", Some(10), None);
        seed_comment(&mut conn, 20, 10, 10);
        seed_comment(&mut conn, 21, 10, 10);
        seed_comment(&mut conn, 30, 20, 10);
        seed_comment(&mut conn, 31, 20, 10);

        seed_kid(&mut conn, 10, 21, 1);
        seed_kid(&mut conn, 10, 20, 0);
        seed_kid(&mut conn, 20, 31, 1);
        seed_kid(&mut conn, 20, 30, 0);

        let mut backend = SqliteStoryTreeBackend::new(&mut conn);
        let tree = retrieve_story_comments_as_tree(&mut backend, 10, StoryTreeOptions::default())
            .expect("sqlite retrieval should succeed");

        assert_eq!(flatten_depth_first_ids(&tree.roots), vec![20, 30, 31, 21]);
        assert!(tree.graph_breaks.is_empty());
    }

    #[test]
    fn sqlite_backend_handles_missing_and_non_story_roots() {
        let mut conn = setup_in_memory_sqlite();
        seed_story(&mut conn, 77, "comment", Some(77), Some(1));

        let mut backend = SqliteStoryTreeBackend::new(&mut conn);
        let err = retrieve_story_comments_as_tree(&mut backend, 12345, StoryTreeOptions::default())
            .expect_err("missing story should return StoryNotFound");
        assert_eq!(
            err,
            RetrieveStoryTreeError::StoryNotFound { story_id: 12345 }
        );

        let err = retrieve_story_comments_as_tree(&mut backend, 77, StoryTreeOptions::default())
            .expect_err("non-story root should return NotAStory");
        assert_eq!(
            err,
            RetrieveStoryTreeError::NotAStory {
                requested_story_id: 77,
                actual_type: Some("comment".to_string()),
            }
        );
    }

    #[test]
    fn sqlite_backend_reports_graph_breaks_and_keeps_partial_tree() {
        let mut conn = setup_in_memory_sqlite();
        seed_story(&mut conn, 50, "story", Some(50), None);
        seed_comment(&mut conn, 60, 50, 50);
        seed_comment(&mut conn, 61, 60, 50);
        seed_comment(&mut conn, 62, 61, 50);

        seed_kid(&mut conn, 50, 60, 0);
        seed_kid(&mut conn, 60, 999, 0);
        seed_kid(&mut conn, 61, 62, 0);
        seed_kid(&mut conn, 62, 61, 0);

        let mut backend = SqliteStoryTreeBackend::new(&mut conn);
        let tree = retrieve_story_comments_as_tree(&mut backend, 50, StoryTreeOptions::default())
            .expect("graph breaks should not fail whole request");

        assert_eq!(flatten_depth_first_ids(&tree.roots), vec![60, 61, 62]);
        assert!(tree
            .graph_breaks
            .iter()
            .any(|b| b.reason == GraphBreakReason::MissingChild && b.child_id == Some(999)));
        assert!(tree
            .graph_breaks
            .iter()
            .any(|b| b.reason == GraphBreakReason::CycleDetected));
    }

    fn seed_story(
        conn: &mut SqliteConnection,
        id: i64,
        item_type: &str,
        story_id: Option<i64>,
        parent: Option<i64>,
    ) {
        sql_query(
            r#"
            INSERT INTO items (id, type, parent, title, story_id, deleted, dead)
            VALUES (?, ?, ?, ?, ?, 0, 0)
            "#,
        )
        .bind::<BigInt, _>(id)
        .bind::<Text, _>(item_type)
        .bind::<Nullable<BigInt>, _>(parent)
        .bind::<Nullable<Text>, _>(Some(format!("item-{id}")))
        .bind::<Nullable<BigInt>, _>(story_id)
        .execute(conn)
        .expect("failed to seed story row");
    }

    fn seed_comment(conn: &mut SqliteConnection, id: i64, parent: i64, story_id: i64) {
        sql_query(
            r#"
            INSERT INTO items (id, type, parent, text, story_id, deleted, dead)
            VALUES (?, 'comment', ?, ?, ?, 0, 0)
            "#,
        )
        .bind::<BigInt, _>(id)
        .bind::<BigInt, _>(parent)
        .bind::<Nullable<Text>, _>(Some(format!("c-{id}")))
        .bind::<BigInt, _>(story_id)
        .execute(conn)
        .expect("failed to seed comment row");
    }

    fn seed_kid(conn: &mut SqliteConnection, parent: i64, kid: i64, display_order: i32) {
        sql_query(
            r#"
            INSERT INTO kids (item, kid, display_order)
            VALUES (?, ?, ?)
            "#,
        )
        .bind::<BigInt, _>(parent)
        .bind::<BigInt, _>(kid)
        .bind::<Integer, _>(display_order)
        .execute(conn)
        .expect("failed to seed kids edge");
    }
}
