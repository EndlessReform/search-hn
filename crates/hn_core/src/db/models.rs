use super::schema::items;
use crate::HnItem;
use diesel::dsl::{delete, insert_into};
use diesel::pg::PgConnection;
use diesel::prelude::*;

#[derive(Clone, Queryable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = super::schema::items)]
pub struct Item {
    // NOTE: `items.domain`, `items.day`, and `items.search_tsv` are generated columns in
    // Postgres and are intentionally omitted from this ingest model so inserts/upserts never
    // try to write them.
    pub id: i64,
    pub deleted: Option<bool>,
    pub type_: Option<String>,
    pub by: Option<String>,
    pub time: Option<i64>,
    pub text: Option<String>,
    pub dead: Option<bool>,
    pub parent: Option<i64>,
    pub poll: Option<i64>,
    pub url: Option<String>,
    pub score: Option<i64>,
    pub title: Option<String>,
    pub parts: Option<Vec<i64>>,
    pub descendants: Option<i64>,
    pub story_id: Option<i64>,
}

impl Item {
    /// Inserts one item row into Postgres.
    ///
    /// Most ingest paths use batched upserts in `PgBatchPersister`; this helper remains
    /// useful for focused tests and one-off admin/debug operations.
    pub fn create(conn: &mut PgConnection, new_item: &Item) -> QueryResult<usize> {
        insert_into(items::table).values(new_item).execute(conn)
    }

    /// Deletes one item row by ID.
    pub fn delete(conn: &mut PgConnection, item_id: i64) -> QueryResult<usize> {
        delete(items::table.filter(items::id.eq(item_id))).execute(conn)
    }

    /// Removes embedded NUL bytes from text fields that map to Postgres `TEXT` columns.
    ///
    /// Why this exists:
    /// - Postgres rejects `TEXT` values containing `\\0` with
    ///   `invalid byte sequence for encoding "UTF8": 0x00`.
    /// - Hacker News payloads are mostly clean, but rare historical rows can contain this byte.
    ///
    /// Returns a list of field names that were mutated so callers can emit telemetry.
    pub fn strip_embedded_nul_bytes(&mut self) -> Vec<&'static str> {
        let mut mutated = Vec::new();
        if strip_nul_from_option_string(&mut self.type_) {
            mutated.push("type_");
        }
        if strip_nul_from_option_string(&mut self.by) {
            mutated.push("by");
        }
        if strip_nul_from_option_string(&mut self.text) {
            mutated.push("text");
        }
        if strip_nul_from_option_string(&mut self.url) {
            mutated.push("url");
        }
        if strip_nul_from_option_string(&mut self.title) {
            mutated.push("title");
        }
        mutated
    }
}

fn strip_nul_from_option_string(value: &mut Option<String>) -> bool {
    let Some(inner) = value else {
        return false;
    };
    if !inner.contains('\0') {
        return false;
    }
    inner.retain(|ch| ch != '\0');
    true
}

impl From<HnItem> for Item {
    fn from(fb_item: HnItem) -> Self {
        Self {
            id: fb_item.id,
            deleted: fb_item.deleted,
            type_: fb_item.type_,
            by: fb_item.by,
            time: fb_item.time,
            text: fb_item.text,
            dead: fb_item.dead,
            parent: fb_item.parent,
            poll: fb_item.poll,
            url: fb_item.url,
            score: fb_item.score,
            title: fb_item.title,
            parts: fb_item.parts,
            descendants: fb_item.descendants,
            story_id: None,
        }
    }
}

#[derive(Queryable, Insertable, AsChangeset)]
#[diesel(table_name = super::schema::kids)]
pub struct Kid {
    pub item: i64,
    pub kid: i64,
    pub display_order: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::Item;

    fn sample_item() -> Item {
        Item {
            id: 1,
            deleted: None,
            type_: Some("story".to_string()),
            by: Some("alice".to_string()),
            time: Some(1_700_000_000),
            text: Some("hello".to_string()),
            dead: Some(false),
            parent: None,
            poll: None,
            url: Some("https://example.com".to_string()),
            score: Some(42),
            title: Some("sample".to_string()),
            parts: None,
            descendants: Some(1),
            story_id: Some(1),
        }
    }

    #[test]
    fn strip_embedded_nul_bytes_mutates_only_text_fields_with_nuls() {
        let mut item = sample_item();
        item.by = Some("a\0b".to_string());
        item.text = Some("\0x\0".to_string());
        item.title = Some("ok".to_string());

        let mutated = item.strip_embedded_nul_bytes();

        assert_eq!(mutated, vec!["by", "text"]);
        assert_eq!(item.by.as_deref(), Some("ab"));
        assert_eq!(item.text.as_deref(), Some("x"));
        assert_eq!(item.title.as_deref(), Some("ok"));
    }
}
