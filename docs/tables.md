# Database Tables and Indexes

## Core Tables

### `items`
Main table storing Hacker News items (stories, comments, polls, etc.).

**Columns:**
- `id` (BIGINT, PK): Unique item identifier
- `type` (TEXT): Item type (story, comment, poll, etc.)
- `by` (TEXT): Username who created the item
- `time` (BIGINT): Unix timestamp of creation
- `text` (TEXT): Item text content
- `deleted` (BOOL): Whether item is deleted
- `dead` (BOOL): Whether item is dead
- `parent` (BIGINT): Parent item ID (for comments)
- `poll` (BIGINT): Poll item ID
- `url` (TEXT): Item URL (for stories)
- `score` (BIGINT): Upvote count
- `title` (TEXT): Story title
- `parts` (TEXT): Array of related item IDs
- `descendants` (BIGINT): Total comment count
- `story_id` (BIGINT, optional): Reference to parent story (for comments)
- `domain` (TEXT, generated): Extracted domain from URL
- `day` (DATE, generated): UTC calendar date from time
- `search_tsv` (TSVECTOR, generated): Full-text search vector

**Indexes:**
- `PRIMARY KEY (id)`
- `idx_items_type (type)`
- `idx_items_parent (parent)`
- `idx_items_story_id_comment (story_id) WHERE type = 'comment' AND story_id IS NOT NULL`
- `idx_items_search_tsv (search_tsv) WHERE type = 'story'` (GIN index)

### `kids`
Stores relationships between items and their child items.

**Columns:**
- `item` (BIGINT, PK): Parent item ID
- `kid` (BIGINT, PK): Child item ID
- `display_order` (BIGINT, PK+optional): Display order for ordered children

**Indexes:**
- `PRIMARY KEY (item, kid)`
- `idx_kids_item_order (item, display_order) INCLUDE (kid)` (B-tree index)

### `users`
Stores Hacker News user information.

**Columns:**
- `id` (TEXT, PK): Username
- `created` (BIGINT, optional): Account creation timestamp
- `karma` (BIGINT, optional): User karma score
- `about` (TEXT, optional): User bio
- `submitted` (TEXT, optional): JSON array of submitted item IDs

**Indexes:**
- `PRIMARY KEY (id)`

### `ingest_segments`
Tracks ingestion job segments for parallel processing.

**Columns:**
- `segment_id` (BIGSERIAL, PK): Unique segment identifier
- `start_id` (BIGINT): First item ID in segment
- `end_id` (BIGINT): Last item ID in segment
- `status` (TEXT): Segment status (pending, in_progress, done, retry_wait, dead_letter)
- `attempts` (INTEGER): Number of retry attempts
- `scan_cursor_id` (BIGINT): Current scan position
- `unresolved_count` (INTEGER): Number of unresolved items
- `heartbeat_at` (TIMESTAMPTZ): Last heartbeat timestamp
- `started_at` (TIMESTAMPTZ): Start time
- `last_error` (TEXT): Last error message
- `failure_class` (TEXT, optional): Failure classification

**Indexes:**
- `ingest_segments_status_idx (status, started_at)`
- `ingest_segments_heartbeat_idx (status, heartbeat_at)`
- `ingest_segments_range_idx (start_id, end_id)`

### `ingest_exceptions`
Tracks individual item exceptions during ingestion.

**Columns:**
- `segment_id` (BIGINT, PK): Reference to ingest_segments
- `item_id` (BIGINT, PK): Failed item ID
- `state` (TEXT): Exception state (pending, retry_wait, terminal_missing, dead_letter)
- `attempts` (INTEGER): Number of retry attempts
- `next_retry_at` (TIMESTAMPTZ, optional): Next retry timestamp
- `last_error` (TEXT): Last error message
- `updated_at` (TIMESTAMPTZ): Last update timestamp
- `failure_class` (TEXT, optional): Failure classification

**Indexes:**
- `PRIMARY KEY (segment_id, item_id)`
- `ingest_exceptions_state_idx (state, next_retry_at)`
- `ingest_exceptions_item_idx (item_id)`

## Generated Columns

### `items.domain`
Extracts domain from URL (best-effort, lowercased, NULL on failure).

### `items.day`
Normalizes Unix timestamp to UTC calendar date.

### `items.search_tsv`
Full-text search vector over title and URL (weighted: title='A', URL='B'), using `simple` config.

## Index Strategy

**Primary Queries:**
- Item type filtering: `idx_items_type`
- Parent-child traversal: `idx_items_parent` + `idx_kids_item_order`
- Story search: `idx_items_search_tsv` (GIN on story type only)
- Comment retrieval: `idx_items_story_id_comment`
- Ingestion status: `ingest_segments_*` indexes
- Exception retry: `ingest_exceptions_*` indexes

**Performance Notes:**
- GIN index on `search_tsv` is conditional on `type = 'story'` to reduce bloat
- `idx_kids_item_order` includes child ID for efficient retrieval
- Ingestion indexes use composite keys for efficient range queries
