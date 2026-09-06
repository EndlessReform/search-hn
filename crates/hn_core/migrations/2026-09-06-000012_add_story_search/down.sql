SET LOCAL lock_timeout = '5s';
DROP TRIGGER story_search_source_changed ON items;
DROP FUNCTION story_search_source_changed();
DROP FUNCTION sync_story_search(items);
DROP FUNCTION story_search_eligible(items);
DROP TABLE story_search;
-- Extensions may have other consumers; leave them installed.
