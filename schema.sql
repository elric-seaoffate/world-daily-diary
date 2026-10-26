CREATE TABLE IF NOT EXISTS articles (
    article_id TEXT PRIMARY KEY,
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    translated_summary TEXT DEFAULT NULL,
    source_id TEXT NOT NULL,
    category_hint TEXT NOT NULL,
    language_code TEXT NOT NULL,
    syndication_velocity INTEGER NOT NULL DEFAULT 1,
    published_at INTEGER NOT NULL,
    ingested_date TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX IF NOT EXISTS idx_articles_daily_picker ON articles (ingested_date, category_hint, syndication_velocity DESC);
CREATE INDEX IF NOT EXISTS idx_articles_pruning ON articles (created_at);

CREATE TABLE IF NOT EXISTS feed_telemetry (
    source_id TEXT PRIMARY KEY,
    rss_name TEXT NOT NULL,
    feed_url TEXT NOT NULL,
    total_polls INTEGER NOT NULL DEFAULT 0,
    successful_polls INTEGER NOT NULL DEFAULT 0,
    failed_polls INTEGER NOT NULL DEFAULT 0,
    avg_response_ms INTEGER NOT NULL DEFAULT 0,
    avg_xml_bytes INTEGER NOT NULL DEFAULT 0,
    total_raw_items INTEGER NOT NULL DEFAULT 0,
    total_fresh_items INTEGER NOT NULL DEFAULT 0,
    total_fluff_filtered INTEGER NOT NULL DEFAULT 0,
    total_duplicates_caught INTEGER NOT NULL DEFAULT 0,
    avg_summary_length INTEGER NOT NULL DEFAULT 0,
    supports_caching_headers INTEGER NOT NULL DEFAULT 0,
    last_status_code INTEGER DEFAULT 0,
    last_error_reason TEXT DEFAULT NULL,
    last_polled_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS published_stories (
    story_id TEXT PRIMARY KEY,
    parent_story_id TEXT DEFAULT NULL,
    ingested_date TEXT NOT NULL,
    category_id TEXT NOT NULL,
    headline TEXT NOT NULL,
    markdown_body TEXT NOT NULL,
    original_body_hash TEXT NOT NULL,
    search_entities_json TEXT NOT NULL,
    sources_json TEXT NOT NULL,
    tracking_status INTEGER NOT NULL DEFAULT 0,
    tracking_origin TEXT DEFAULT NULL,
    followup_directive TEXT DEFAULT NULL,
    expansion_count INTEGER NOT NULL DEFAULT 0,
    last_expanded_at INTEGER DEFAULT NULL,
    bookstack_synced INTEGER NOT NULL DEFAULT 0,
    bookstack_page_id INTEGER DEFAULT NULL,
    r2_synced INTEGER NOT NULL DEFAULT 0,
    zfs_synced INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX IF NOT EXISTS idx_published_sync_queue ON published_stories (ingested_date, bookstack_synced, r2_synced, zfs_synced);
CREATE INDEX IF NOT EXISTS idx_published_recent_lookup ON published_stories (category_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_published_active_tracking ON published_stories (tracking_status, category_id, created_at DESC);
