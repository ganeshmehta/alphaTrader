-- Bronze layer: raw, lightly-typed landing table for Wikimedia recent-change events.
-- One row per (filtered) event consumed from the raw_wiki_changes Kafka topic.

CREATE TABLE IF NOT EXISTS wiki_changes_bronze (
    id              BIGSERIAL PRIMARY KEY,
    wiki            TEXT,
    change_type     TEXT,       -- 'edit' | 'new' | 'log' | 'categorize'
    title           TEXT,
    "user"          TEXT,
    is_bot          BOOLEAN,
    is_minor        BOOLEAN,
    comment         TEXT,
    event_timestamp TIMESTAMP,  -- when the change happened, from the source event
    server_name     TEXT,
    length_old      INTEGER,
    length_new      INTEGER,
    rev_old         BIGINT,
    rev_new         BIGINT,
    meta_id         TEXT,       -- Wikimedia event UUID, useful for dedup
    meta_dt         TIMESTAMP,  -- when Wikimedia emitted the event
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wiki_changes_bronze_wiki ON wiki_changes_bronze (wiki);
CREATE INDEX IF NOT EXISTS idx_wiki_changes_bronze_event_ts ON wiki_changes_bronze (event_timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS idx_wiki_changes_bronze_meta_id ON wiki_changes_bronze (meta_id);
