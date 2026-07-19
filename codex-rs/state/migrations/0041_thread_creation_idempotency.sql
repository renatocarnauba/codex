CREATE TABLE thread_creation_idempotency (
    originator TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('start', 'fork')),
    thread_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'committed' CHECK (status IN ('pending', 'committed')),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (originator, idempotency_key),
    UNIQUE (thread_id)
);

CREATE INDEX idx_thread_creation_idempotency_thread
    ON thread_creation_idempotency(thread_id);

-- SessionMeta is the canonical source for legacy creation bindings. Re-open the
-- existing leased startup backfill so migration 0041 projects those bindings
-- before the app-server accepts creation requests.
UPDATE backfill_state
SET status = 'pending', last_watermark = NULL, last_success_at = NULL, updated_at = 0
WHERE id = 1;
