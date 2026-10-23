-- StockStream Backlog Sync Processor — provided schema.
-- You may ADD columns / tables / indexes / constraints, but must not modify
-- or remove anything defined here. Applied automatically by seed.js.

CREATE TABLE IF NOT EXISTS receipt_files (
  id       UUID PRIMARY KEY,
  -- When the receipt upload completed. NULL = the gateway reported the upload failed.
  ready_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS queue_messages (
  id          BIGSERIAL PRIMARY KEY,  -- arrival order at the platform edge
  message_id  UUID NOT NULL,          -- the update's identity, assigned by the gateway
  chain_id    UUID NOT NULL,
  store_id    UUID NOT NULL,
  seq         INTEGER NOT NULL,       -- store-local sequence, contiguous per (chain_id, store_id) from 1
  kind        TEXT NOT NULL CHECK (kind IN ('snapshot', 'delta')),
  payload     JSONB NOT NULL,
  receipt_id  UUID NOT NULL REFERENCES receipt_files(id),
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS queue_messages_key_seq
  ON queue_messages (chain_id, store_id, seq);

CREATE TABLE IF NOT EXISTS store_stock (
  chain_id   UUID NOT NULL,
  store_id   UUID NOT NULL,
  stock      JSONB NOT NULL DEFAULT '{}'::jsonb,
  version    INTEGER NOT NULL DEFAULT 0,  -- count of updates applied to this store
  last_seq   INTEGER NOT NULL DEFAULT 0,  -- store-local seq of the newest applied update
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chain_id, store_id)
);

CREATE TABLE IF NOT EXISTS apply_log (
  apply_pos  BIGSERIAL PRIMARY KEY,
  message_id UUID NOT NULL,
  chain_id   UUID NOT NULL,
  store_id   UUID NOT NULL,
  seq        INTEGER NOT NULL,
  kind       TEXT NOT NULL,
  worker_id  TEXT NOT NULL,  -- identifies the OS process that applied the update
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
