ALTER TABLE delivery_ledger ADD COLUMN message_id TEXT;
ALTER TABLE delivery_ledger ADD COLUMN received_at TEXT;
ALTER TABLE delivery_ledger ADD COLUMN completed_at TEXT;
ALTER TABLE delivery_ledger ADD COLUMN processing_latency_ms INTEGER;

CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_ledger_message
    ON delivery_ledger(message_id);
