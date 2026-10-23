import type { Client } from 'pg'

// Additive only. schema.sql stays untouched. seed.js truncates rows and leaves
// these columns and constraints in place.
const STATEMENTS = [
  `ALTER TABLE queue_messages ADD COLUMN IF NOT EXISTS outcome text`,
  `DO $$
   BEGIN
     IF NOT EXISTS (
       SELECT 1 FROM pg_constraint WHERE conname = 'queue_messages_outcome_check'
     ) THEN
       ALTER TABLE queue_messages
         ADD CONSTRAINT queue_messages_outcome_check
         CHECK (outcome IS NULL OR outcome IN ('applied', 'duplicate'));
     END IF;

     IF NOT EXISTS (
       SELECT 1 FROM pg_constraint WHERE conname = 'apply_log_message_id_unique'
     ) THEN
       ALTER TABLE apply_log
         ADD CONSTRAINT apply_log_message_id_unique UNIQUE (message_id);
     END IF;

     IF NOT EXISTS (
       SELECT 1 FROM pg_constraint WHERE conname = 'apply_log_store_seq_unique'
     ) THEN
       ALTER TABLE apply_log
         ADD CONSTRAINT apply_log_store_seq_unique UNIQUE (chain_id, store_id, seq);
     END IF;
   END $$`,
]

export async function migrate(client: Client): Promise<void> {
  await client.query('BEGIN')
  try {
    for (const statement of STATEMENTS) {
      await client.query(statement)
    }
    await client.query('COMMIT')
  } catch (err) {
    try {
      await client.query('ROLLBACK')
    } catch {
      // The connection may already be idle if the transaction aborted.
    }
    throw err
  }
}
