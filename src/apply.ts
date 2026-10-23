import type { Client } from 'pg'

export type ApplyStatus = 'applied' | 'caught-up' | 'not-ready' | 'locked' | 'conflict'

type ReceiptState = 'ready' | 'future' | 'null'

// Remove unset keys, then merge set, so a key named in both keeps the set value.
function stockSql(base: string): string {
  return `CASE
    WHEN q.kind = 'snapshot' THEN q.payload->'stock'
    ELSE (
      (
        ${base} - ARRAY(
          SELECT jsonb_array_elements_text(COALESCE(q.payload->'unset', '[]'::jsonb))
        )
      ) || COALESCE(q.payload->'set', '{}'::jsonb)
    )
  END`
}

type MessageRow = {
  id: string
  message_id: string
  seq: number
  kind: string
  receipt: ReceiptState
}

const RECEIPT_SQL = `CASE
  WHEN r.ready_at IS NULL THEN 'null'
  WHEN r.ready_at <= now() THEN 'ready'
  ELSE 'future'
END`

// One transaction. The advisory lock is per store and ends with the transaction.
export async function applyNextUpdate(
  client: Client,
  chainId: string,
  storeId: string,
): Promise<ApplyStatus> {
  await client.query('BEGIN')
  try {
    const status = await applyLocked(client, chainId, storeId)
    if (status === 'locked' || status === 'conflict') {
      await client.query('ROLLBACK')
      return status
    }
    await client.query('COMMIT')
    return status
  } catch (err) {
    try {
      await client.query('ROLLBACK')
    } catch {
      // Already rolled back.
    }
    throw err
  }
}

async function applyLocked(client: Client, chainId: string, storeId: string): Promise<ApplyStatus> {
  const lock = await client.query<{ locked: boolean }>(
    `SELECT pg_try_advisory_xact_lock(hashtextextended($1::text || ':' || $2::text, 0)) AS locked`,
    [chainId, storeId],
  )
  if (!lock.rows[0]?.locked) return 'locked'

  const existing = await client.query<{ last_seq: number }>(
    `SELECT last_seq
     FROM store_stock
     WHERE chain_id = $1::uuid AND store_id = $2::uuid
     FOR UPDATE`,
    [chainId, storeId],
  )
  const lastSeq = existing.rows.length === 0 ? 0 : Number(existing.rows[0].last_seq)
  const nextSeq = lastSeq + 1
  const inserting = existing.rows.length === 0

  const messages = await client.query<MessageRow>(
    `SELECT m.id::text AS id,
            m.message_id::text AS message_id,
            m.seq,
            m.kind,
            ${RECEIPT_SQL} AS receipt
     FROM queue_messages m
     JOIN receipt_files r ON r.id = m.receipt_id
     WHERE m.chain_id = $1::uuid
       AND m.store_id = $2::uuid
       AND m.seq = $3::int
     ORDER BY m.id
     FOR UPDATE OF m`,
    [chainId, storeId, nextSeq],
  )
  if (messages.rows.length === 0) return 'caught-up'

  const chosen = messages.rows[0]
  // A future receipt on the contiguous next seq is a wait, even when a later
  // snapshot is already ready. Only a null receipt authorizes the walk.
  if (chosen.receipt === 'future') return 'not-ready'
  if (chosen.receipt === 'null') {
    return resumeAtSnapshot(client, chainId, storeId, lastSeq, inserting)
  }
  if (chosen.receipt !== 'ready') {
    throw new Error(`unexpected receipt state ${String(chosen.receipt)}`)
  }

  const wrote = await writeStock(client, chainId, storeId, lastSeq, nextSeq, chosen.id, inserting)
  if (!wrote) return 'conflict'
  await recordApplied(client, chainId, storeId, nextSeq, chosen, messages.rows.length)
  return 'applied'
}

// Walk later snapshots in seq order. The first one whose receipt is not null
// is the candidate. Gap rows are marked only in this same transaction.
async function resumeAtSnapshot(
  client: Client,
  chainId: string,
  storeId: string,
  lastSeq: number,
  inserting: boolean,
): Promise<ApplyStatus> {
  const snaps = await client.query<MessageRow>(
    `SELECT m.id::text AS id,
            m.message_id::text AS message_id,
            m.seq,
            m.kind,
            ${RECEIPT_SQL} AS receipt
     FROM queue_messages m
     JOIN receipt_files r ON r.id = m.receipt_id
     WHERE m.chain_id = $1::uuid
       AND m.store_id = $2::uuid
       AND m.kind = 'snapshot'
       AND m.seq > $3::int
     ORDER BY m.seq, m.id
     FOR UPDATE OF m`,
    [chainId, storeId, lastSeq],
  )

  let seenSeq = Number.NaN
  let candidate: MessageRow | undefined
  for (const row of snaps.rows) {
    const seq = Number(row.seq)
    if (seq === seenSeq) continue
    seenSeq = seq
    if (row.receipt === 'null') continue
    candidate = row
    break
  }

  // No later snapshot can reopen the store. Leave the prefix in place.
  if (!candidate) return 'caught-up'
  // Do not apply an even later snapshot that is ready now.
  if (candidate.receipt === 'future') return 'not-ready'
  if (candidate.receipt !== 'ready') {
    throw new Error(`unexpected snapshot receipt state ${String(candidate.receipt)}`)
  }

  const snapSeq = Number(candidate.seq)
  const atSeq = snaps.rows.filter((row) => Number(row.seq) === snapSeq)
  const wrote = await writeStock(client, chainId, storeId, lastSeq, snapSeq, candidate.id, inserting)
  if (!wrote) return 'conflict'

  await markGapSkipped(client, chainId, storeId, lastSeq, snapSeq)
  await recordApplied(client, chainId, storeId, snapSeq, candidate, atSeq.length)
  return 'applied'
}

async function recordApplied(
  client: Client,
  chainId: string,
  storeId: string,
  seq: number,
  chosen: MessageRow,
  rowsAtSeq: number,
): Promise<void> {
  await client.query(
    `INSERT INTO apply_log (message_id, chain_id, store_id, seq, kind, worker_id)
     VALUES ($1::uuid, $2::uuid, $3::uuid, $4::int, $5::text, $6::text)`,
    [chosen.message_id, chainId, storeId, seq, chosen.kind, String(process.pid)],
  )

  const marked = await client.query(
    `UPDATE queue_messages
     SET outcome = CASE WHEN id = $4::bigint THEN 'applied' ELSE 'duplicate' END
     WHERE chain_id = $1::uuid AND store_id = $2::uuid AND seq = $3::int`,
    [chainId, storeId, seq, chosen.id],
  )
  if ((marked.rowCount ?? 0) !== rowsAtSeq) {
    throw new Error(
      `expected to mark ${rowsAtSeq} queue rows for ${chainId}/${storeId} seq ${seq}, marked ${marked.rowCount ?? 0}`,
    )
  }
}

async function markGapSkipped(
  client: Client,
  chainId: string,
  storeId: string,
  lastSeq: number,
  snapSeq: number,
): Promise<void> {
  await client.query(
    `SELECT id
     FROM queue_messages
     WHERE chain_id = $1::uuid
       AND store_id = $2::uuid
       AND seq > $3::int
       AND seq < $4::int
     ORDER BY id
     FOR UPDATE`,
    [chainId, storeId, lastSeq, snapSeq],
  )

  const marked = await client.query(
    `UPDATE queue_messages
     SET outcome = 'skipped'
     WHERE chain_id = $1::uuid
       AND store_id = $2::uuid
       AND seq > $3::int
       AND seq < $4::int
       AND outcome IS NULL`,
    [chainId, storeId, lastSeq, snapSeq],
  )
  if ((marked.rowCount ?? 0) < 1) {
    throw new Error(`poison jump for ${chainId}/${storeId} onto seq ${snapSeq} marked an empty gap`)
  }

  const left = await client.query<{ n: number }>(
    `SELECT count(*)::int AS n
     FROM queue_messages
     WHERE chain_id = $1::uuid
       AND store_id = $2::uuid
       AND seq > $3::int
       AND seq < $4::int
       AND outcome IS DISTINCT FROM 'skipped'`,
    [chainId, storeId, lastSeq, snapSeq],
  )
  if (left.rows[0].n !== 0) {
    throw new Error(`poison jump for ${chainId}/${storeId} left ${left.rows[0].n} gap rows unmarked`)
  }
}

async function writeStock(
  client: Client,
  chainId: string,
  storeId: string,
  lastSeq: number,
  nextSeq: number,
  messageRowId: string,
  insert: boolean,
): Promise<boolean> {
  if (insert) {
    const inserted = await client.query(
      `INSERT INTO store_stock (chain_id, store_id, stock, version, last_seq)
       SELECT $1::uuid, $2::uuid, ${stockSql(`'{}'::jsonb`)}, 1, $3::int
       FROM queue_messages q
       WHERE q.id = $4::bigint
       RETURNING last_seq`,
      [chainId, storeId, nextSeq, messageRowId],
    )
    if ((inserted.rowCount ?? 0) !== 1) {
      throw new Error(`store_stock insert failed for ${chainId}/${storeId} seq ${nextSeq}`)
    }
    return true
  }

  const updated = await client.query(
    `UPDATE store_stock AS s
     SET stock = ${stockSql('s.stock')},
         version = s.version + 1,
         last_seq = $4::int,
         updated_at = now()
     FROM queue_messages AS q
     WHERE s.chain_id = $1::uuid
       AND s.store_id = $2::uuid
       AND s.last_seq = $3::int
       AND q.id = $5::bigint
     RETURNING s.last_seq`,
    [chainId, storeId, lastSeq, nextSeq, messageRowId],
  )
  return (updated.rowCount ?? 0) === 1
}
