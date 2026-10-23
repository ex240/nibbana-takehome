import type { Client } from 'pg'

// Independent replay of the queue. This file must not import the processor.

export type Stock = Record<string, number | string>

export type AppliedUpdate = {
  messageId: string
  seq: number
  kind: 'snapshot' | 'delta'
}

export type StoreReplay = {
  chainId: string
  storeId: string
  stock: Stock
  applied: AppliedUpdate[]
}

type DeltaPayload = {
  set?: Stock
  unset?: string[]
}

type ReceiptState = 'ready' | 'future' | 'null'

type QueueRow = {
  id: string
  message_id: string
  chain_id: string
  store_id: string
  seq: number
  kind: string
  payload: { stock?: Stock; set?: Stock; unset?: string[] }
  receipt: ReceiptState
}

type SeqGroup = {
  seq: number
  chosen: QueueRow
}

// Unset first, then set. A key named in both keeps the assigned quantity.
export function applyDelta(stock: Stock, payload: DeltaPayload): Stock {
  const next: Stock = { ...stock }
  for (const key of payload.unset ?? []) {
    delete next[key]
  }
  for (const [key, value] of Object.entries(payload.set ?? {})) {
    next[key] = value
  }
  return next
}

function canonical(rows: QueueRow[]): QueueRow {
  return rows.reduce((best, row) => (Number(row.id) < Number(best.id) ? row : best))
}

function groupBySeq(rows: QueueRow[]): SeqGroup[] {
  const groups: SeqGroup[] = []
  let index = 0
  while (index < rows.length) {
    const seq = Number(rows[index].seq)
    const atSeq: QueueRow[] = []
    while (index < rows.length && Number(rows[index].seq) === seq) {
      atSeq.push(rows[index])
      index += 1
    }
    groups.push({ seq, chosen: canonical(atSeq) })
  }
  return groups
}

function applyChosen(stock: Stock, row: QueueRow): Stock {
  if (row.kind === 'snapshot') return { ...(row.payload.stock ?? {}) }
  if (row.kind === 'delta') return applyDelta(stock, row.payload)
  throw new Error(`unknown kind ${row.kind}`)
}

// Same walk as the processor. A future receipt stops the store. A null receipt
// on the contiguous next seq walks later snapshots: the first non-null receipt
// is the candidate, a future candidate waits, and a null snapshot is not a baseline.
function replayStore(rows: QueueRow[]): StoreReplay {
  const first = rows[0]
  const groups = groupBySeq(rows)
  const bySeq = new Map(groups.map((group) => [group.seq, group]))
  let stock: Stock = {}
  const applied: AppliedUpdate[] = []
  let expectedSeq = 1

  for (;;) {
    const group = bySeq.get(expectedSeq)
    if (!group) break

    const chosen = group.chosen
    if (chosen.receipt === 'future') break
    if (chosen.receipt === 'ready') {
      stock = applyChosen(stock, chosen)
      applied.push({
        messageId: chosen.message_id,
        seq: group.seq,
        kind: chosen.kind === 'snapshot' ? 'snapshot' : 'delta',
      })
      expectedSeq += 1
      continue
    }
    if (chosen.receipt !== 'null') {
      throw new Error(`unexpected receipt state ${String(chosen.receipt)}`)
    }

    const candidate = groups.find(
      (later) => later.seq > expectedSeq && later.chosen.kind === 'snapshot' && later.chosen.receipt !== 'null',
    )
    if (!candidate || candidate.chosen.receipt === 'future') break
    if (candidate.chosen.receipt !== 'ready') {
      throw new Error(`unexpected snapshot receipt state ${String(candidate.chosen.receipt)}`)
    }

    stock = { ...(candidate.chosen.payload.stock ?? {}) }
    applied.push({
      messageId: candidate.chosen.message_id,
      seq: candidate.seq,
      kind: 'snapshot',
    })
    expectedSeq = candidate.seq + 1
  }

  return {
    chainId: first.chain_id,
    storeId: first.store_id,
    stock,
    applied,
  }
}

export async function replayQueue(client: Client): Promise<StoreReplay[]> {
  const result = await client.query<QueueRow>(
    `SELECT m.id::text AS id,
            m.message_id::text AS message_id,
            m.chain_id::text AS chain_id,
            m.store_id::text AS store_id,
            m.seq,
            m.kind,
            m.payload,
            CASE
              WHEN r.ready_at IS NULL THEN 'null'
              WHEN r.ready_at <= now() THEN 'ready'
              ELSE 'future'
            END AS receipt
     FROM queue_messages m
     JOIN receipt_files r ON r.id = m.receipt_id
     ORDER BY m.chain_id, m.store_id, m.seq, m.id`,
  )

  const grouped = new Map<string, QueueRow[]>()
  for (const row of result.rows) {
    const key = `${row.chain_id}:${row.store_id}`
    const list = grouped.get(key)
    if (list) list.push(row)
    else grouped.set(key, [row])
  }

  return [...grouped.values()].map((rows) => replayStore(rows))
}
