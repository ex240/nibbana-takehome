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

type QueueRow = {
  id: string
  message_id: string
  chain_id: string
  store_id: string
  seq: number
  kind: string
  payload: { stock?: Stock; set?: Stock; unset?: string[] }
  ready: boolean
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

function replayStore(rows: QueueRow[]): StoreReplay {
  const first = rows[0]
  let stock: Stock = {}
  const applied: AppliedUpdate[] = []
  let expectedSeq = 1
  let index = 0

  while (index < rows.length) {
    const seq = Number(rows[index].seq)
    if (seq !== expectedSeq) break

    const atSeq: QueueRow[] = []
    while (index < rows.length && Number(rows[index].seq) === seq) {
      atSeq.push(rows[index])
      index += 1
    }

    const chosen = canonical(atSeq)
    if (!chosen.ready) break

    if (chosen.kind === 'snapshot') {
      stock = { ...(chosen.payload.stock ?? {}) }
    } else if (chosen.kind === 'delta') {
      stock = applyDelta(stock, chosen.payload)
    } else {
      throw new Error(`unknown kind ${chosen.kind}`)
    }

    applied.push({
      messageId: chosen.message_id,
      seq,
      kind: chosen.kind,
    })
    expectedSeq += 1
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
            (r.ready_at IS NOT NULL AND r.ready_at <= now()) AS ready
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
