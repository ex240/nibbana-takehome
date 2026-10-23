import path from 'node:path'
import { pathToFileURL } from 'node:url'
import pg from 'pg'
import { applyNextUpdate } from './apply.js'
import { migrate } from './migrate.js'

const { Client } = pg

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'

const MAX_IDLE_MS = 500
const LOCK_WAIT_MS = 50

type StoreRef = {
  chainId: string
  storeId: string
}

type Pending = {
  ready: number
  waitMs: number | null
}

// Contiguous next receipt, or — only when that receipt is null — the earliest
// later snapshot whose receipt is not null. A future ready_at on the next seq
// hides every later snapshot. A future candidate hides snapshots after it.
const FOCUS_CTE = `
  WITH progress AS (
    SELECT stores.chain_id, stores.store_id, COALESCE(s.last_seq, 0) AS last_seq
    FROM (
      SELECT DISTINCT chain_id, store_id
      FROM queue_messages
    ) AS stores
    LEFT JOIN store_stock AS s
      ON s.chain_id = stores.chain_id
     AND s.store_id = stores.store_id
  ),
  next_receipt AS (
    SELECT DISTINCT ON (p.chain_id, p.store_id)
           p.chain_id, p.store_id, p.last_seq, r.ready_at
    FROM progress AS p
    JOIN queue_messages AS m
      ON m.chain_id = p.chain_id
     AND m.store_id = p.store_id
     AND m.seq = p.last_seq + 1
    JOIN receipt_files AS r ON r.id = m.receipt_id
    ORDER BY p.chain_id, p.store_id, m.id
  ),
  snapshot_rows AS (
    SELECT DISTINCT ON (n.chain_id, n.store_id, m.seq)
           n.chain_id, n.store_id, m.seq, r.ready_at
    FROM next_receipt AS n
    JOIN queue_messages AS m
      ON m.chain_id = n.chain_id
     AND m.store_id = n.store_id
     AND m.kind = 'snapshot'
     AND m.seq > n.last_seq
    JOIN receipt_files AS r ON r.id = m.receipt_id
    WHERE n.ready_at IS NULL
    ORDER BY n.chain_id, n.store_id, m.seq, m.id
  ),
  candidate AS (
    SELECT DISTINCT ON (chain_id, store_id)
           chain_id, store_id, ready_at
    FROM snapshot_rows
    WHERE ready_at IS NOT NULL
    ORDER BY chain_id, store_id, seq
  ),
  focus AS (
    SELECT n.chain_id, n.store_id,
           CASE
             WHEN n.ready_at IS NOT NULL THEN n.ready_at
             ELSE c.ready_at
           END AS ready_at
    FROM next_receipt AS n
    LEFT JOIN candidate AS c
      ON c.chain_id = n.chain_id
     AND c.store_id = n.store_id
  )
`

// Ordered differently per process so workers under contention try different stores first.
const FIND_READY_STORES = `
  ${FOCUS_CTE}
  SELECT focus.chain_id AS "chainId", focus.store_id AS "storeId"
  FROM focus
  WHERE focus.ready_at IS NOT NULL
    AND focus.ready_at <= now()
  ORDER BY hashtextextended(focus.chain_id::text || ':' || focus.store_id::text, $1::bigint)
`

const PENDING_SQL = `
  ${FOCUS_CTE}
  SELECT
    (COUNT(*) FILTER (WHERE ready_at <= now()))::int AS ready,
    (
      CEIL(
        EXTRACT(EPOCH FROM (MIN(ready_at) FILTER (WHERE ready_at > now()) - now())) * 1000
      )
    )::int AS wait_ms
  FROM focus
  WHERE ready_at IS NOT NULL
`

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

function sameStore(left: StoreRef | null, right: StoreRef): boolean {
  return left !== null && left.chainId === right.chainId && left.storeId === right.storeId
}

function orderStores(stores: StoreRef[], prefer: StoreRef | null): StoreRef[] {
  if (!prefer) return stores
  const index = stores.findIndex((store) => sameStore(prefer, store))
  if (index <= 0) return stores
  const next = stores.slice()
  const [hit] = next.splice(index, 1)
  next.unshift(hit)
  return next
}

async function findReadyStores(client: pg.Client): Promise<StoreRef[]> {
  const found = await client.query<StoreRef>(FIND_READY_STORES, [process.pid])
  return found.rows
}

async function readPending(client: pg.Client): Promise<Pending> {
  const result = await client.query<{ ready: number; wait_ms: number | null }>(PENDING_SQL)
  const row = result.rows[0]
  return {
    ready: Number(row?.ready ?? 0),
    waitMs: row?.wait_ms === null || row?.wait_ms === undefined ? null : Number(row.wait_ms),
  }
}

// True when some store's next seq is ready or delayed, or a null next seq has
// a snapshot candidate that is ready or delayed. A hole with no such candidate
// does not keep the process alive.
export async function hasActionableReceipt(client: pg.Client): Promise<boolean> {
  const pending = await readPending(client)
  return pending.ready > 0 || pending.waitMs !== null
}

export async function drainQueue(client: pg.Client): Promise<number> {
  let applied = 0
  let prefer: StoreRef | null = null

  for (;;) {
    const ready = orderStores(await findReadyStores(client), prefer)
    let locked = 0
    let acted = false

    for (const store of ready) {
      const status = await applyNextUpdate(client, store.chainId, store.storeId)
      if (status === 'locked') {
        locked += 1
        continue
      }
      acted = true
      if (status === 'applied') {
        applied += 1
        prefer = store
      } else if (sameStore(prefer, store)) {
        prefer = null
      }
      break
    }

    if (acted) continue
    if (locked > 0) {
      await sleep(LOCK_WAIT_MS)
      continue
    }

    const pending = await readPending(client)
    if (pending.ready > 0) continue
    if (pending.waitMs !== null) {
      await sleep(Math.min(MAX_IDLE_MS, Math.max(0, pending.waitMs)))
      continue
    }

    const confirm = await readPending(client)
    if (confirm.ready > 0 || confirm.waitMs !== null) continue
    return applied
  }
}

export async function main(): Promise<void> {
  const client = new Client({ connectionString: DATABASE_URL })
  await client.connect()
  try {
    await migrate(client)
    const applied = await drainQueue(client)
    console.log(`applied ${applied} updates`)
  } finally {
    await client.end()
  }
}

function launchedDirectly(): boolean {
  const entry = process.argv[1]
  if (!entry) return false
  return import.meta.url === pathToFileURL(path.resolve(entry)).href
}

if (launchedDirectly()) {
  main()
    .then(() => {
      process.exit(0)
    })
    .catch((err: unknown) => {
      console.error(err)
      process.exit(1)
    })
}
