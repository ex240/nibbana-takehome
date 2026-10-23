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

// Next contiguous seq that is already ready. Ordered differently per process so
// workers under contention try different stores first.
const FIND_READY_STORES = `
  SELECT stores.chain_id AS "chainId", stores.store_id AS "storeId"
  FROM (
    SELECT DISTINCT chain_id, store_id
    FROM queue_messages
  ) AS stores
  LEFT JOIN store_stock AS s
    ON s.chain_id = stores.chain_id
   AND s.store_id = stores.store_id
  WHERE EXISTS (
    SELECT 1
    FROM queue_messages AS m
    JOIN receipt_files AS r ON r.id = m.receipt_id
    WHERE m.chain_id = stores.chain_id
      AND m.store_id = stores.store_id
      AND m.seq = COALESCE(s.last_seq, 0) + 1
      AND r.ready_at IS NOT NULL
      AND r.ready_at <= now()
  )
  ORDER BY hashtextextended(stores.chain_id::text || ':' || stores.store_id::text, $1::bigint)
`

// Null ready_at is not a wait. A future ready_at on last_seq+1 is.
const PENDING_SQL = `
  WITH next_receipt AS (
    SELECT DISTINCT ON (m.chain_id, m.store_id)
           r.ready_at
    FROM queue_messages AS m
    JOIN receipt_files AS r ON r.id = m.receipt_id
    LEFT JOIN store_stock AS s
      ON s.chain_id = m.chain_id
     AND s.store_id = m.store_id
    WHERE m.seq = COALESCE(s.last_seq, 0) + 1
    ORDER BY m.chain_id, m.store_id, m.id
  )
  SELECT
    (COUNT(*) FILTER (WHERE ready_at <= now()))::int AS ready,
    (
      CEIL(
        EXTRACT(EPOCH FROM (MIN(ready_at) FILTER (WHERE ready_at > now()) - now())) * 1000
      )
    )::int AS wait_ms
  FROM next_receipt
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

// True when some store's next seq is ready now or becomes ready later.
// A null receipt does not count: this commit does not walk past it.
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
