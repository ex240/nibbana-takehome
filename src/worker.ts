import path from 'node:path'
import { pathToFileURL } from 'node:url'
import pg from 'pg'
import { applyNextUpdate } from './apply.js'
import { migrate } from './migrate.js'

const { Client } = pg

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'

type ReadyStore = {
  chainId: string
  storeId: string
}

// Next contiguous seq whose receipt is already ready. Future ready_at is not a wait.
const FIND_READY_STORE = `
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
  ORDER BY stores.chain_id, stores.store_id
  LIMIT 1
`

export async function drainReadyBacklog(client: pg.Client): Promise<number> {
  let applied = 0
  for (;;) {
    const found = await client.query<ReadyStore>(FIND_READY_STORE)
    const store = found.rows[0]
    if (!store) return applied

    const status = await applyNextUpdate(client, store.chainId, store.storeId)
    if (status !== 'applied') {
      throw new Error(`store ${store.chainId}/${store.storeId} looked ready but apply returned ${status}`)
    }
    applied += 1
  }
}

export async function main(): Promise<void> {
  const client = new Client({ connectionString: DATABASE_URL })
  await client.connect()
  try {
    await migrate(client)
    const applied = await drainReadyBacklog(client)
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
