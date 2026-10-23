// Seeds the queue deterministically (SEED env var, default 42).
// Usage: node seed.js
//
// Do not modify this file.

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import pg from 'pg'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'
const SEED = Number(process.env.SEED ?? 42)
const POISON = process.argv.includes('--poison')

const CHAINS = 4
const STORES_PER_CHAIN = 6
const MIN_MSGS = 40
const MAX_MSGS = 110
const RESNAPSHOT_PROB = 0.25
const DUP_RATE = 0.05
const DELAY_RATE = 0.12
const DELAY_MIN_S = 3
const DELAY_MAX_S = 25

const ITEMS = ['SKU-4711', 'SKU-1002', 'SKU-2205', 'SKU-8813', 'SKU-9034', 'promo_code']

// --- deterministic RNG (mulberry32) ---
function mulberry32(a) {
  return function () {
    a |= 0; a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
const rnd = mulberry32(SEED)
const ri = (min, max) => min + Math.floor(rnd() * (max - min + 1))

function uuid() {
  const h = Array.from({ length: 32 }, () => Math.floor(rnd() * 16).toString(16))
  h[12] = '4'
  h[16] = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16)
  return `${h.slice(0, 8).join('')}-${h.slice(8, 12).join('')}-${h.slice(12, 16).join('')}-${h.slice(16, 20).join('')}-${h.slice(20).join('')}`
}

function itemValue(item) {
  if (item === 'promo_code') return `v${ri(1, 4)}.${ri(0, 9)}`
  return ri(0, 4000) / 10
}

function snapshotStock() {
  const n = ri(3, ITEMS.length)
  const picked = [...ITEMS].sort(() => rnd() - 0.5).slice(0, n)
  return Object.fromEntries(picked.map((f) => [f, itemValue(f)]))
}

function deltaPayload() {
  const set = Object.fromEntries(
    Array.from({ length: ri(1, 3) }, () => {
      const f = ITEMS[ri(0, ITEMS.length - 1)]
      return [f, itemValue(f)]
    }),
  )
  const unset = rnd() < 0.25 ? [ITEMS[ri(0, ITEMS.length - 1)]] : []
  return { set, unset }
}

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1))
    ;[arr[i], arr[j]] = [arr[j], arr[i]]
  }
  return arr
}

// --- build the stream ---
const chains = Array.from({ length: CHAINS }, uuid)
const messages = []
const receipts = []

for (const chainId of chains) {
  for (let s = 0; s < STORES_PER_CHAIN; s++) {
    const storeId = uuid()
    const n = ri(MIN_MSGS, MAX_MSGS)
    const resnapAt = rnd() < RESNAPSHOT_PROB ? ri(10, n - 5) : -1
    for (let seq = 1; seq <= n; seq++) {
      const isSnap = seq === 1 || seq === resnapAt
      const receipt = {
        id: uuid(),
        readyOffsetS: rnd() < DELAY_RATE ? ri(DELAY_MIN_S, DELAY_MAX_S) : -3600,
      }
      receipts.push(receipt)
      messages.push({
        messageId: uuid(),
        chainId,
        storeId,
        seq,
        kind: isSnap ? 'snapshot' : 'delta',
        payload: isSnap ? { stock: snapshotStock() } : deltaPayload(),
        receiptId: receipt.id,
      })
    }
  }
}

if (POISON) {
  const candidates = messages.filter((m) => m.seq > 5 && m.seq < 30)
  const victim = candidates[ri(0, candidates.length - 1)]
  const receipt = receipts.find((x) => x.id === victim.receiptId)
  receipt.readyOffsetS = null
  console.log(`poison: message seq=${victim.seq} of store ${victim.storeId} will never be ready`)
}

const dups = shuffle([...messages]).slice(0, Math.floor(messages.length * DUP_RATE))
const rows = shuffle([...messages, ...dups])

// --- write to the DB ---
const client = new pg.Client({ connectionString: DATABASE_URL })
await client.connect()
try {
  await client.query(fs.readFileSync(path.join(__dirname, 'schema.sql'), 'utf8'))
  await client.query('TRUNCATE queue_messages, apply_log, store_stock, receipt_files RESTART IDENTITY CASCADE')

  for (let i = 0; i < receipts.length; i += 500) {
    const chunk = receipts.slice(i, i + 500)
    const params = []
    const values = chunk.map((r, j) => {
      params.push(r.id, r.readyOffsetS)
      return `($${j * 2 + 1}, CASE WHEN $${j * 2 + 2}::int IS NULL THEN NULL ELSE now() + make_interval(secs => $${j * 2 + 2}::int) END)`
    })
    await client.query(`INSERT INTO receipt_files (id, ready_at) VALUES ${values.join(',')}`, params)
  }

  for (let i = 0; i < rows.length; i += 500) {
    const chunk = rows.slice(i, i + 500)
    const params = []
    const values = chunk.map((m, j) => {
      const b = j * 7
      params.push(m.messageId, m.chainId, m.storeId, m.seq, m.kind, JSON.stringify(m.payload), m.receiptId)
      return `($${b + 1}, $${b + 2}, $${b + 3}, $${b + 4}, $${b + 5}, $${b + 6}::jsonb, $${b + 7})`
    })
    await client.query(
      `INSERT INTO queue_messages (message_id, chain_id, store_id, seq, kind, payload, receipt_id) VALUES ${values.join(',')}`,
      params,
    )
  }

  console.log(
    `seeded: ${rows.length} queue rows across ${CHAINS * STORES_PER_CHAIN} stores (${CHAINS} chains)`,
  )
  console.log('gateway activity continues for ~30s — start your app now')
} finally {
  await client.end()
}
