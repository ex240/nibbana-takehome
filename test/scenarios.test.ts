import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { once } from 'node:events'
import fs from 'node:fs/promises'
import path from 'node:path'
import { describe, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import pg from 'pg'
import { applyDelta, replayQueue, type Stock } from './oracle.js'

const { Client } = pg

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'

// Prefer 42. Later values are only used when that seed has no second snapshot.
const SEED_CANDIDATES = [42, ...Array.from({ length: 40 }, (_, i) => i + 1).filter((n) => n !== 42)]

function run(command: string, args: string[], env: NodeJS.ProcessEnv = {}): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: root,
      env: { ...process.env, ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let out = ''
    child.stdout.on('data', (chunk: Buffer) => {
      out += chunk.toString()
    })
    child.stderr.on('data', (chunk: Buffer) => {
      out += chunk.toString()
    })
    child.on('error', reject)
    child.on('exit', (code) => {
      if (code === 0) resolve(out)
      else reject(new Error(`${command} ${args.join(' ')} exited ${code}\n${out}`))
    })
  })
}

async function waitForPostgres(): Promise<void> {
  const deadline = Date.now() + 90_000
  let last = 'not started'
  while (Date.now() < deadline) {
    const client = new Client({ connectionString: DATABASE_URL })
    try {
      await client.connect()
      await client.query('SELECT 1')
      await client.end()
      return
    } catch (err) {
      last = err instanceof Error ? err.message : String(err)
      try {
        await client.end()
      } catch {
        // Connection never opened.
      }
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
  throw new Error(`postgres did not become ready: ${last}`)
}

type QueueShape = {
  second: boolean
  future: number
}

async function queueShape(client: pg.Client): Promise<QueueShape> {
  const result = await client.query<{ second: boolean; future: number }>(
    `SELECT
       EXISTS (
         SELECT 1
         FROM queue_messages
         WHERE kind = 'snapshot'
         GROUP BY chain_id, store_id
         HAVING COUNT(DISTINCT seq) >= 2
       ) AS second,
       (
         SELECT count(*)::int
         FROM receipt_files
         WHERE ready_at > now() + interval '2 seconds'
       ) AS future`,
  )
  return result.rows[0]
}

function startFleet(track: (child: ChildProcess) => void): Promise<string> {
  const child = spawn('npm', ['start'], {
    cwd: root,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true,
  })
  track(child)
  let output = ''
  child.stdout?.on('data', (chunk: Buffer) => {
    output += chunk.toString()
  })
  child.stderr?.on('data', (chunk: Buffer) => {
    output += chunk.toString()
  })

  return new Promise((resolve, reject) => {
    let settled = false
    const stop = (): void => {
      if (child.exitCode !== null || child.pid === undefined) return
      try {
        process.kill(-child.pid, 'SIGTERM')
      } catch {
        child.kill('SIGTERM')
      }
    }
    const finish = (error?: Error): void => {
      if (settled) return
      settled = true
      stop()
      if (error) reject(error)
      else resolve(output)
    }
    child.once('error', (err) => finish(err))
    child.once('close', (code) => {
      if (code === 0) finish()
      else finish(new Error(`npm start exited ${code}\n${output}`))
    })
  })
}

async function hasSecondSnapshot(client: pg.Client): Promise<boolean> {
  const result = await client.query<{ ok: boolean }>(
    `SELECT EXISTS (
       SELECT 1
       FROM queue_messages
       WHERE kind = 'snapshot'
       GROUP BY chain_id, store_id
       HAVING COUNT(DISTINCT seq) >= 2
     ) AS ok`,
  )
  return result.rows[0].ok
}

describe('ready backlog', { concurrency: 1 }, () => {
  test('a delta keeps the set quantity when the same key is unset', () => {
    const stock = applyDelta(
      { 'SKU-4711': 4, 'SKU-1002': 1 },
      { set: { 'SKU-4711': 9 }, unset: ['SKU-4711'] },
    )
    assert.deepStrictEqual(stock, { 'SKU-4711': 9, 'SKU-1002': 1 })
  })

  test('one seeded run matches the oracle', { timeout: 300_000 }, async () => {
    const oracleSource = await fs.readFile(new URL('./oracle.ts', import.meta.url), 'utf8')
    assert.equal(
      /(?:import|require)\s*(?:[^'"\n]*from\s*)?['"][^'"]*src(?:\/|['"])/.test(oracleSource),
      false,
    )

    await run('docker', ['compose', 'up', '-d', '--wait'])
    await waitForPostgres()

    const client = new Client({ connectionString: DATABASE_URL })
    await client.connect()
    let child: ChildProcess | undefined
    try {
      let seed = 0
      for (const candidate of SEED_CANDIDATES) {
        await run(process.execPath, ['seed.js'], { SEED: String(candidate) })
        await client.query(
          `UPDATE receipt_files
           SET ready_at = now() - interval '1 hour'
           WHERE ready_at IS NOT NULL`,
        )
        if (await hasSecondSnapshot(client)) {
          seed = candidate
          break
        }
      }
      assert.ok(seed !== 0, 'no candidate seed has a second snapshot')
      assert.equal(await hasSecondSnapshot(client), true, `SEED=${seed} has no second snapshot`)

      const duplicates = await client.query<{ n: number }>(
        `SELECT count(*)::int AS n
         FROM (
           SELECT message_id
           FROM queue_messages
           GROUP BY message_id
           HAVING count(*) > 1
         ) d`,
      )
      assert.ok(duplicates.rows[0].n > 0, `SEED=${seed} has no duplicate deliveries`)

      const before = await client.query<{ stock: number; logged: number }>(
        `SELECT
           (SELECT count(*)::int FROM store_stock) AS stock,
           (SELECT count(*)::int FROM apply_log) AS logged`,
      )
      assert.equal(before.rows[0].stock, 0)
      assert.equal(before.rows[0].logged, 0)

      child = spawn(process.execPath, ['--import', 'tsx', 'src/worker.ts'], {
        cwd: root,
        env: process.env,
        stdio: ['ignore', 'pipe', 'pipe'],
      })
      let output = ''
      child.stdout?.on('data', (chunk: Buffer) => {
        output += chunk.toString()
      })
      child.stderr?.on('data', (chunk: Buffer) => {
        output += chunk.toString()
      })
      const [code] = (await once(child, 'exit')) as [number | null]
      assert.equal(code, 0, output)

      const expected = await replayQueue(client)
      assert.ok(expected.some((store) => store.applied.filter((row) => row.kind === 'snapshot').length >= 2))

      const stockRows = await client.query<{
        chain_id: string
        store_id: string
        stock: Stock
        version: number
        last_seq: number
      }>(
        `SELECT chain_id::text, store_id::text, stock, version, last_seq
         FROM store_stock`,
      )
      const logRows = await client.query<{
        message_id: string
        chain_id: string
        store_id: string
        seq: number
        kind: string
        worker_id: string
      }>(
        `SELECT message_id::text, chain_id::text, store_id::text, seq, kind, worker_id
         FROM apply_log
         ORDER BY chain_id, store_id, seq`,
      )

      const actualByStore = new Map(stockRows.rows.map((row) => [`${row.chain_id}:${row.store_id}`, row]))
      const logByStore = new Map<string, typeof logRows.rows>()
      for (const row of logRows.rows) {
        const key = `${row.chain_id}:${row.store_id}`
        const list = logByStore.get(key)
        if (list) list.push(row)
        else logByStore.set(key, [row])
      }

      assert.equal(actualByStore.size, expected.length)
      const seenMessages = new Set<string>()
      for (const store of expected) {
        const key = `${store.chainId}:${store.storeId}`
        const actual = actualByStore.get(key)
        assert.ok(actual, `missing store_stock for ${key}`)
        assert.deepStrictEqual(actual.stock, store.stock, key)

        const logged = logByStore.get(key) ?? []
        assert.equal(actual.version, actual.last_seq, key)
        assert.equal(actual.version, logged.length, key)
        assert.equal(logged.length, store.applied.length, key)
        assert.deepStrictEqual(
          logged.map((row) => row.seq),
          store.applied.map((_, index) => index + 1),
          key,
        )
        assert.deepStrictEqual(
          logged.map((row) => ({ messageId: row.message_id, seq: row.seq, kind: row.kind })),
          store.applied,
          key,
        )
        for (const row of logged) {
          assert.equal(seenMessages.has(row.message_id), false, row.message_id)
          seenMessages.add(row.message_id)
          assert.equal(row.worker_id, String(child.pid))
        }
      }

      const queue = await client.query<{ rows: number; messages: number; applied: number; duplicate: number; pending: number }>(
        `SELECT
           count(*)::int AS rows,
           count(DISTINCT message_id)::int AS messages,
           count(*) FILTER (WHERE outcome = 'applied')::int AS applied,
           count(*) FILTER (WHERE outcome = 'duplicate')::int AS duplicate,
           count(*) FILTER (WHERE outcome IS NULL)::int AS pending
         FROM queue_messages`,
      )
      const totals = queue.rows[0]
      assert.ok(totals.rows > totals.messages)
      assert.equal(totals.pending, 0)
      assert.equal(totals.applied, logRows.rows.length)
      assert.equal(totals.applied + totals.duplicate, totals.rows)
      assert.equal(logRows.rows.length, totals.messages)

      const doubleApplied = await client.query<{ n: number }>(
        `SELECT count(*)::int AS n
         FROM (
           SELECT q.message_id
           FROM queue_messages q
           GROUP BY q.message_id
           HAVING count(*) > 1
              AND (SELECT count(*) FROM apply_log a WHERE a.message_id = q.message_id) <> 1
         ) bad`,
      )
      assert.equal(doubleApplied.rows[0].n, 0)

      const seqMarks = await client.query<{ n: number }>(
        `SELECT count(*)::int AS n
         FROM (
           SELECT chain_id, store_id, seq
           FROM queue_messages
           GROUP BY chain_id, store_id, seq
           HAVING count(*) FILTER (WHERE outcome = 'applied') <> 1
               OR count(*) FILTER (WHERE outcome = 'duplicate') <> count(*) - 1
         ) bad`,
      )
      assert.equal(seqMarks.rows[0].n, 0)

      const beforeRerun = await client.query<{
        stock_rows: number
        log_rows: number
        version_sum: string
        last_seq_sum: string
      }>(
        `SELECT
           (SELECT count(*)::int FROM store_stock) AS stock_rows,
           (SELECT count(*)::int FROM apply_log) AS log_rows,
           (SELECT coalesce(sum(version), 0)::text FROM store_stock) AS version_sum,
           (SELECT coalesce(sum(last_seq), 0)::text FROM store_stock) AS last_seq_sum`,
      )
      const stockBeforeRerun = await client.query(
        `SELECT chain_id::text, store_id::text, stock, version, last_seq
         FROM store_stock
         ORDER BY chain_id, store_id`,
      )
      const logBeforeRerun = await client.query(
        `SELECT apply_pos, message_id::text, chain_id::text, store_id::text, seq, kind, worker_id
         FROM apply_log
         ORDER BY apply_pos`,
      )

      child = spawn(process.execPath, ['--import', 'tsx', 'src/worker.ts'], {
        cwd: root,
        env: process.env,
        stdio: ['ignore', 'pipe', 'pipe'],
      })
      let secondOutput = ''
      child.stdout?.on('data', (chunk: Buffer) => {
        secondOutput += chunk.toString()
      })
      child.stderr?.on('data', (chunk: Buffer) => {
        secondOutput += chunk.toString()
      })
      const [secondCode] = (await once(child, 'exit')) as [number | null]
      assert.equal(secondCode, 0, secondOutput)
      assert.match(secondOutput, /applied 0/)

      const afterRerun = await client.query(
        `SELECT
           (SELECT count(*)::int FROM store_stock) AS stock_rows,
           (SELECT count(*)::int FROM apply_log) AS log_rows,
           (SELECT coalesce(sum(version), 0)::text FROM store_stock) AS version_sum,
           (SELECT coalesce(sum(last_seq), 0)::text FROM store_stock) AS last_seq_sum`,
      )
      const stockAfterRerun = await client.query(
        `SELECT chain_id::text, store_id::text, stock, version, last_seq
         FROM store_stock
         ORDER BY chain_id, store_id`,
      )
      const logAfterRerun = await client.query(
        `SELECT apply_pos, message_id::text, chain_id::text, store_id::text, seq, kind, worker_id
         FROM apply_log
         ORDER BY apply_pos`,
      )
      assert.deepStrictEqual(afterRerun.rows, beforeRerun.rows)
      assert.deepStrictEqual(stockAfterRerun.rows, stockBeforeRerun.rows)
      assert.deepStrictEqual(logAfterRerun.rows, logBeforeRerun.rows)
    } finally {
      if (child && child.exitCode === null && !child.killed) child.kill('SIGTERM')
      await client.end()
    }
  })

  test('four workers wait for future receipts', { timeout: 300_000 }, async (t) => {
    const oracleSource = await fs.readFile(new URL('./oracle.ts', import.meta.url), 'utf8')
    assert.equal(
      /(?:import|require)\s*(?:[^'"\n]*from\s*)?['"][^'"]*src(?:\/|['"])/.test(oracleSource),
      false,
    )

    let fleet: ChildProcess | undefined
    t.after(() => {
      if (fleet && fleet.exitCode === null && fleet.pid !== undefined) {
        try {
          process.kill(-fleet.pid, 'SIGTERM')
        } catch {
          fleet.kill('SIGTERM')
        }
      }
    })

    await run('docker', ['compose', 'up', '-d', '--wait'])
    await waitForPostgres()

    const client = new Client({ connectionString: DATABASE_URL })
    await client.connect()
    try {
      const runSeed = async (seed: number, expectSecond: boolean): Promise<void> => {
        await run(process.execPath, ['seed.js'], { SEED: String(seed) })
        const shape = await queueShape(client)
        assert.ok(shape.future > 0, `SEED=${seed} has no receipt still 2 seconds ahead`)
        if (expectSecond) {
          assert.equal(shape.second, true, `SEED=${seed} has no second snapshot`)
        }
        // Seed left ready_at where the gateway put it. Start before that window closes.
        const output = await startFleet((child) => {
          fleet = child
        })
        await assertFleet(client, seed, output, shape.second)
      }

      await runSeed(42, false)
      let other = 0
      for (const candidate of SEED_CANDIDATES) {
        if (candidate === 42) continue
        await run(process.execPath, ['seed.js'], { SEED: String(candidate) })
        const shape = await queueShape(client)
        if (!shape.second || shape.future === 0) continue
        other = candidate
        const output = await startFleet((child) => {
          fleet = child
        })
        await assertFleet(client, candidate, output, true)
        break
      }
      assert.notEqual(other, 0, 'no second seed has a snapshot and a future ready_at')
    } finally {
      await client.end()
    }
  })
})

async function assertFleet(
  client: pg.Client,
  seed: number,
  output: string,
  expectSecond: boolean,
): Promise<void> {
  const started = output.match(/^supervisor started_at=(\S+)$/m)
  assert.ok(started, output)
  const supervisorPid = output.match(/^supervisor pid=(\d+)$/m)
  assert.ok(supervisorPid, output)
  assert.match(output, /^fleet size=4$/m)

  const spawned = [...output.matchAll(/^spawned worker pid=(\d+)$/gm)].map((match) => match[1])
  assert.ok(spawned.length >= 4, `SEED=${seed} spawned ${spawned.length}\n${output}`)
  const exits = [...output.matchAll(/^worker pid=(\d+) exited code=(\S+) signal=/gm)]
  assert.ok(exits.length >= 4, output)
  for (const exit of exits) assert.equal(exit[2], '0', output)

  const expected = await replayQueue(client)
  if (expectSecond) {
    assert.ok(expected.some((store) => store.applied.filter((row) => row.kind === 'snapshot').length >= 2))
  }

  const stockRows = await client.query<{
    chain_id: string
    store_id: string
    stock: Stock
    version: number
    last_seq: number
  }>(
    `SELECT chain_id::text, store_id::text, stock, version, last_seq
     FROM store_stock`,
  )
  const logRows = await client.query<{
    message_id: string
    chain_id: string
    store_id: string
    seq: number
    kind: string
    worker_id: string
  }>(
    `SELECT message_id::text, chain_id::text, store_id::text, seq, kind, worker_id
     FROM apply_log
     ORDER BY chain_id, store_id, seq`,
  )

  const actualByStore = new Map(stockRows.rows.map((row) => [`${row.chain_id}:${row.store_id}`, row]))
  const logByStore = new Map<string, typeof logRows.rows>()
  for (const row of logRows.rows) {
    const key = `${row.chain_id}:${row.store_id}`
    const list = logByStore.get(key)
    if (list) list.push(row)
    else logByStore.set(key, [row])
  }

  assert.equal(actualByStore.size, expected.length, `SEED=${seed}`)
  const seenMessages = new Set<string>()
  const workerIds = new Set<string>()
  for (const store of expected) {
    const key = `${store.chainId}:${store.storeId}`
    const actual = actualByStore.get(key)
    assert.ok(actual, `missing store_stock for ${key}`)
    assert.deepStrictEqual(actual.stock, store.stock, key)
    const logged = logByStore.get(key) ?? []
    assert.equal(actual.version, actual.last_seq, key)
    assert.equal(actual.version, logged.length, key)
    assert.equal(logged.length, store.applied.length, key)
    assert.deepStrictEqual(
      logged.map((row) => row.seq),
      store.applied.map((row) => row.seq),
      key,
    )
    assert.deepStrictEqual(
      logged.map((row) => ({ messageId: row.message_id, seq: row.seq, kind: row.kind })),
      store.applied,
      key,
    )
    for (const row of logged) {
      assert.equal(seenMessages.has(row.message_id), false, row.message_id)
      seenMessages.add(row.message_id)
      assert.equal(row.worker_id === supervisorPid[1], false, `supervisor pid applied ${row.message_id}`)
      workerIds.add(row.worker_id)
    }
  }

  const futureApplied = await client.query<{ n: number }>(
    `SELECT count(DISTINCT a.message_id)::int AS n
     FROM apply_log a
     JOIN queue_messages m ON m.message_id = a.message_id
     JOIN receipt_files r ON r.id = m.receipt_id
     WHERE EXTRACT(EPOCH FROM r.ready_at) > $1::numeric`,
    [started[1]],
  )
  assert.ok(futureApplied.rows[0].n > 0, `SEED=${seed} applied nothing that was still in the future at start`)

  const early = await client.query<{ message_id: string; ready_at: Date; applied_at: Date }>(
    `SELECT a.message_id::text, r.ready_at, a.applied_at
     FROM apply_log a
     JOIN LATERAL (
       SELECT receipt_id
       FROM queue_messages m
       WHERE m.message_id = a.message_id
       ORDER BY m.id
       LIMIT 1
     ) q ON true
     JOIN receipt_files r ON r.id = q.receipt_id
     WHERE r.ready_at IS NULL OR r.ready_at > a.applied_at
     LIMIT 5`,
  )
  assert.deepStrictEqual(early.rows, [])

  assert.ok(workerIds.size >= 2, `SEED=${seed} worker_ids=${[...workerIds].join(',')}`)
  for (const workerId of workerIds) {
    assert.ok(spawned.includes(workerId), `SEED=${seed} apply_log worker ${workerId} was not spawned`)
  }
  if (workerIds.size < 4) {
    console.log(
      `note: SEED=${seed} apply_log has ${workerIds.size} worker_ids (${[...workerIds].join(', ')}); ` +
        `supervisor spawned ${spawned.length} (${spawned.join(', ')}). ` +
        `A worker can exit before it wins a store when the ready backlog finishes first.`,
    )
  } else {
    console.log(`SEED=${seed} apply_log worker_ids=${[...workerIds].join(',')} spawned=${spawned.join(',')}`)
  }

  const queue = await client.query<{
    rows: number
    messages: number
    applied: number
    duplicate: number
    pending: number
  }>(
    `SELECT
       count(*)::int AS rows,
       count(DISTINCT message_id)::int AS messages,
       count(*) FILTER (WHERE outcome = 'applied')::int AS applied,
       count(*) FILTER (WHERE outcome = 'duplicate')::int AS duplicate,
       count(*) FILTER (WHERE outcome IS NULL)::int AS pending
     FROM queue_messages`,
  )
  const totals = queue.rows[0]
  assert.ok(totals.rows > totals.messages, `SEED=${seed}`)
  assert.equal(totals.pending, 0, `SEED=${seed}`)
  assert.equal(totals.applied, logRows.rows.length, `SEED=${seed}`)
  assert.equal(totals.applied + totals.duplicate, totals.rows, `SEED=${seed}`)
  assert.equal(logRows.rows.length, totals.messages, `SEED=${seed}`)
}
