import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { randomUUID } from 'node:crypto'
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

  // Locked after classifying --poison for seeds 1..48. The shape is re-read
  // from seed.js stdout and the rows; these two are the pair that covers both.
  test('poison resumes at a later snapshot or leaves the prefix', { timeout: 300_000 }, async (t) => {
    let fleet: ChildProcess | undefined
    t.after(() => stopFleet(fleet))

    await run('docker', ['compose', 'up', '-d', '--wait'])
    await waitForPostgres()
    const client = new Client({ connectionString: DATABASE_URL })
    await client.connect()
    try {
      const shapes = new Set<PoisonShape>()
      for (const seed of [2, 42]) {
        const found = await runPoisonSeed(client, seed, (child) => {
          fleet = child
        })
        shapes.add(found)
      }
      assert.deepStrictEqual([...shapes].sort(), ['prefix', 'resume'])
    } finally {
      await client.end()
    }
  })

  test('a delayed snapshot candidate blocks a later ready snapshot', { timeout: 300_000 }, async (t) => {
    let fleet: ChildProcess | undefined
    t.after(() => stopFleet(fleet))

    await run('docker', ['compose', 'up', '-d', '--wait'])
    await waitForPostgres()
    const client = new Client({ connectionString: DATABASE_URL })
    await client.connect()
    try {
      await run(process.execPath, ['seed.js'], { SEED: '42' })
      const fixture = await insertWalkFixture(client, 'delayed')
      const early = await replayQueue(client)
      const earlyStore = early.find((store) => store.storeId === fixture.storeId)
      assert.ok(earlyStore)
      assert.deepStrictEqual(
        earlyStore.applied.map((row) => row.seq),
        [1, 2],
        'oracle applied S2 while S1 was still delayed',
      )

      const fleetDone = startFleet((child) => {
        fleet = child
      })
      const watched = fleetDone.then(
        (output) => ({ output, error: undefined as Error | undefined }),
        (error: Error) => ({ output: '', error }),
      )
      const sawHold = await watchDelayedHold(client, fixture)
      const result = await watched
      if (result.error) throw result.error
      assert.equal(sawHold, true, 'never saw the prefix held while S1 was still delayed')

      await assertMatchesReplay(client, 42, result.output)
      await assertWalkStore(client, fixture, 'delayed')
      const counters = await storeCounters(client, fixture.chainId, fixture.storeId)
      console.log(
        `fixture delayed store=${fixture.storeId} sawHold=${sawHold} s2BlockedUntilS1=true version=${counters.version} last_seq=${counters.last_seq}`,
      )
    } finally {
      await client.end()
    }
  })

  test('a null snapshot candidate yields to the next ready snapshot', { timeout: 300_000 }, async (t) => {
    let fleet: ChildProcess | undefined
    t.after(() => stopFleet(fleet))

    await run('docker', ['compose', 'up', '-d', '--wait'])
    await waitForPostgres()
    const client = new Client({ connectionString: DATABASE_URL })
    await client.connect()
    try {
      await run(process.execPath, ['seed.js'], { SEED: '42' })
      const fixture = await insertWalkFixture(client, 'failed')
      const early = await replayQueue(client)
      const earlyStore = early.find((store) => store.storeId === fixture.storeId)
      assert.ok(earlyStore)
      assert.deepStrictEqual(
        earlyStore.applied.map((row) => row.seq),
        [1, 2, 6],
        'oracle waited on a null snapshot instead of applying S2',
      )
      assert.deepStrictEqual(earlyStore.stock, fixture.s2Stock)

      const output = await startFleet((child) => {
        fleet = child
      })
      await assertMatchesReplay(client, 42, output)
      await assertWalkStore(client, fixture, 'failed')
      const counters = await storeCounters(client, fixture.chainId, fixture.storeId)
      console.log(
        `fixture failed store=${fixture.storeId} s2AppliedWithoutWaiting=true version=${counters.version} last_seq=${counters.last_seq}`,
      )
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

type PoisonShape = 'resume' | 'prefix'

type WalkFixture = {
  chainId: string
  storeId: string
  s1Receipt: string
  s2Stock: Stock
  ids: {
    prefixSnap: string
    prefixDelta: string
    poison: string
    s1: string
    delta: string
    s2: string
  }
}

function stopFleet(fleet: ChildProcess | undefined): void {
  if (fleet && fleet.exitCode === null && fleet.pid !== undefined) {
    try {
      process.kill(-fleet.pid, 'SIGTERM')
    } catch {
      fleet.kill('SIGTERM')
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

async function runPoisonSeed(
  client: pg.Client,
  seed: number,
  track: (child: ChildProcess) => void,
): Promise<PoisonShape> {
  const seeded = await run(process.execPath, ['seed.js', '--poison'], { SEED: String(seed) })
  const line = seeded.match(/poison: message seq=(\d+) of store ([0-9a-f-]{36})/)
  assert.ok(line, seeded)
  const poisonSeq = Number(line[1])
  const storeId = line[2]

  const holes = await client.query<{ chain_id: string; store_id: string; seq: number }>(
    `SELECT DISTINCT m.chain_id::text, m.store_id::text, m.seq
     FROM queue_messages m
     JOIN receipt_files r ON r.id = m.receipt_id
     WHERE r.ready_at IS NULL`,
  )
  assert.equal(holes.rows.length, 1, `SEED=${seed} null receipts`)
  assert.equal(holes.rows[0].store_id, storeId)
  assert.equal(Number(holes.rows[0].seq), poisonSeq)
  const chainId = holes.rows[0].chain_id

  const later = await client.query<{ resume: boolean; snap_seq: number | null }>(
    `SELECT
       EXISTS (
         SELECT 1
         FROM queue_messages m
         JOIN receipt_files r ON r.id = m.receipt_id
         WHERE m.chain_id = $1::uuid
           AND m.store_id = $2::uuid
           AND m.kind = 'snapshot'
           AND m.seq > $3::int
           AND r.ready_at IS NOT NULL
       ) AS resume,
       (
         SELECT min(m.seq)
         FROM queue_messages m
         JOIN receipt_files r ON r.id = m.receipt_id
         WHERE m.chain_id = $1::uuid
           AND m.store_id = $2::uuid
           AND m.kind = 'snapshot'
           AND m.seq > $3::int
           AND r.ready_at IS NOT NULL
       ) AS snap_seq`,
    [chainId, storeId, poisonSeq],
  )
  const shape: PoisonShape = later.rows[0].resume ? 'resume' : 'prefix'

  const output = await startFleet(track)
  await assertMatchesReplay(client, seed, output)

  const counters = await storeCounters(client, chainId, storeId)
  if (shape === 'resume') {
    assert.ok(later.rows[0].snap_seq !== null)
    assert.ok(counters.last_seq > counters.version, `SEED=${seed} resume did not jump`)
    assert.ok(counters.last_seq > poisonSeq, `SEED=${seed} last_seq stayed on the prefix`)
    const skipped = await client.query<{ n: number }>(
      `SELECT count(*)::int AS n
       FROM queue_messages
       WHERE chain_id = $1::uuid AND store_id = $2::uuid
         AND seq >= $3::int AND seq < $4::int
         AND outcome IS DISTINCT FROM 'skipped'`,
      [chainId, storeId, poisonSeq, later.rows[0].snap_seq],
    )
    assert.equal(skipped.rows[0].n, 0, `SEED=${seed} gap was not marked skipped`)
  } else {
    assert.equal(counters.version, counters.last_seq, `SEED=${seed} prefix counters diverged`)
    assert.equal(counters.last_seq, poisonSeq - 1, `SEED=${seed} prefix cursor`)
    const tail = await client.query<{ marked: number; skipped: number }>(
      `SELECT
         count(*) FILTER (WHERE outcome IS NOT NULL)::int AS marked,
         count(*) FILTER (WHERE outcome = 'skipped')::int AS skipped
       FROM queue_messages
       WHERE chain_id = $1::uuid AND store_id = $2::uuid AND seq >= $3::int`,
      [chainId, storeId, poisonSeq],
    )
    assert.equal(tail.rows[0].marked, 0, `SEED=${seed} marked a prefix it did not resume`)
    assert.equal(tail.rows[0].skipped, 0)
  }

  const poisonLogged = await client.query<{ n: number }>(
    `SELECT count(*)::int AS n
     FROM apply_log
     WHERE chain_id = $1::uuid AND store_id = $2::uuid AND seq = $3::int`,
    [chainId, storeId, poisonSeq],
  )
  assert.equal(poisonLogged.rows[0].n, 0, `SEED=${seed} billed the poisoned seq`)

  const constraints = await client.query<{ conname: string; def: string }>(
    `SELECT conname, pg_get_constraintdef(oid) AS def
     FROM pg_constraint
     WHERE conname IN (
       'queue_messages_outcome_check',
       'apply_log_message_id_unique',
       'apply_log_store_seq_unique'
     )`,
  )
  assert.equal(constraints.rows.length, 3)
  const check = constraints.rows.find((row) => row.conname === 'queue_messages_outcome_check')
  assert.ok(check?.def.includes('skipped'))

  console.log(
    `poison seed=${seed} shape=${shape} seq=${poisonSeq} store=${storeId} version=${counters.version} last_seq=${counters.last_seq}`,
  )
  return shape
}

async function storeCounters(
  client: pg.Client,
  chainId: string,
  storeId: string,
): Promise<{ version: number; last_seq: number; stock: Stock }> {
  const result = await client.query<{ version: number; last_seq: number; stock: Stock }>(
    `SELECT version, last_seq, stock
     FROM store_stock
     WHERE chain_id = $1::uuid AND store_id = $2::uuid`,
    [chainId, storeId],
  )
  assert.equal(result.rows.length, 1, `${chainId}/${storeId}`)
  return result.rows[0]
}

async function assertMatchesReplay(client: pg.Client, seed: number, output: string): Promise<void> {
  const started = output.match(/^supervisor started_at=(\S+)$/m)
  assert.ok(started, output)
  const supervisorPid = output.match(/^supervisor pid=(\d+)$/m)
  assert.ok(supervisorPid, output)
  assert.match(output, /^fleet size=4$/m)
  const spawned = [...output.matchAll(/^spawned worker pid=(\d+)$/gm)].map((match) => match[1])
  assert.ok(spawned.length >= 4, output)
  for (const exit of output.matchAll(/^worker pid=(\d+) exited code=(\S+) signal=/gm)) {
    assert.equal(exit[2], '0', output)
  }

  const expected = await replayQueue(client)
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
     ORDER BY apply_pos`,
  )

  const actualByStore = new Map(stockRows.rows.map((row) => [`${row.chain_id}:${row.store_id}`, row]))
  const logByStore = new Map<string, typeof logRows.rows>()
  for (const row of logRows.rows) {
    const key = `${row.chain_id}:${row.store_id}`
    const list = logByStore.get(key)
    if (list) list.push(row)
    else logByStore.set(key, [row])
  }

  const seenMessages = new Set<string>()
  const workerIds = new Set<string>()
  for (const store of expected) {
    const key = `${store.chainId}:${store.storeId}`
    const logged = logByStore.get(key) ?? []
    if (store.applied.length === 0) {
      assert.equal(actualByStore.has(key), false, key)
      assert.equal(logged.length, 0, key)
      continue
    }
    const actual = actualByStore.get(key)
    assert.ok(actual, `missing store_stock for ${key}`)
    assert.deepStrictEqual(actual.stock, store.stock, key)
    assert.equal(actual.version, store.applied.length, key)
    assert.equal(actual.version, logged.length, key)
    assert.equal(actual.last_seq, store.applied[store.applied.length - 1].seq, key)
    const contiguous = store.applied.every((row, index) => row.seq === index + 1)
    if (contiguous) assert.equal(actual.version, actual.last_seq, key)
    else assert.ok(actual.last_seq > actual.version, key)
    assert.deepStrictEqual(
      logged.map((row) => ({ messageId: row.message_id, seq: row.seq, kind: row.kind })),
      store.applied,
      key,
    )
    const appliedSeqs = new Set(store.applied.map((row) => row.seq))
    for (const row of store.applied) {
      if (row.kind === 'delta') {
        assert.equal(appliedSeqs.has(row.seq - 1), true, `${key} delta ${row.seq}`)
      }
    }
    for (const row of logged) {
      assert.equal(seenMessages.has(row.message_id), false, row.message_id)
      seenMessages.add(row.message_id)
      assert.equal(row.worker_id === supervisorPid[1], false, row.message_id)
      assert.ok(spawned.includes(row.worker_id), `SEED=${seed} worker ${row.worker_id}`)
      workerIds.add(row.worker_id)
    }
  }
  assert.equal(logByStore.size, expected.filter((store) => store.applied.length > 0).length)
  assert.ok(workerIds.size >= 1, `SEED=${seed}`)

  const early = await client.query(
    `SELECT a.message_id::text
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

  const billedSkip = await client.query<{ n: number }>(
    `SELECT count(*)::int AS n
     FROM apply_log a
     JOIN queue_messages m
       ON m.chain_id = a.chain_id
      AND m.store_id = a.store_id
      AND m.seq = a.seq
     WHERE m.outcome = 'skipped'`,
  )
  assert.equal(billedSkip.rows[0].n, 0, `SEED=${seed} billed a skipped seq`)

  const futureApplied = await client.query<{ n: number }>(
    `SELECT count(DISTINCT a.message_id)::int AS n
     FROM apply_log a
     JOIN queue_messages m ON m.message_id = a.message_id
     JOIN receipt_files r ON r.id = m.receipt_id
     WHERE EXTRACT(EPOCH FROM r.ready_at) > $1::numeric`,
    [started[1]],
  )
  assert.ok(futureApplied.rows[0].n > 0, `SEED=${seed} did not wait out a future ready_at`)
}

async function insertWalkFixture(client: pg.Client, mode: 'delayed' | 'failed'): Promise<WalkFixture> {
  const chainId = randomUUID()
  const storeId = randomUUID()
  const ids = {
    prefixSnap: randomUUID(),
    prefixDelta: randomUUID(),
    poison: randomUUID(),
    s1: randomUUID(),
    delta: randomUUID(),
    s2: randomUUID(),
  }
  const receipts = {
    prefixSnap: randomUUID(),
    prefixDelta: randomUUID(),
    poison: randomUUID(),
    s1: randomUUID(),
    delta: randomUUID(),
    s2: randomUUID(),
  }
  const s2Stock: Stock = { 'SKU-9034': 42, promo_code: 'v9.9' }
  const s1Ready = mode === 'delayed' ? `now() + interval '3 seconds'` : 'NULL'
  await client.query(
    `INSERT INTO receipt_files (id, ready_at) VALUES
       ($1, now() - interval '1 hour'),
       ($2, now() - interval '1 hour'),
       ($3, NULL),
       ($4, ${s1Ready}),
       ($5, now() - interval '1 hour'),
       ($6, now() - interval '1 hour')`,
    [receipts.prefixSnap, receipts.prefixDelta, receipts.poison, receipts.s1, receipts.delta, receipts.s2],
  )

  const messages: Array<[string, number, string, unknown, string]> = [
    [ids.prefixSnap, 1, 'snapshot', { stock: { 'SKU-4711': 1, 'SKU-1002': 2 } }, receipts.prefixSnap],
    [ids.prefixDelta, 2, 'delta', { set: { 'SKU-4711': 3 }, unset: [] }, receipts.prefixDelta],
    [ids.poison, 3, 'delta', { set: { 'SKU-1002': 9 }, unset: [] }, receipts.poison],
    [ids.s1, 4, 'snapshot', { stock: { 'SKU-2205': 7 } }, receipts.s1],
    [ids.delta, 5, 'delta', { set: { 'SKU-8813': 8 }, unset: [] }, receipts.delta],
    [ids.s2, 6, 'snapshot', { stock: s2Stock }, receipts.s2],
  ]
  for (const [messageId, seq, kind, payload, receiptId] of messages) {
    await client.query(
      `INSERT INTO queue_messages (message_id, chain_id, store_id, seq, kind, payload, receipt_id)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
      [messageId, chainId, storeId, seq, kind, JSON.stringify(payload), receiptId],
    )
  }

  return { chainId, storeId, s1Receipt: receipts.s1, s2Stock, ids }
}

async function watchDelayedHold(client: pg.Client, fixture: WalkFixture): Promise<boolean> {
  const deadline = Date.now() + 40_000
  let sawHold = false
  while (Date.now() < deadline) {
    const state = await client.query<{ delayed: boolean | null; last_seq: number | null; s2: boolean }>(
      `SELECT
         (SELECT ready_at > now() FROM receipt_files WHERE id = $1::uuid) AS delayed,
         (SELECT last_seq FROM store_stock WHERE chain_id = $2::uuid AND store_id = $3::uuid) AS last_seq,
         EXISTS (SELECT 1 FROM apply_log WHERE message_id = $4::uuid) AS s2`,
      [fixture.s1Receipt, fixture.chainId, fixture.storeId, fixture.ids.s2],
    )
    const row = state.rows[0]
    const lastSeq = row.last_seq === null ? null : Number(row.last_seq)
    if (row.delayed && lastSeq !== null && lastSeq > 2) {
      throw new Error(`seq advanced to ${lastSeq} while S1 was still delayed`)
    }
    if (row.delayed && row.s2) {
      throw new Error('S2 entered apply_log while S1 was still delayed')
    }
    if (row.delayed && lastSeq === 2 && !row.s2) sawHold = true
    if (!row.delayed) return sawHold
    await sleep(50)
  }
  return sawHold
}

async function assertWalkStore(client: pg.Client, fixture: WalkFixture, mode: 'delayed' | 'failed'): Promise<void> {
  const counters = await storeCounters(client, fixture.chainId, fixture.storeId)
  assert.deepStrictEqual(counters.stock, fixture.s2Stock)
  assert.equal(counters.last_seq, 6)

  const log = await client.query<{ message_id: string; seq: number }>(
    `SELECT message_id::text, seq
     FROM apply_log
     WHERE chain_id = $1::uuid AND store_id = $2::uuid
     ORDER BY apply_pos`,
    [fixture.chainId, fixture.storeId],
  )
  const outcomes = await client.query<{ seq: number; outcome: string | null; n: number }>(
    `SELECT seq, outcome, count(*)::int AS n
     FROM queue_messages
     WHERE chain_id = $1::uuid AND store_id = $2::uuid
     GROUP BY seq, outcome
     ORDER BY seq`,
    [fixture.chainId, fixture.storeId],
  )

  if (mode === 'delayed') {
    assert.equal(counters.version, 5)
    assert.ok(counters.last_seq > counters.version)
    assert.deepStrictEqual(
      log.rows.map((row) => row.message_id),
      [fixture.ids.prefixSnap, fixture.ids.prefixDelta, fixture.ids.s1, fixture.ids.delta, fixture.ids.s2],
    )
    assert.deepStrictEqual(
      log.rows.map((row) => row.seq),
      [1, 2, 4, 5, 6],
    )
    const early = await client.query<{ early: boolean }>(
      `SELECT s2.applied_at < r.ready_at AS early
       FROM apply_log s2
       JOIN receipt_files r ON r.id = $2::uuid
       WHERE s2.message_id = $1::uuid`,
      [fixture.ids.s2, fixture.s1Receipt],
    )
    assert.equal(early.rows[0].early, false)
    assert.deepStrictEqual(outcomes.rows, [
      { seq: 1, outcome: 'applied', n: 1 },
      { seq: 2, outcome: 'applied', n: 1 },
      { seq: 3, outcome: 'skipped', n: 1 },
      { seq: 4, outcome: 'applied', n: 1 },
      { seq: 5, outcome: 'applied', n: 1 },
      { seq: 6, outcome: 'applied', n: 1 },
    ])
    return
  }

  assert.equal(counters.version, 3)
  assert.ok(counters.last_seq > counters.version)
  assert.deepStrictEqual(
    log.rows.map((row) => row.message_id),
    [fixture.ids.prefixSnap, fixture.ids.prefixDelta, fixture.ids.s2],
  )
  const absent = await client.query<{ n: number }>(
    `SELECT count(*)::int AS n
     FROM apply_log
     WHERE message_id = ANY($1::uuid[])`,
    [[fixture.ids.s1, fixture.ids.delta]],
  )
  assert.equal(absent.rows[0].n, 0)
  const gap = await client.query<{ seconds: number }>(
    `SELECT EXTRACT(EPOCH FROM (s2.applied_at - prefix.applied_at))::float AS seconds
     FROM apply_log s2
     JOIN apply_log prefix ON prefix.message_id = $2::uuid
     WHERE s2.message_id = $1::uuid`,
    [fixture.ids.s2, fixture.ids.prefixDelta],
  )
  assert.ok(gap.rows[0].seconds < 5, `S2 waited ${gap.rows[0].seconds}s after the prefix`)
  assert.deepStrictEqual(outcomes.rows, [
    { seq: 1, outcome: 'applied', n: 1 },
    { seq: 2, outcome: 'applied', n: 1 },
    { seq: 3, outcome: 'skipped', n: 1 },
    { seq: 4, outcome: 'skipped', n: 1 },
    { seq: 5, outcome: 'skipped', n: 1 },
    { seq: 6, outcome: 'applied', n: 1 },
  ])
}
