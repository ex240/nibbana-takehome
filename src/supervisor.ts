import { fork, type ChildProcess } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import pg from 'pg'
import { migrate } from './migrate.js'
import { hasActionableReceipt } from './worker.js'

const { Client } = pg

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'

const FLEET_SIZE = 4
const MAX_SPAWNS = 32

const workerPath = fileURLToPath(new URL('./worker.ts', import.meta.url))

type Exit = {
  pid: number
  code: number | null
  signal: NodeJS.Signals | null
}

function workerExecArgv(): string[] {
  if (process.execArgv.some((arg) => arg.includes('tsx'))) return process.execArgv
  return ['--import', 'tsx', ...process.execArgv]
}

export async function supervise(client: pg.Client): Promise<number> {
  const children = new Set<ChildProcess>()
  const exits: Exit[] = []
  let waiters: Array<() => void> = []
  let spawns = 0
  let live = 0
  let failure: number | null = null
  let stopping = false

  function notify(): void {
    const pending = waiters
    waiters = []
    for (const wake of pending) wake()
  }

  function nextExit(): Promise<void> {
    if (exits.length > 0) return Promise.resolve()
    return new Promise((resolve) => {
      waiters.push(resolve)
    })
  }

  function killChildren(): void {
    for (const child of children) {
      if (child.exitCode === null && !child.killed) child.kill('SIGTERM')
    }
  }

  function onSignal(signal: NodeJS.Signals): void {
    if (stopping) return
    stopping = true
    console.error(`supervisor pid=${process.pid} received ${signal}`)
    killChildren()
    process.exit(1)
  }

  process.once('SIGTERM', onSignal)
  process.once('SIGINT', onSignal)
  process.once('exit', killChildren)

  function spawnOne(): void {
    if (stopping) return
    if (spawns >= MAX_SPAWNS) {
      console.error(`not respawning after ${spawns} workers; work still remains`)
      failure = failure ?? 1
      return
    }

    const child = fork(workerPath, [], {
      execArgv: workerExecArgv(),
      stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
    })
    if (child.pid === undefined) {
      throw new Error('fork failed to start a worker')
    }

    spawns += 1
    live += 1
    children.add(child)
    // The fork carries no store and no message. Postgres assigns the work.
    fs.writeSync(1, `spawned worker pid=${child.pid}\n`)

    let settled = false
    const finish = (code: number | null, signal: NodeJS.Signals | null): void => {
      if (settled) return
      settled = true
      children.delete(child)
      exits.push({ pid: child.pid as number, code, signal })
      notify()
    }
    child.once('exit', (code, signal) => finish(code, signal))
    child.once('error', (err) => {
      console.error(`worker pid=${child.pid} error`, err)
      finish(1, null)
    })
  }

  fs.writeSync(1, `supervisor pid=${process.pid}\n`)
  console.log(`fleet size=${FLEET_SIZE}`)
  for (let i = 0; i < FLEET_SIZE; i += 1) spawnOne()

  while (live > 0) {
    await nextExit()
    const done = exits.shift()
    if (!done) continue
    live -= 1
    const codeText = done.code === null ? 'null' : String(done.code)
    console.log(`worker pid=${done.pid} exited code=${codeText} signal=${done.signal ?? ''}`)
    // SIGKILL drops the connection and rolls the open transaction back. The
    // replacement finishes the run, so a signal is not a failed exit code.
    if (done.code !== null && done.code !== 0) failure = failure ?? done.code

    const remains = await hasActionableReceipt(client)
    if (remains && !stopping) spawnOne()
  }

  return failure ?? 0
}

export async function main(): Promise<number> {
  const client = new Client({
    connectionString: DATABASE_URL,
    application_name: `stockstream-supervisor-${process.pid}`,
  })
  await client.connect()
  try {
    const started = await client.query<{ epoch: string }>(
      `SELECT EXTRACT(EPOCH FROM now())::text AS epoch`,
    )
    console.log(`supervisor started_at=${started.rows[0].epoch}`)
    await migrate(client)
    return await supervise(client)
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
    .then((code) => {
      process.exit(code)
    })
    .catch((err: unknown) => {
      console.error(err)
      process.exit(1)
    })
}
