# StockStream backlog processor

Four worker processes drain `queue_messages` into `store_stock` and write one `apply_log` row per applied update. `npm start` is the one command that brings up all four workers.

Node.js 20 or newer, npm, and Docker are required.

## Run

```bash
docker compose up -d --wait
npm install
node seed.js
npm start
```

`docker compose up -d --wait` starts Postgres and waits until it accepts connections. The database is published on port 5439.

`node seed.js` applies `schema.sql`, truncates the four seed tables, and fills the queue. Start the app right after it returns. Some receipts are a few seconds in the future, and the processor waits for them.

`npm start` runs `src/supervisor.ts`. That process forks four worker OS processes. Postgres is the only channel they share. The run exits 0 when nothing left can become applyable.

Connection string, also the default when `DATABASE_URL` is unset:

```text
postgres://postgres:postgres@localhost:5439/stockstream
```

## Seed and poison

`SEED` picks the random stream. The same value produces the same queue. The default is 42.

```bash
SEED=1 node seed.js
npm start
```

`--poison` sets one receipt's `ready_at` to NULL. That upload never becomes ready. The seed prints the store and the seq.

```bash
SEED=2 node seed.js --poison
npm start
```

## Tests

```bash
npm test
```

The tests start the same Postgres, seed it, and run the processor. They need Docker.
