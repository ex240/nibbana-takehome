# StockStream — Backlog Sync Processor

## The product

A company runs many shops. Head office wants one central database that
always knows **what is currently on the shelves of every shop** — so it
never has to phone a shop to ask.

Each shop has a small program that reports its shelves to head office. It
reports in two ways:

- **A full list ("snapshot").** "Here is *everything* on my shelves right
  now." A shop sends this when it first comes online (and again if it
  restarts).
- **Small changes ("deltas").** After the full list, the shop only reports
  what changed: "milk is now 12", "we stopped stocking bread". Each change
  is small, so they're cheap to send all day long.

To know a shop's current shelves, you start from its last full list and
replay its changes in order:

```
snapshot  #1: { milk: 10, eggs: 6, bread: 3 }
delta     #2: set eggs = 4            →  { milk: 10, eggs: 4, bread: 3 }
delta     #3: remove bread            →  { milk: 10, eggs: 4 }
delta     #4: set milk = 12           →  { milk: 12, eggs: 4 }
                                          ^ the shop's shelves right now
```

All these reports from all the shops pile up in one queue. A program has to
work through that queue and update the central database — draining whatever
has accumulated. **That program is what you are building.**

```
  shop 1 ─┐
  shop 2 ─┤   each shop sends a full list, then a stream of small changes
  shop 3 ─┤
   ...   ─┘   (many shops — not just the four drawn here)
     │
     ▼
  ┌──────────────────────┐
  │   the queue          │   all reports from all shops, piled up
  │  (queue_messages)    │   in the order the network happened to deliver them
  └──────────────────────┘
     │
     ▼
  ┌──────────────────────┐
  │   YOUR processor     │   4 worker processes drain the queue and
  │   (what you build)   │   replay each shop's reports to rebuild its shelves
  └──────────────────────┘
     │
     ▼
  ┌──────────────────────┐
  │   central database   │   the current shelves for every shop,
  │   (store_stock)      │   plus a log of every report you applied
  └──────────────────────┘
```

The rest of this document uses the platform's own vocabulary: a shop is a
**store**, the reporting program is a **POS gateway**, the full list is a
**snapshot**, a change is a **delta**, and the central shelf table is
**`store_stock`**. The company that owns a group of stores is a **chain**.

## How updates reach the platform (facts about the world)

These are the realities of how stores and their gateways behave today. What they imply
for your design is for you to work out — **this spec deliberately does not
enumerate edge cases**; identifying them is part of the assignment.

- Stores are often in areas with poor connectivity. Gateways **retransmit
  an update until the platform acknowledges it**, and they buffer while
  offline, so updates from many stores arrive interleaved at the platform
  edge in whatever order the network delivers them.
- Every update the store generates gets a store-local sequence number
  (`seq`) — the order the store actually experienced events.
- Each update is backed by a **receipt file** (proof of the change) which
  the gateway uploads to object storage **separately**
  from the queue message. Uploads take time, and occasionally a gateway
  reports that an upload **failed for good**. Compliance policy: an update
  may not be considered processed until its receipt file is available.
- Power cycles are routine in stores. A gateway that restarts
  **re-registers and begins with a fresh snapshot** of the store's complete
  stock, then continues with deltas.

## What the business needs

1. **Accurate stock.** After the backlog is drained, `store_stock` must
   reflect exactly what each store's update stream says the store has —
   a chain buyer looking at the table must see the store's reality.
2. **A trustworthy audit trail.** Every applied update is recorded in
   `apply_log` (see the data reference). Billing charges chains **per
   processed update**, and compliance auditors reconcile the log against
   stores' books — the log must be exactly right, in every circumstance.
3. **Resilient operations.** Worker processes get OOM-killed, redeployed,
   and restarted mid-run. That is normal life, not an incident: no
   circumstance of process death or restart may corrupt the stock or the
   audit trail.
4. **Throughput.** Stores are independent businesses. One slow, broken, or
   misbehaving store must never hold up the rest of the chain's sync — and
   a permanently broken store must not prevent the run from finishing.
5. **Completion.** When the processor has done everything that can be
   done, it exits with code 0.

## Operational constraints

- The fleet is exactly **4 worker OS processes** (e.g. `child_process.fork`,
  `cluster`, or 4 separately launched processes; a supervisor is fine). Async
  concurrency inside a worker is welcome but is not a substitute.
  **Postgres is the only channel workers may share** — no shared memory, no
  IPC-based coordination, no local files.
- **TypeScript is preferred**, but you may use any language —
  just include a README explaining how to run it. Talk to Postgres with a
  plain driver (`pg`, `psycopg`, `pgx`, …) plus minor utilities.
  **No queue/job frameworks** (pg-boss, graphile-worker, BullMQ, Celery, …)
  and **no ORMs** — building the coordination is the assignment.
- You may **add** columns, tables, indexes, and constraints to help you.
  You may **not** modify or remove anything `schema.sql` defines, and you
  may not edit `seed.js` or `schema.sql`.
- Polling is fine (keep it ≤ ~500 ms when idle); `LISTEN/NOTIFY` is a nice
  touch, not required.
- The seed simulates the tail of a live business day: some receipt uploads
  land in the ~30 seconds *after* you seed, so their `ready_at` is set a
  little into the future. **Start your app immediately after seeding** so it
  experiences that window, the way the real processor would.

## Getting started

```bash
docker compose up -d
npm install
node seed.js     # applies schema.sql, resets, fills the queue (~2,000 rows)
# ... run YOUR app ...
```

Connection string (also the scripts' default):
`postgres://postgres:postgres@localhost:5439/stockstream`

## Data reference

**`queue_messages`** — the update queue. `id` is arrival order at the
platform edge. `message_id` is the update's identity, assigned by the
gateway. `seq` is the store-local sequence number, contiguous from 1 per
`(chain_id, store_id)`. `kind` is `snapshot` or `delta`; `payload`:

```jsonc
// snapshot — the store's COMPLETE stock at that moment
{ "stock": { "SKU-4711": 12, "SKU-2205": 340.5, "promo_code": "v2.4" } }

// delta — a partial update; "unset" delists an item
{ "set": { "SKU-4711": 9 }, "unset": ["promo_code"] }
```

**`receipt_files`** — one row per receipt. `ready_at` is when the upload
completed; `NULL` means the gateway reported the upload failed.

**`store_stock`** — the central stock: one row per store. `stock` (JSONB),
`version` (count of updates applied to this store), `last_seq` (the
store-local seq of the newest applied update).

**`apply_log`** — the audit trail: one row per applied update —
`(message_id, chain_id, store_id, seq, kind, worker_id)`, where
`worker_id` identifies the OS process that applied it (its PID is fine).
Auditors treat this table and `store_stock` as one consistent record.

## Deliverables

1. A git repository (zip it with `.git/` included, or share a private
   link). **Commit as you go** — we read the history, and a single
   "initial commit" with everything in it is a red flag.
2. `README.md` — how to run it (one command should bring up all 4 workers).
3. `DESIGN.md` — the heart of the submission:
   - **The risks you identified.** What can go wrong in this system — in
     the data, the timing, the infrastructure — and where you found each
     (reading the spec, inspecting the seeded data, reasoning about
     failures).
   - **How you handle each one**, and what you considered and rejected.
   - **How you convinced yourself it works.** You don't get our test
     suite, so show us yours: checks, queries, chaos experiments —
     whatever you used to trust your own code.
4. `AI-USAGE.md` — we assume you'll use AI tools and that's fine. Tell us
   honestly where and how: what you generated, what you wrote or reworked
   by hand, and at least one place the AI got it wrong and you caught it.

## How we evaluate

We seed, run your app, and reconcile the resulting `store_stock` and
`apply_log` against the update stream with our own automated checks — under
normal conditions and under deliberately hostile ones (including killing a
worker process mid-run and restarting your app, and re-seeding with a
different `SEED`). Then we read your code, your history, and your
DESIGN.md, and hold a ~60-minute follow-up conversation where we extend the
design together.

Expect to spend roughly **5–6 focused hours**. The risks you find and
defend against matter more to us than polish — we'd rather see the hard
guarantees bulletproof and one thing left rough (and honestly listed in
DESIGN.md) than everything half-finished.
