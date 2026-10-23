# Design

`npm start` runs a supervisor. It forks four worker processes. Each worker holds one Postgres connection and applies one update at a time. The workers do not pass messages to each other. Postgres is the only shared channel.

One transaction updates `store_stock`, inserts one `apply_log` row, and marks the queue rows for that seq. On a jump past a failed receipt, the same transaction marks the gap skipped. Commit saves all of it. A crash rolls all of it back.

`version` counts applied updates. It equals the number of `apply_log` rows for that store. `last_seq` is the newest applied seq. After a jump, `last_seq` is greater than `version`. Duplicates move neither field.

## Risks

### Arrival order is not store order

A buyer needs the order the store lived through. The spec says gateways buffer and retransmit, so updates from many stores arrive interleaved, in whatever order the network delivered them. `schema.sql` says `queue_messages.id` is arrival order at the platform edge, and `seq` is the store-local sequence, contiguous from 1 for each `(chain_id, store_id)`. `seed.js` shuffles the rows before insert, so a higher `id` can be an earlier `seq`. The spec's replay example is per store, in seq order.

The next message for a store is `seq = last_seq + 1`. Workers do not scan the queue in `id` order. Applying in arrival order was rejected. A delta would land before the snapshot it depends on whenever the network delivered them backwards.

### The same update is delivered twice

The spec says a gateway retransmits an update until the platform acknowledges it. `seed.js` copies about 5% of messages and inserts them again with the same `message_id`, the same payload, and the same `receipt_id`. The queue allows that. There is no unique constraint on `queue_messages.message_id`.

One delivery identity is one applied update. Billing is per processed update, not per queue row. The transaction inserts one `apply_log` row, increments `version` once, marks the earliest queue row of that seq `applied`, and marks the other rows of the same `(chain_id, store_id, seq)` `duplicate`. The earliest row is the lowest `id`. On startup the app adds two unique constraints on `apply_log`: `message_id`, and `(chain_id, store_id, seq)`. Billing both copies was rejected. A leased `processing` row was rejected too, because a dead worker would leave it behind.

### A delayed receipt is not a failed receipt

The spec says the receipt is uploaded separately from the queue message. Uploads take time, and sometimes the gateway reports that the upload failed for good. An update may not be processed until its receipt is available. `schema.sql` says `ready_at` NULL means the gateway reported the upload failed. `seed.js` puts about 12% of receipts 3–25 seconds in the future and the rest an hour in the past, and it tells you to start the app now. `--poison` picks one message with `5 < seq < 30` and sets that receipt's `ready_at` to NULL. The seed prints that this one will never be ready.

Readiness uses the database clock: `ready_at <= now()`. A future timestamp is a wait. NULL is permanent. Treating NULL as a long delay was rejected, because the run would never finish. Treating a future timestamp as a hole was rejected, because the receipt is about to land.

### A snapshot replaces the shelf. A delta edits it.

The spec's worked example starts from a full list and then applies small changes. A snapshot payload is `{ stock }`. A delta payload is `{ set, unset }`. In `seed.js`, seq 1 is always a snapshot, and about a quarter of stores get one later snapshot at a random seq.

A snapshot write sets `stock` to that payload. Keys absent from the new list leave the shelf. A delta removes the `unset` keys and then merges `set`. A quantity of 0 stays, because the store reported a quantity. Only `unset` removes a key. Merging a snapshot over the old shelf was rejected. That would keep items the store no longer lists.

### The same item in `set` and `unset`

The spec's example removes and assigns in separate messages. It never defines one payload that names one item twice. `seed.js` draws the two fields independently. `set` holds 1–3 generated values. With probability 0.25, `unset` holds one random item key and no value. Those draws can name the same item. The quantity for that item is only in `set`.

The delta removes `unset` keys first, then merges `set`. The assigned quantity remains. The other order was rejected: `unset` second would erase the only quantity in the message, and the shelf would show neither the old value nor the new one. Refusing the delta was rejected too. That would treat a normal seed row as a permanent failure.

The SQL in `src/apply.ts` and the plain-object fold in `test/oracle.ts` both follow this rule. They do not share code.

### Worker death

The spec says workers get OOM-killed, redeployed, and restarted mid-run, and that this must not corrupt the stock or the audit trail. The failure that matters is a crash after the shelf changes and before the log row, or a lock that stays held after the process is gone.

The shelf, the counters, the `apply_log` row, and the queue marks are one transaction. The per-store lock is `pg_try_advisory_xact_lock` on `chain_id || ':' || store_id`. It ends on commit, rollback, or disconnect. There is no leased `processing` row. Process death drops the connection, and Postgres rolls the open transaction back. The stock update is guarded with the `last_seq` just read, so a stale write does not land. Two commits, one for the shelf and one for the log, were rejected. A lock the worker would have to heartbeat was rejected.

`worker_id` is the pid of the process that committed the row. The supervisor's pid never appears. A respawn can leave more than four pids in the log over one run. Steady state is four workers.

### One store blocking the others

The spec says stores are independent. One slow, broken, or misbehaving store must not hold up the rest, and a permanently broken store must not stop the run from finishing. The seed is 4 chains times 6 stores, 24 stores, and the fleet is 4 processes. One global lock, or a worker that sits on one store until that store is finished, would stall the other stores behind a single future receipt or a single null receipt.

The lock is per `(chain_id, store_id)`. `store_id` alone is not the identity. If the lock is already held, the worker rolls back and tries a different ready store. It does not wait on the lock. A store whose next receipt is only delayed is left off the ready list, so the other stores keep moving. The supervisor's `fork` carries no store and no message. A hash collision on the advisory lock only makes two stores take turns. Assigning stores to workers over IPC was rejected. The spec forbids any channel besides Postgres. One global lock was rejected.

### Exiting while receipts are still in the future

The spec says the process exits 0 when it has done everything that can be done. The seed says gateway activity continues for about 30 seconds. "Nothing is ready this instant" is a different claim from "nothing left can become applyable." Exiting at the first idle moment would drop updates whose receipts are only delayed.

A worker stays up while any store has a contiguous next receipt that is ready or delayed, or a null next receipt whose snapshot walk still has a candidate that is ready or delayed. Idle sleep is at most 500ms. When the only blocker is the clock, the sleep is the time until that `ready_at`, capped at 500ms. A hole with no candidate does not keep the process alive. The supervisor exits 0 when every child has exited 0. Exiting as soon as the ready list is empty was rejected.

### A re-seed leaves extra tables behind

The assignment allows added columns and tables, and it forbids editing `schema.sql` and `seed.js`. `seed.js` truncates only `queue_messages`, `apply_log`, `store_stock`, and `receipt_files`. An extra table would survive a re-seed. An extra column on `queue_messages` does not keep its values, because truncate clears the rows. Constraints added later do survive.

Progress lives in `store_stock` and `apply_log`, which the seed already clears. Restart reads those two tables. There is no progress file and no extra progress table. `queue_messages.outcome` is a breadcrumb, with values `applied`, `duplicate`, and `skipped`. The truth is the shelf plus the log. A side table of cursors was rejected. After `node seed.js` that table would still say a store was at seq 40 while the queue had been replaced.

Startup adds the outcome column and the two unique constraints in its own transaction, under a two-integer advisory lock. That lock is a different keyspace from the per-store locks, so a migration does not block an apply already in progress. The statements are idempotent. `schema.sql` is untouched.

## The null receipt

The rejected rule was: stop forever at the null receipt and keep `version` equal to `last_seq`. Apply `1 .. K-1`, then leave `K` and every later message unapplied, including a snapshot whose own receipt is ready.

The chosen rule is the snapshot walk. It runs only when the contiguous next seq has a null receipt. Later snapshots are considered in `seq` order.

- If the first later snapshot's receipt is only delayed, wait for that snapshot. A snapshot further ahead that is already ready is not a candidate.
- If that receipt is null, the snapshot can never be a baseline. Try the next snapshot.
- If that receipt is ready, apply it. Replace the shelf, set `last_seq` to that seq, mark every unapplied row in the open gap `skipped`, and write one `apply_log` row for the snapshot.
- If every later snapshot is missing or null, leave the prefix in place.

A future `ready_at` on the contiguous next seq is never skipped, even when a later snapshot is already ready. Only a null receipt on that next seq opens the walk. Deltas in the open gap are never applied. They get no `apply_log` row and they do not change `version`.

`version` counts applied updates. `last_seq` is the newest applied seq and may be greater. They stay equal only when the applied seqs are the prefix `1 .. last_seq`. The code does not force them back together. `schema.sql` defines the columns differently, and a resume is where they diverge. Applied seqs `1..6` and then a jump to snapshot 20 is `version = 7` and `last_seq = 20`. The next legal seq is 21.

Resuming is the rule that keeps the shelf a buyer would see. A snapshot payload is the complete shelf, and the spec says a gateway that restarts begins again with a fresh snapshot. Once that receipt is ready, the shelf is known without the missing delta. The permanent stop leaves a stale prefix, and it leaves a receipt-backed snapshot out of `apply_log`, so the log under-bills an update the platform was allowed to process. Both rules fail the poisoned update and the deltas in the gap: the poison has no receipt, and a delta needs the prior shelf, which is unknowable across the hole. After a resume, an auditor sees a gap whose first missing seq has `ready_at` NULL, then a snapshot, then a contiguous tail. If no later snapshot can reopen the store, the run still exits 0. The other stores are unaffected.

Inside one delta, `set` wins over `unset` because the seed puts the quantity only in `set`.

## Crash behavior

An ordinary SIGKILL in the tests hit an autocommit ready-store lookup, outside `BEGIN` / `COMMIT`. That lookup asks which stores are ready. It is not the apply transaction. The kill removed a process that was between commits. Rows already committed stayed committed. The supervisor stayed up, logged the signal, and forked a replacement. Another pid applied the rest. The oracle matched, with no duplicate `message_id`.

A separate test pauses inside the gap transaction. `STOCKSTREAM_PAUSE_ON_GAP_MS` is unset for normal runs. When it is set, the worker sleeps after the stock update, the skipped marks, and the `apply_log` insert, and before `COMMIT`, on a poison jump. The test saw that backend idle in transaction. SIGKILL dropped the connection. Postgres rolled the transaction back: `last_seq` was still the prefix, the gap rows were unmarked, and the snapshot was not in `apply_log`. The supervisor forked a replacement. A different pid then committed `last_seq` and the skipped marks together.

SIGKILL of the whole fleet, supervisor and workers, and then a new `npm start`, matches an uninterrupted run of the same seed. The second start reads `store_stock` and `apply_log` and continues. A third start applies nothing.

A signal death respawns. The child has no exit code, so the supervisor does not treat it as a failed run. It forks a replacement while any store can still move, and it can still exit 0. A non-zero exit code fails the supervisor. The supervisor remembers that code. It still replaces the child while work remains, and its own exit code is that non-zero code when the fleet stops. SIGTERM or SIGINT on the supervisor stops respawns and exits 1. SIGKILL on the supervisor does not run that handler.

## How this was checked

`test/oracle.ts` reads the queue and the receipts and replays each store in `seq` order. It does not import `src/`. The suite reads that file and fails if it does. The oracle dedupes by `message_id`, waits out future receipts the way the clock does, and on a null receipt takes the same snapshot walk. It compares parsed stock to `store_stock`. It checks `apply_log` against the same applied set: one row per applied `message_id`, `version` equal to that count, `last_seq` equal to the max applied seq, every logged delta's predecessor present, every logged row's receipt ready at `applied_at`, and no `apply_log` row for a skipped seq.

`npm test` runs `test/scenarios.test.ts`. Before this write-up, that was 9 passing.

- `a delta keeps the set quantity when the same key is unset`. Direct fold of `{ set: { "SKU-4711": 9 }, unset: ["SKU-4711"] }` onto a shelf that already held `SKU-4711` at 4. The result keeps 9. No database, so the check does not depend on a seed colliding.
- `one seeded run matches the oracle`. Seed 42, receipts forced ready, one worker. That seed has duplicate deliveries and a second snapshot. Stock and the log match the oracle. A second run of the same process logs `applied 0` and changes neither table.
- `four workers wait for future receipts`. `npm start` immediately after `node seed.js`, with the future `ready_at` values left where the seed put them. Seeds 42 and 1. Seed 1 is the next seed that has both a later snapshot and receipts still ahead. At least one applied row had a future `ready_at` at process start. `worker_id` values are spawned worker pids, never the supervisor. The log shows `fleet size=4`.
- `poison resumes at a later snapshot or leaves the prefix`. `node seed.js --poison` for seeds 2 and 42, with the shape read from the seeded rows. Seed 2 poisons seq 21. The next snapshot is seq 82, so the run resumes there and finishes at version 48, last_seq 109. Seed 42 poisons seq 23. That store's other snapshot is seq 22, which is before the hole, so there is no later snapshot to resume on. It stays at version 22, last_seq 22. The poisoned seq is absent from `apply_log`. Every other store matches a full replay. Exit code 0.
- `a delayed snapshot candidate blocks a later ready snapshot`. `seed.js` emits at most one later snapshot, so the test inserts its own store onto a seed 42 queue. Seq 1 and 2 are ready, seq 3 is null, snapshot S1 at seq 4 is a few seconds ahead, the delta at seq 5 is ready, and snapshot S2 at seq 6 is already ready. The test fails if S2 appears in `apply_log` while S1 is still delayed. After S1 is ready, the log is seq 1, 2, 4, 5, 6, in that order. `version` is 5 and `last_seq` is 6. Final stock is S2's snapshot. The audit trail is the check that matters: stock alone would also match a processor that jumped straight to S2.
- `a null snapshot candidate yields to the next ready snapshot`. The same inserted store, except S1's receipt is null and S2 is ready now. S2 is applied without waiting. S1 and the delta between them are absent from `apply_log`. `version` is 3 and `last_seq` is 6. Stock is S2's snapshot. Seq 3, 4, and 5 are `skipped`.
- `SIGKILL one worker and another finishes the run`. Seed 42, receipts matured, `npm start`. After the first log rows, the test kills one worker pid taken from `apply_log.worker_id`. That kill hit the autocommit ready-store lookup described above. A replacement pid finishes the run. The oracle matches.
- `SIGKILL the fleet and npm start again matches one uninterrupted run`. Seed 42, receipts matured. One full run is kept as the picture. A second run is SIGKILLed as a process group after at least one commit, the backends are gone, and `npm start` runs again. The finished message set and the stock match the uninterrupted run. One more `npm start` applies nothing.
- `SIGKILL during a poison-gap transaction rolls the jump back`. Seed 42 plus the null-candidate fixture, with `STOCKSTREAM_PAUSE_ON_GAP_MS=6000`. The test waits for `pause before commit` and for the backend to be idle in transaction, then SIGKILLs that pid. Before the kill, and again after the backend is gone, the store is still version 2, last_seq 2, seq 3 and later are unmarked, and S2 is not in `apply_log`. The replacement commits version 3, last_seq 6, with seq 3, 4, and 5 skipped and seq 6 applied, under a pid that is not the one that was killed.

## What is still rough

These crash cases were considered and not added.

- A kill of the supervisor alone. SIGKILL does not run its exit handler, so the four workers can keep running as orphans. The fleet test kills the workers as well.
- A pause on an ordinary apply. The pause hook runs only on the poison-jump path, after the gap writes and before commit.
- A kill between the skip update and the `apply_log` insert. The pause is after both of those statements. They share one transaction, so a kill between them should roll both back. That window was not observed.
- A crash loop that hits the spawn cap. The supervisor stops forking after 32 workers and then exits non-zero if work remains. The suite does not drive deaths until that cap.
- A restart while receipts are still in the future. The kill-and-restart test matures every non-null receipt before it starts. The four-worker test waits the delay out, and it does not kill during the wait.
- A poison prefix with no later snapshot, under SIGKILL. Seed 42 with `--poison` covers that prefix on a clean run. The gap-kill test uses a fixture that has a later snapshot.
- SIGKILL during migrate. The migration transaction is idempotent, so the next start retries it. The suite does not kill it there.
- Two workers killed at once. The tests kill one worker, or the whole fleet.

The GitHub repo for this work is private. A reviewer cannot see it until they are invited. The assignment also allows a zip of the repository that includes `.git`.
