# AI usage

I used Cursor. A model drafted the processor, the tests, and these three documents. I did not hand-write the implementation.

I set the scope: four OS processes, one Postgres transaction per applied update, progress only in the tables `seed.js` truncates, and an oracle in `test/oracle.ts` that does not import `src/`. I required the work to land as separate commits, each one runnable, so the history shows the steps.

The model first recommended a permanent stop at the failed receipt: leave every later message unapplied, and keep `version` equal to `last_seq`. That is the wrong rule. It under-bills a later snapshot that has a valid receipt, and it leaves the shelf on a stale prefix when a complete snapshot is sitting in the queue. I rejected it. The rule in the code is the snapshot walk in `DESIGN.md`: wait if the first later snapshot is only delayed, and if that receipt is null, try the next snapshot.

I also asked for the kill timing to be distinguished. An ordinary SIGKILL in the suite landed between transactions, on the autocommit ready-store lookup. That is a different fact from a death inside an open transaction. `STOCKSTREAM_PAUSE_ON_GAP_MS` exists so the gap test can hold the jump open, show the rollback, and show a different pid committing `last_seq` and the skipped marks together. It is unset for a normal run.

I plan to practice the live extensions myself before the interview. The follow-up is a design conversation, and I need to be able to change this code without the model in the room.
