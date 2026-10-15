# Sync gaps: pelliper-daemon ← Flectar Mail

Where `pelliper-daemon`'s IMAP sync is weaker than `flectar-mail`'s, and what
to port. Derived from the comparison of this repo against
`flectar-mail-core/src/sync/` + `src/imap/` in the Flectar Mail project.

Scope note: Flectar is a full native client (Gmail REST, JMAP, calendar,
contacts, threads, AI) while this is an IMAP-only backend daemon. The items
below are only the ones that affect **correctness or durability of mail sync**,
not raw feature parity. Each is independently shippable.

## Priority summary

| # | Gap | Impact | Effort | Risk |
|---|-----|--------|--------|------|
| 1 | ~~No UIDVALIDITY tracking~~ ✅ done | Data corruption/mis-association on server UID reset | S | M |
| 2 | ~~No persisted sync cursor~~ ✅ done | Repeated full `UID SEARCH ALL` + newest-page refetch | M | M |
| 3 | ~~No expunge/flag reconciliation~~ ✅ done | Stale rows; server-side changes never applied locally | M | M |
| 4 | ~~No durable action ledger (non-send)~~ ✅ done | Offline/failed flags, moves, deletes silently diverge | L | M |
| 5 | ~~Cache mutated even when IMAP op fails~~ ✅ done | Local state claims success that server rejected | S | L |
| 6 | ~~No IDLE capability probe~~ ✅ done | Connects IDLE to servers that don't advertise it | S | L |
| 7 | ~~Trigger-driven FTS fragility~~ ✅ done | A failed trigger silently drops body indexing | M | M |
| 8 | No migration runner | Schema changes are silent no-ops on existing DBs | S | L |
| 9 | Threading owned implicitly by UI | No threads table; contract undocumented | L | L |
| 10 | `sync_failures` ledger absent | Failed header/body fetches are lost for the session | S | L |

Effort: S = hours, M = a day or two, L = multi-day. Risk = chance of
regressing sync correctness if done carelessly.

---

## 1. Track UIDVALIDITY per folder

**Status: implemented.** `folders.uidvalidity` is now persisted (with an
additive `ALTER TABLE` migration in `create_tables` for existing databases).
`imap::folder_uidvalidity` reads it via EXAMINE; `Worker::sync_folder_first_page`
clears the folder's cached messages and queued background work when the value
changes, then re-seeds; `BackfillJob` carries the UIDVALIDITY it was planned
under and `backfill_uidvalidity_ok` drops stale jobs. Covered by four new cases
in `src/cache.test.rs`.

**Original analysis.** Every fetch calls `session.examine(folder)` and never reads the
returned UIDVALIDITY (`src/imap.rs`, `fetch_recent_summaries`,
`fetch_summaries_by_uids`, `fetch_body`, `fetch_all_uids`). The cache keys
messages by `(account_id, folder_path, uid)` (`src/cache.rs` `messages` table).
If a server ever changes UIDVALIDITY, old UIDs can be reused for different
messages and cached rows/previews/bodies become mis-associated.

**Flectar reference.** `sync/engine.rs` in `sync_folder` compares the selected
mailbox's UIDVALIDITY with the stored value and calls `reset_uid_mappings`,
clearing the inbox baseline; body work is guarded so in-flight fetches for the
old UIDVALIDITY cannot persist (see `can_persist_selective_content` and the
body-worker guards). The persisted state lives on the folder row.

**Proposed change.**
- Add `folders.uidvalidity INTEGER` (default 0).
- In `imap.rs`, return the UIDVALIDITY from `examine` alongside summaries (or
  add a small `select_folder` helper that returns `(uidvalidity, exists)`).
- In `Worker::sync_folder_first_page`, if the selected UIDVALIDITY differs from
  the cached one, delete all `messages`/`message_headers`/`bodies` rows for that
  `(account_id, folder_path)` (cascade handles headers/bodies), set the new
  UIDVALIDITY, and treat the folder as cold.
- Because backfill chunks are queued in memory, also tag each `BackfillJob` with
  the UIDVALIDITY it was planned under and drop it if the folder's changed.

**Files.** `src/cache.rs` (`create_tables`, `upsert_folder`, new
`set_folder_uidvalidity` / `clear_folder_messages`), `src/imap.rs`,
`src/worker.rs` (`sync_folder_first_page`), `src/worker/backfill.rs`.

**Test.** Extend `src/cache.test.rs` with a case that changes UIDVALIDITY and
asserts the folder's rows are cleared and the FTS index no longer returns the
old messages.

---

## 2. Persist a per-folder sync cursor

**Status: implemented.** `folders.last_seen_uid` (forward high-water) and
`folders.backfill_cursor` (history walk position) are persisted with additive
migrations in `create_tables`. Cold folders seed from a sequence-range page and
snapshot the server high-water; warm folders fetch only `imap::uid_search_after`
(`UID SEARCH UID (last+1):*`, client-side filtered) in `BACKFILL_CHUNK`-wide
windows. History backfill walks the UID space downward from a persisted cursor
(`imap::mailbox_highest_uid`, trusted only when `UIDNEXT − 1 ≥ EXISTS`, else a
search fallback), so `UID SEARCH ALL` never runs after a folder is seeded.
`clear_folder_messages` resets both cursors on a UIDVALIDITY change. Covered by
new cases in `src/imap.test.rs` and `src/cache.test.rs`.

**Original analysis.** Each sync refetches the newest `FIRST_PAGE_SIZE`/`REOPEN_PAGE_SIZE`
messages by sequence range (`sync_folder_first_page` → `fetch_recent_summaries`),
and backfill issues `UID SEARCH ALL` then diffs against every cached UID
(`worker/backfill.rs` `queue_backfill_for_folder`). Nothing is persisted between
cycles: `worker.rs` uses the in-memory `body_prefetched`/`backfill_queue` sets.

**Flectar reference.** `folders.last_seen_uid` / `backfill_cursor` /
`backfill_done` are durable. Incremental sync issues `UID SEARCH UID <last+1>:*`
(`imap/mod.rs::uid_search_after`), guarded by a UIDNEXT high-water mark that is
only trusted when consistent with EXISTS. History backfill runs on a separate
connection, waking every 60 s, one resumable step per folder, walking downward.

**Proposed change.**
- Add `folders.last_seen_uid INTEGER DEFAULT 0` and
  `folders.backfill_cursor INTEGER` (NULL = not started).
- On a cold folder, snapshot the mailbox high-water UID (`UIDNEXT − 1`) as
  `last_seen_uid`, set the backfill cursor below it, and let the existing
  newest-page fetch seed the UI.
- On later cycles, if the mailbox high-water is above `last_seen_uid`, fetch only
  `UID SEARCH UID (last_seen_uid+1):*` and advance. Keep the sequence-range page
  as the cold-cache fallback.
- Persist the backfill cursor after each `BACKFILL_CHUNK` so an interrupted
  backfill resumes instead of re-running `UID SEARCH ALL`.
- Only trust UIDNEXT when it is consistent with `EXISTS` (Flectar's rule),
  otherwise fall back to search.

**Files.** `src/cache.rs`, `src/imap.rs` (new `uid_search_after`), `src/worker.rs`,
`src/worker/backfill.rs`.

**Test.** `src/imap.test.rs` can unit-test the cursor/UIDNEXT consistency rule
without a server, mirroring Flectar's
`uidnext_is_used_only_when_it_is_a_consistent_high_watermark`.

---

## 3. Reconcile expunges and flags

**Current.** Backfill only **adds** missing UIDs; a message deleted from another
client stays in the local cache forever. Newest-page sync upserts flags only for
the newest page, so flag changes on older mail are never picked up. Only
user-initiated deletes call `cache.delete_message`.

**Flectar reference.** `run_cycle` reconciles flags (a bounded `FLAG_WINDOW`) and
expunges each cycle; `reconcile_flags` skips rows with a pending intent, and
`reconcile_expunges` skips in-flight moves.

**Proposed change.**
- After a full UID diff (item 2), delete cached UIDs for the folder that are no
  longer present on the server. With a persisted cursor this becomes:
  reconcile only the affected window rather than diffing all UIDs.
- Refresh flags for the selected window (`UID FETCH <range> FLAGS`) and update
  `messages.is_read`/`is_starred` in bulk.
- Once item 4 (pending actions) exists, skip rows with an in-flight local intent.

**Files.** `src/worker.rs`, `src/worker/backfill.rs`, `src/cache.rs`
(new bulk `cached_uids_in_range`/`delete_messages`/`update_message_flags`).

**Test.** Cache-level test: seed rows, delete some server-side, assert only the
survivors remain and FTS no longer matches the removed ones.

---

## 4. Durable action ledger for non-send mutations

**Status: implemented.** Flags, moves and deletes are now written to a
`pending_actions` table *before* the server is touched, so an offline or
rejected mutation survives a restart and is retried instead of being lost.

- **Storage** (`src/cache.rs`): `pending_actions(id, account_id, folder_path,
  uid, kind, payload, state, attempts, not_before, created_at, last_error)` with
  an `(account_id, state, not_before)` index and an `ON DELETE CASCADE` FK to
  `accounts`. `enqueue_action` / `claim_due_action` / `complete_action` /
  `reschedule_action` / `reset_in_flight_actions` are the repo fns.
- **Policy** (`src/actions.rs`): pure, unit-tested backoff
  (`backoff_secs` / `backoff_secs_jittered`, base 5 s doubling to a 30 min cap),
  permanent-vs-transient classification (`is_permanent_error`), and the
  retry/drop decision (`retry_decision`, `MAX_ACTION_ATTEMPTS = 8`).
- **Execution** (`src/worker/actions.rs`): `process_action` runs the IMAP op
  (keeping the one-shot OAuth refresh + retry), applies the local cache effect
  only on success, then drops the row; on failure it reschedules with backoff or
  drops the row when the error is permanent / attempts are exhausted.
  `drain_one_action` claims one due action per loop iteration.
- **Enqueue-first** (`src/worker/handlers.rs`): `handle_set_flags`,
  `handle_move_message` and `handle_delete_message` record the intent, then call
  `process_action` for immediate feedback. The move/delete handlers no longer
  bail out when `ensure_session` fails — role resolution reads the cache, so an
  offline account still queues its intent. Delete-to-trash is stored as a
  `move` action with `refresh_dest = false`; delete-in-trash as a permanent
  `delete`.
- **Loop placement** (`src/worker.rs`): step 0.5 drains one due action — after
  user requests, before body prefetch/backfill — and worker startup calls
  `reset_in_flight_actions` to recover actions stranded by a crash.

Sends keep the dedicated `outbox` + SMTP flusher; only non-send mutations move
to the ledger.

**Not done (deliberately).** The local cache is updated only *after* the server
accepts the action, so there is no optimistic mutation and therefore no
rollback. This is consistent with gap #5 and avoids the UI briefly showing a
change the server later rejects; `mark_failed_and_rollback` from Flectar is
unnecessary under this model. Bounded slices (Flectar's
`MAX_ACTIONS_PER_SLICE`) are not needed either — one action per loop iteration
already keeps the loop responsive.

**Tests.** `src/actions.test.rs` covers backoff growth/saturation, jitter
bounds, permanent-vs-transient classification, and the bounded retry/drop
decision. `src/cache.test.rs` covers: a claimed action is not returned again and
is gone after completion; `not_before` gates claims; rescheduling bumps the
attempt count and returns it to the queue; `reset_in_flight_actions` recovers
stranded rows; and an unknown `kind` is rejected rather than guessed at.

**Original analysis.** `Send` failures are durably queued (`outbox` table,
retried every 30 s, capped at 10 attempts — `src/outbox.rs`). Flags, move,
delete and folder ops were **not**: `src/worker/handlers.rs` called
`do_set_flags` / `do_move` once, retried only on an auth error, and then moved
on. Offline or rejected mutations were lost.

**Flectar reference.** `queue/mod.rs` + `db/repo/actions.rs` implement
`pending_actions`: optimistic local mutation first, durable intent row, atomic
`try_claim`, bounded slices (`MAX_ACTIONS_PER_SLICE = 20`, 2 s), exponential
backoff + jitter, `MAX_ATTEMPTS = 8`, transient vs permanent classification
(`is_permanent`), and `mark_failed_and_rollback` which undoes an optimistic move
only if it is still the newest intent for that message.

**Files.** `src/cache.rs` (table + repo fns), `src/actions.rs` (+ `.test.rs`,
new), `src/worker/actions.rs` (new), `src/worker/handlers.rs`, `src/worker.rs`,
`src/types.rs`, `src/main.rs`.

**Note.** Folder create/rename/delete are still *not* in the ledger — they are
rarer, usually interactive, and their failure is surfaced directly. Extending
`ActionKind` with folder ops is the natural follow-up if that changes.


---

## 5. Don't mutate the cache when the IMAP op failed

**Status: implemented.** `handle_set_flags`, `handle_move_message`, and
`handle_delete_message` now gate every local cache write on the IMAP result:
the STORE/MOVE/EXPUNGE must return `Ok`, otherwise a warning is logged and the
handler returns without touching the cache. Each mutation keeps its existing
one-shot OAuth token refresh + retry, but a failed retry no longer falls
through to the cache write. The expunge branch was extracted into a
`do_expunge` helper (mirroring `do_set_flags`/`do_move`) so the
STORE+EXPUNGE sequence is a single `Result` to gate on; previously `select`
and `store_flags` errors were discarded with `let _ =`. No new unit test —
the gating is control flow over a live IMAP session, so it is only reachable
with a mock server; the pure `is_auth_error` classifier remains the testable
seam.

**Original analysis.** `handle_set_flags` (`src/worker/handlers.rs`) calls
`do_set_flags`, then unconditionally runs `cache.update_message_flags` and
`update_folder_unread`. `handle_move_message` and `handle_delete_message` do the
same with `cache.move_message` / `cache.delete_message`. On a non-auth server
error the local cache records a change the server rejected.

**Flectar reference.** Local mutation is tracked as intent, and
`mark_failed_and_rollback` reverses an optimistic move when the action
permanently fails.

**Remaining work.** None — the durable retry that this gating deferred to is
item 4, now implemented (`pending_actions`). A failed mutation is enqueued and
retried with backoff instead of being dropped; the cache is still written only
after the server accepts the action.

**Files.** `src/worker/handlers.rs`.

---

## 6. Probe IDLE capability once per connection

**Status: implemented.** On connect, `Worker` now probes CAPABILITY via
`imap::supports_idle` and caches `supports_idle`; the priority loop only takes
the IDLE branch when true (otherwise it falls back to the existing 120s
periodic resync). `imap::supports_idle` is a thin wrapper over
`session.capabilities()` that returns true if `IDLE` is advertised. The
pre-existing `idle_wait` logic (including the 29m RFC793 IDLE timeout and
interleaving request checks) remains unchanged.

---

## 7. Contentless, manually maintained FTS

**Status: implemented.** `messages_fts` is now contentless (`content=''`,
`contentless_delete=1`) with **no triggers**, and is maintained explicitly by
`Cache::index_message` / `Cache::deindex_message`.

- **Schema** (`src/cache.rs::fts_init`): the table is created contentless with
  the `contentless_delete=1` option (SQLite 3.43+; the bundled build is 3.45),
  which is what makes `DELETE`/`UPDATE` legal on a contentless table. Rows are
  keyed on `messages.rowid`, so a search hit joins straight back to the message.
- **Indexing**: `index_message` deletes then re-inserts (contentless tables
  cannot update in place), pulling `subject`/`sender` from `message_headers`,
  `preview` from `messages`, and `body` from `bodies` via a `LEFT JOIN` so a
  message with no body yet is still searchable by its headers. It runs from
  `save_message_headers` and `save_body`.
- **Deindexing**: `delete_message`, `delete_messages`, `clear_folder_messages`,
  `move_message` and `remove_account` all remove the index row(s) before the
  `messages` row goes (the rowid is resolved through `messages`, so order
  matters). `move_message` re-keys the row from the source rowid to the
  destination's.
- **Upgrade path**: `fts_init` drops the seven legacy triggers unconditionally
  and, when it finds the old contentful shape (`PRAGMA`-free probe via
  `sqlite_master.sql`), drops the table and reindexes from `messages`. It only
  backfills when the index is empty, so a correct database pays nothing on
  startup.

**Behaviour note for search consumers.** The daemon itself never queries
`messages_fts` (there is no search D-Bus method), so a client that reads the DB
must join on `messages_fts.rowid = messages.rowid`; the old `UNINDEXED`
`account_id`/`folder_path`/`uid` columns are gone because a contentless table
does not store column values.

**Original analysis.** `messages_fts` was kept in sync by triggers that rebuilt
the whole row on every header/body insert (`src/cache.rs::fts_init`). The
comments record that a partial-column insert used to silently roll back every
cached body; the design remained fragile to future trigger edits.

**Flectar reference.** `messages_fts` is contentless (`content=''`,
`contentless_delete=1`) with **no triggers**; `repo::search::index_message`
explicitly deletes + reinserts, and `repo::messages::delete` removes the index
row. Callers index at known points (store_headers, body save, move, delete).

**Tests.** The pre-existing FTS cases all still pass and now exercise the
explicit path: headers/body indexed on write, body re-save reindexes instead of
duplicating, flag flips leave the index untouched, delete removes the entry, and
a move reindexes at the destination. Two were added:
`moving_a_message_rekeys_its_index_row_to_the_destination` (the surviving hit
joins back to the destination row, and the source hit does not linger) and
`fts_init_rebuilds_a_legacy_triggered_index` (drops triggers, converts to
contentless, reindexes existing mail).

**Files.** `src/cache.rs` (`fts_init` + `index_message`/`deindex_*` + SQL
consts), `src/cache.test.rs`.


---

## 8. Add a real migration runner

**Current.** `Cache::open` sets `PRAGMA user_version=1` but nothing reads it;
`create_tables` is all `CREATE TABLE IF NOT EXISTS` (`src/cache.rs`). Adding a
column to a released schema will silently not apply.

**Flectar reference.** `db/migrations.rs` holds an ordered
`include_str!` migration array, `LATEST_VERSION = len`, applies each pending
migration in its own transaction, and rejects a profile written by a newer app.

**Proposed change.** Introduce a small `migrations` module: `MIGRATIONS: &[&str]`
with `001_init.sql`, `002_*.sql`, …; `run(conn)` applies `user_version + 1..` in
transactions and updates `user_version`; reject `user_version > LATEST`. Fold the
current `create_tables` + `fts_init` into `001_init.sql`. This unblocks items 1,
2, 4, and 10, whose schema additions would otherwise be no-ops on existing DBs.

**Files.** `src/cache.rs`, new `src/db/migrations.rs` + `src/db/migrations/*.sql`
(or keep SQL inline if preferred).

**Test.** A test that opens a v0 DB and migrates to latest, and that a newer
`user_version` is rejected without mutation.

---

## 9. Decide and document threading ownership

**Current.** `message_headers` stores `in_reply_to`/`references_` and
a `message_refs` helper, but there is no thread table and no thread
resolution in the daemon. Comments imply a separate UI repo does the JWZ
union-find. The contract is undocumented in-repo.

**Flectar reference.** `sync/threading.rs::resolve_thread` does JWZ-lite
(references match, then a 14-day subject fallback), with a `threads` table whose
denormalized counts are recomputed on every change.

**Options.**
- **Keep UI-owned (cheapest):** add an explicit note to `AGENTS.md` and a
  doc-comment on `message_headers`, and make sure the D-Bus `get_messages`
  payload carries the fields the UI needs (`in_reply_to`, `references_`,
  `message_id`) — it already does.
- **Daemon-owned:** add `threads` + `message_refs` tables and call
  `resolve_thread` after `save_message_headers`, recomputing aggregates on
  flag/move/delete. This is the larger option and only worth it if a second
  client will consume the cache.

**Files.** `AGENTS.md` (contract) or `src/cache.rs` + a new `src/threading.rs`.

---

## 10. Retry ledger for failed header/content fetches

**Current.** A failed `fetch_summaries_by_uids` / `fetch_body` is logged and
dropped; the UID will be retried only if a later backfill diff or page fetch
happens to include it. There is no record of which messages still lack headers
or bodies.

**Flectar reference.** A durable `sync_failures` table stores `stage`
(header/content) + `retry_at` (60 s) and a background pass
(`retry_one_header_failure`, `record_content_failure_details`) retries them;
`status_for_account` surfaces a `retrying` phase.

**Proposed change.** Add a `sync_failures(account_id, folder_path, uid, stage,
attempts, retry_at, last_error)` table; record on fetch/parse failure; retry due
rows at the top of the worker loop (after user requests), capped. Optionally
expose counts via D-Bus.

**Files.** `src/cache.rs`, `src/worker.rs`, `src/worker/backfill.rs`,
`src/dbus.rs` (optional status method).

---

## Suggested order

1. **8 (migrations)** first — everything else needs schema changes that apply
   cleanly to existing databases.
2. **1 (UIDVALIDITY)** and **6 (IDLE probe)** — small and directly prevent
   wrong behavior.
3. **2 (cursors)** then **3 (reconcile)** — the core sync-quality fix, best done
   together.
4. **5 (don't write on failure)** ✅ done — the stopgap that landed before **4
   (durable actions)** ✅ done. Together they make flags/moves/deletes
   server-authoritative: cache writes only happen on success, and failures are
   queued and retried instead of lost.
5. **10 (failure ledger)**, then **9 (threading)** as a follow-up.
   **7 (FTS)** ✅ done — the trigger-driven index was replaced by explicit,
   testable indexing; it was moved ahead of #10 because it touched every writer
   and was worth landing while that code was fresh.

## Deliberately not proposed

- Gmail REST / JMAP backends — a scope change, not a sync fix.
- Calendar / contacts / files / AI / OpenPGP — out of scope for this daemon.
- Pulling in Flectar's separate reader/writer DB threads — the single
  `Arc<Mutex<Cache>>` is adequate until profiling shows contention.
