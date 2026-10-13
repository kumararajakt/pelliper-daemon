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
| 2 | No persisted sync cursor | Repeated full `UID SEARCH ALL` + newest-page refetch | M | M |
| 3 | No expunge/flag reconciliation | Stale rows; server-side changes never applied locally | M | M |
| 4 | No durable action ledger (non-send) | Offline/failed flags, moves, deletes silently diverge | L | M |
| 5 | Cache mutated even when IMAP op fails | Local state claims success that server rejected | S | L |
| 6 | No IDLE capability probe | Connects IDLE to servers that don't advertise it | S | L |
| 7 | Trigger-driven FTS fragility | A failed trigger silently drops body indexing | M | M |
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

**Current.** Each sync refetches the newest `FIRST_PAGE_SIZE`/`REOPEN_PAGE_SIZE`
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
(new bulk `load_cached_uids`/`delete_messages`/`update_flags_bulk`).

**Test.** Cache-level test: seed rows, delete some server-side, assert only the
survivors remain and FTS no longer matches the removed ones.

---

## 4. Durable action ledger for non-send mutations

**Current.** `Send` failures are durably queued (`outbox` table, retried every
30 s, capped at 10 attempts — `src/outbox.rs`). Flags, move, delete and folder
ops are **not**: `src/worker/handlers.rs` calls `do_set_flags` / `do_move` once,
retries only on an auth error, and then moves on. Offline or rejected mutations
are lost.

**Flectar reference.** `queue/mod.rs` + `db/repo/actions.rs` implement
`pending_actions`: optimistic local mutation first, durable intent row, atomic
`try_claim`, bounded slices (`MAX_ACTIONS_PER_SLICE = 20`, 2 s), exponential
backoff + jitter, `MAX_ATTEMPTS = 8`, transient vs permanent classification
(`is_permanent`), and `mark_failed_and_rollback` which undoes an optimistic move
only if it is still the newest intent for that message.

**Proposed change.**
- Generalize the existing `outbox` pattern into a `pending_actions` table:
  `id, account_id, message_id/uid, folder_path, kind, payload(json), state,
  attempts, not_before, created_at, last_error`.
- Enqueue the intent before mutating the server; apply the local cache update
  only when the action succeeds (or explicitly mark it optimistic and roll back
  on permanent failure).
- Drain due actions at the top of the worker loop (already the priority-0 slot),
  before background prefetch/backfill.
- Keep sends on the existing dedicated path; the `payload` can reuse
  `EmailAttachment`/outbox fields.

**Files.** `src/cache.rs` (new table + repo fns), `src/worker/handlers.rs`,
`src/outbox.rs` (or a new `src/actions.rs`), `src/worker.rs`.

**Test.** Pure-state tests for attempt/backoff/permanent classification, plus a
cache test that a claimed action is not returned again.

---

## 5. Don't mutate the cache when the IMAP op failed

**Current.** `handle_set_flags` (`src/worker/handlers.rs`) calls `do_set_flags`,
then unconditionally runs `cache.update_message_flags` and
`update_folder_unread`. `handle_move_message` and `handle_delete_message` do the
same with `cache.move_message` / `cache.delete_message`. On a non-auth server
error the local cache records a change the server rejected.

**Flectar reference.** Local mutation is tracked as intent, and
`mark_failed_and_rollback` reverses an optimistic move when the action
permanently fails.

**Proposed change.** Until item 4 lands, at minimum gate the cache write on the
IMAP result: only update local state when the STORE/MOVE/EXPUNGE returned `Ok`,
and emit a warning (or a D-Bus error) otherwise. This is a small change but must
be paired with the retry semantics so offline is not simply "lost" — hence it
pairs with item 4.

**Files.** `src/worker/handlers.rs`.

**Test.** Hard to reach without a mock server; start with a unit-testable helper
that maps `Result` to "should apply local mutation".

---

## 6. Probe IDLE capability once per connection

**Current.** `idle_wait` (`src/worker/idle.rs`) issues `session.idle()` and
`handle.init()` whenever a session exists and a folder is selected; it never
checks the server's CAPABILITY. Servers without `IDLE` (or that reject `IDLE`
mid-session) cause a disconnect/reselect loop instead of falling back to
polling.

**Flectar reference.** `imap/mod.rs::supports_idle` probes CAPABILITY once per
connection; the actor only enters IDLE when advertised and otherwise polls on a
deadline.

**Proposed change.** On connect, read CAPABILITY and cache `supports_idle` on the
`Worker`; in the priority loop, only take the IDLE branch when true, otherwise
rely on the existing `RESYNC_INTERVAL_SECS` timer.

**Files.** `src/imap.rs` (new `supports_idle`), `src/worker.rs`, `src/worker/idle.rs`.

**Test.** Unit test the CAPABILITY parser in `src/imap.test.rs`.

---

## 7. Contentless, manually maintained FTS (optional hardening)

**Current.** `messages_fts` is kept in sync by triggers that rebuild the whole
row on every header/body insert (`src/cache.rs::fts_init`). The comments record
that a partial-column insert used to silently roll back every cached body; the
design remains fragile to future trigger edits.

**Flectar reference.** `messages_fts` is contentless (`content=''`,
`contentless_delete=1`) with **no triggers**; `repo::search::index_message`
explicitly deletes + reinserts, and `repo::messages::delete` removes the index
row. Callers index at known points (store_headers, body save, move, delete).

**Proposed change.** Only if FTS keeps causing trouble: drop the triggers, make
`messages_fts` contentless, and call an explicit `index_message` from
`save_message_headers`, `save_body`, `move_message`, and `delete_message`.
This is a larger change with its own ordering rules, so treat as a follow-up.

**Files.** `src/cache.rs` (`fts_init` → schema + index fn), all FTS writers.

**Test.** Existing `src/cache.test.rs` FTS cases should keep passing; add one for
move and one for delete.

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
4. **5 (don't write on failure)** immediately, as a stopgap, before **4
   (durable actions)** lands.
5. **10 (failure ledger)**, then **7 (FTS)** and **9 (threading)** as follow-ups.

## Deliberately not proposed

- Gmail REST / JMAP backends — a scope change, not a sync fix.
- Calendar / contacts / files / AI / OpenPGP — out of scope for this daemon.
- Pulling in Flectar's separate reader/writer DB threads — the single
  `Arc<Mutex<Cache>>` is adequate until profiling shows contention.
