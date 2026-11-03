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
| 8 | ~~No migration runner~~ ✅ done | Schema changes are silent no-ops on existing DBs | S | L |
| 9 | ~~Threading owned implicitly by UI~~ ✅ done | No threads table; contract undocumented | L | L |
| 10 | ~~`sync_failures` ledger absent~~ ✅ done | Failed header/body fetches are lost for the session | S | L |
| 11 | ~~No D-Bus signals for real-time UI push~~ ✅ done | UI must poll daemon for new mail, status, and folder changes | S | L |
| 12 | ~~No D-Bus search method for FTS~~ ✅ done | `messages_fts` is maintained but unqueried via D-Bus | S | L |
| 13 | ~~Single connection drops INBOX push on folder switch~~ ✅ done | Viewing custom/sent folders stops real-time new mail alerts | M | M |
| 14 | ~~Folder operations unhedged by action ledger~~ ✅ done | Offline folder create/rename/delete drops on network glitch | S | L |
| 15 | ~~Corrupt MIME structure and missing filename on sent attachments~~ ✅ done | Attachments show as unnamed.dat or overwrite text body | S | L |
| 16 | Outbox table drops attachment paths and reply headers on retry | Offline sends lose attachments and threading on retry | S | M |
| 17 | Inline cid: images unmapped in body HTML | Embedded logos/screenshots appear broken in Qt MessageView | S | L |
| 18 | No IMAP APPEND to Sent folder on SMTP send | Non-Gmail accounts have empty Sent folder in client | M | M |
| 19 | Redundant dual LoadBody requests from UI | loadBody + listAttachments both trigger concurrent body fetches | S | L |

Effort: S = hours, M = a day or two, L = multi-day. Risk = chance of
regressing sync correctness if done carelessly.

---

## 1. Track UIDVALIDITY per folder

**Status: implemented.** `folders.uidvalidity` is now persisted (with an
additive column repair in `src/migrations.rs` for pre-migration databases).
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

**Files.** `src/cache.rs` (`upsert_folder`, new
`set_folder_uidvalidity` / `clear_folder_messages`), `src/imap.rs`,
`src/worker.rs` (`sync_folder_first_page`), `src/worker/backfill.rs`.

**Test.** Extend `src/cache.test.rs` with a case that changes UIDVALIDITY and
asserts the folder's rows are cleared and the FTS index no longer returns the
old messages.

---

## 2. Persist a per-folder sync cursor

**Status: implemented.** `folders.last_seen_uid` (forward high-water) and
`folders.backfill_cursor` (history walk position) are persisted with additive
column repairs in `src/migrations.rs`. Cold folders seed from a sequence-range page and
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

**Status: implemented.** Remote message expunges and flag mutations made by
other clients are reconciled locally without downloading entire message headers.

- **Storage & Cache Primitives** (`src/cache.rs`):
  - `Cache::cached_uids_in_range(account_id, folder_path, lo, hi)` returns sorted
    cached UIDs in the target range.
  - `Cache::pending_action_uids(account_id, folder_path)` queries active
    entries in `pending_actions` to protect in-flight local mutations from being
    clobbered or erroneously purged as expunges.
  - `Cache::update_message_flags_bulk(account_id, folder_path, &flags)` bulk updates
    `is_read` and `is_starred` flags in a single transaction, skipping rows
    whose state already matches to minimize SQLite disk writes.
  - `Cache::delete_messages(account_id, folder_path, &uids)` purges expunged
    messages across `messages`, cascading to `message_headers` and `bodies`,
    while explicitly deindexing from `messages_fts` and cleaning up
    `attachments` and `sync_failures`.
- **IMAP Protocol** (`src/imap.rs`):
  - `fetch_uid_flags(session, folder, uid_set)` fetches lightweight
    `UID FETCH <range> (UID FLAGS)` returning `(UID, is_read, is_starred)`
    in a single round trip without full header or envelope overhead.
- **Worker Active Window Reconciliation** (`src/worker.rs`):
  - On warm folder sync (`sync_folder_first_page`), reconciles an active window
    `[lo, hi]` (`RECONCILE_WINDOW = 1,000` UIDs) via `Worker::reconcile_window`.
    Missing cached UIDs (not pending locally) are deleted, server flag changes
    are bulk updated, and folder unread counts are updated from cache.
  - If `exists == 0`, all cached messages (excluding pending actions) are
    purged immediately.
  - If `high < last_seen_uid` (e.g. top messages deleted), `last_seen_uid` is
    clamped back down to `high`.
- **History Backfill Reconciliation** (`src/worker/backfill.rs`):
  - During descending history backfill chunks (`run_backfill_chunk`), cached
    UIDs in `[lo, hi]` absent from server summaries are expunged and unread
    counts updated.
- **Tests** (`src/cache.test.rs`):
  - Unit tests verifying `cached_uids_in_range`, `pending_action_uids` isolation,
    idempotent `update_message_flags_bulk`, and multi-table purge + FTS
    deindexing in `delete_messages`.

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

**Status: implemented.** Schema changes now go through an ordered,
transactional migration runner instead of `CREATE TABLE IF NOT EXISTS`.

- **Module** (`src/migrations.rs`): `MIGRATIONS: &[&str]` with the SQL inline
  (`M001_INIT` is the whole relational baseline) and `latest_version()`.
  `run(conn)` reads `PRAGMA user_version` — the count of migrations applied so
  far — applies `MIGRATIONS[version..]`, and refuses a database whose
  `user_version` is greater than `latest_version()`. Each migration runs in its
  own transaction *together with* the `user_version` bump, so a failed migration
  leaves the database exactly at its previous version rather than half-migrated.
- **Call sites** (`src/cache.rs`, `src/main.rs`): `Cache::create_tables` is
  replaced by `Cache::migrate`, and `Cache::open` no longer stamps the
  unconditional `PRAGMA user_version = 1` — that stamp wrote a version nobody
  read, which is why the counter could never be trusted. Startup is
  `open → migrate → fts_init`.
- **Legacy databases**: every database written before this change carries that
  unreliable `user_version = 1`, so it *skips* `M001_INIT`. Those databases were
  kept at the current shape by startup probes, and the one way they can still lag
  is a `folders` column added by a historical probe; `run` repairs that through
  `ensure_legacy_columns` (idempotent, `PRAGMA table_info`-guarded). New columns
  must go through a numbered migration instead.
- **FTS is deliberately not a migration.** Migrating `messages_fts` means
  detecting the old contentful shape, dropping legacy triggers, and rebuilding
  from `messages` — logic no plain SQL statement can express — so it stays in
  `Cache::fts_init`. The original proposal to fold `fts_init` into `001_init.sql`
  was not done, and `fts_init`'s create/migrate/reindex logic was left untouched.

**Tests.** Three cases in `src/cache.test.rs`: a version-0 database migrates to
latest and really creates the schema (not just stamps a version); re-running is a
no-op across startups; a newer `user_version` is rejected *before* any migration
runs, leaving the stamp untouched and no tables created. The two `folders`
column-repair tests were retargeted from `create_tables` to `migrate`.

**Original analysis.** `Cache::open` sets `PRAGMA user_version=1` but nothing reads it;
`create_tables` is all `CREATE TABLE IF NOT EXISTS` (`src/cache.rs`). Adding a
column to a released schema will silently not apply.

**Flectar reference.** `db/migrations.rs` holds an ordered
`include_str!` migration array, `LATEST_VERSION = len`, applies each pending
migration in its own transaction, and rejects a profile written by a newer app.

**Files.** new `src/migrations.rs`, `src/cache.rs` (`create_tables` → `migrate`,
`open` drops the version stamp), `src/main.rs`, `src/cache.test.rs`.

---

## 9. Decide and document threading ownership

**Status: decided — the UI owns threading.** No schema change; the whole gap
was "the contract is undocumented in-repo", so it is now written down where the
next reader will hit it:

- **The payload is the contract** (`src/types.rs`): `types::Message` — what
  `get_messages` serializes to the other repo — carries `in_reply_to`,
  `message_id`, and `references`, each documented as a UI threading input. The
  UI runs JWZ over those three fields; the daemon just passes them through
  verbatim. Verified: `Cache::load_messages` selects all three and
  `dbus::get_messages` serializes them, so nothing was missing — only the
  wording was.
- **The schema says so too** (`src/migrations.rs`): the `message_headers`
  comment states there is no `threads` table, no union-find, no subject
  fallback, and no aggregate recompute on flag/move/delete, and that adding one
  would be a one-way contract change worth taking only if a second client needs
  the cache.
- **`Cache::message_refs` is scoped** (`src/cache.rs`): documented as reply
  construction rather than thread resolution, pointing callers that want a whole
  conversation at `load_messages` instead.

**Chosen over the daemon-owned option** (`threads` + `message_refs` tables, a
`resolve_thread` JWZ-lite pass after `save_message_headers`, aggregates
recomputed on flag/move/delete) because the cache has exactly one consumer
today, and the daemon would then have to re-run that work on every mutation
just to maintain counts the UI can derive cheaply from the payload it already
gets.

**Flectar reference.** `sync/threading.rs::resolve_thread` does JWZ-lite
(references match, then a 14-day subject fallback), with a `threads` table whose
denormalized counts are recomputed on every change. That is the shape to follow
if the daemon-owned option is ever taken.

**Files.** `src/types.rs`, `src/migrations.rs`, `src/cache.rs`, this document.

---

## 10. Retry ledger for failed header/content fetches

**Status: implemented.** A failed per-message fetch is now recorded in a
`sync_failures` table and retried by the worker's priority loop instead of being
logged and forgotten.

- **Storage** (`src/cache.rs`): `sync_failures(account_id, folder_path, uid,
  stage, attempts, retry_at, last_error)` keyed on
  `(account_id, folder_path, uid, stage)`, with an `(account_id, retry_at)`
  index and an `ON DELETE CASCADE` FK to `accounts`. `record_sync_failure`
  upserts (bumping `attempts`), `claim_due_sync_failure` claims the most overdue
  due row and pushes its `retry_at` out so a retry that errors before it can be
  rescheduled is not re-claimed immediately, `clear_sync_failure` drops it on
  success, and `count_sync_failures` backs the startup log.
  `clear_folder_messages` clears a folder's rows on a UIDVALIDITY change (their
  UIDs no longer name the same messages).
- **Policy** (`src/sync_failures.rs`, +`.test.rs`): a flat 60 s retry
  (`RETRY_DELAY_SECS`, matching Flectar) and `MAX_SYNC_FAILURE_ATTEMPTS = 8`
  with `retry_decision`.
- **Execution** (`src/worker/sync_failures.rs`): `retry_one_sync_failure` claims
  and retries one due row per loop iteration; a `body` retry re-fetches the
  body, a `header` retry re-fetches the single UID's summary. Success clears the
  row; failure reschedules, or drops it once the cap is reached.
  `note_sync_failure` / `note_header_write_failures` are the recording entry
  points shared by all call sites.
- **Recording sites**: `run_one_body_prefetch` (covers attachment prefetch too)
  and the final failed attempt in `handle_load_body` record `body`;
  `write_summaries_to_cache` returns the messages whose header rows failed to
  persist and its callers (new-mail sync, backfill) record `header`.
- **Loop placement** (`src/worker.rs`): step 0.6 retries one due fetch — after
  user requests and queued actions, before body prefetch/backfill — so a
  transient failure heals without waiting for an unrelated re-sync.

**Scope note.** Batch header-window failures are deliberately *not* recorded
per-UID: a failed `UID FETCH` whose cursor is not advanced is already retried by
the next sync (item 2), so recording each UID would be redundant. The `header`
stage therefore covers the per-message case — a message whose header row could
not be persisted — which is what actually leaves a message headerless.

**Tests.** `src/sync_failures.test.rs` covers the retry/drop boundary.
`src/cache.test.rs` covers: a due row is claimed once and cleared on success;
re-recording accumulates attempts instead of duplicating; header and body
failures for one message are separate rows; clearing a folder drops its queued
failures; removing an account cascades them; and an unknown `stage` is rejected
rather than guessed.

**Original analysis.** A failed `fetch_summaries_by_uids` / `fetch_body` is
logged and dropped; the UID will be retried only if a later backfill diff or
page fetch happens to include it. There is no record of which messages still
lack headers or bodies.

**Flectar reference.** A durable `sync_failures` table stores `stage`
(header/content) + `retry_at` (60 s) and a background pass
(`retry_one_header_failure`, `record_content_failure_details`) retries them;
`status_for_account` surfaces a `retrying` phase.

**Files.** `src/cache.rs` (table + repo fns + `row_to_sync_failure`),
`src/sync_failures.rs` (+ `.test.rs`, new), `src/worker/sync_failures.rs` (new),
`src/worker.rs` (loop step + `write_summaries_to_cache` failure return),
`src/worker/prefetch.rs` (`fetch_and_store_body`), `src/worker/handlers.rs`,
`src/worker/backfill.rs`, `src/types.rs`, `src/main.rs`.

**Not done (deliberately).** No D-Bus status method exposes the failure count;
the startup log and per-retry tracing are enough for now, and the UI has no
`retrying` phase wired up. A `status_for_account`-style method is the natural
follow-up if the UI wants it.

---

## 11. Emit D-Bus signals for real-time UI updates

**Status: implemented.** Real-time push signals are emitted over D-Bus (`app.pelliper.Daemon`
at `/app/pelliper/Daemon`), eliminating the need for frontends to poll for new mail, flag
mutations, unread counter changes, or background outbox delivery.

- **D-Bus Signal Definitions** (`src/dbus.rs`):
  - `NewMailReceived(account_id: i64, folder_path: &str, count: i32)`
  - `FolderUnreadChanged(account_id: i64, folder_path: &str, unread_count: i32)`
  - `MessageFlagsChanged(account_id: i64, folder_path: &str, uid: i64, is_read: bool, is_starred: bool)`
  - `OutboxItemSent(account_id: i64, item_id: i64)`
- **Signal Bus & Dispatcher** (`src/types.rs`, `src/main.rs`):
  - `DaemonSignal` enum and `tokio::sync::mpsc::unbounded_channel` event channel decoupled
    from the D-Bus connection lifecycle so workers and tests run safely without a D-Bus bus.
  - Asynchronous forwarder loop in `src/main.rs` consumes internal `DaemonSignal` events and
    broadcasts them via `zbus::object_server::SignalEmitter`.
- **Emitters**:
  - `NewMailReceived`: Emitted when new mail arrives via IDLE push notifications or
    incremental folder sync (`sync_folder_first_page`).
  - `FolderUnreadChanged`: Emitted whenever folder unread counts are recomputed in
    `write_summaries_to_cache`, `reconcile_window`, `run_backfill_chunk`, empty mailbox purge,
    or local action application.
  - `MessageFlagsChanged`: Emitted for every modified message flag in `reconcile_window`
    (server changes) and `apply_local_effect` for `ActionKind::SetFlags` (client changes).
  - `OutboxItemSent`: Emitted by `outbox_flusher` when an offline message is successfully
    delivered via SMTP.

---

## 12. Expose Full-Text Search over D-Bus

**Status: implemented.** Added D-Bus method `search_messages(account_id: i64, query: &str) -> String`
and `Cache::search_messages(account_id, query)`:
- Sanitizes input query via `sanitize_fts_query` (splits tokens, strips wildcards/quotes,
  escapes internal quotes, and wraps in `"term"*` joined with `AND`) to safely allow
  prefix search without raw FTS5 syntax errors.
- Queries `messages_fts` joined to `messages` and `message_headers`, ranked by
  `bm25(messages_fts)` and `h.date_ts DESC`, returning up to 100 matching `SearchResult`
  records containing account ID, folder path, UID, subject, sender, date, preview, flags,
  and threading headers.
- If `account_id <= 0`, searches across all accounts; if `account_id > 0`, scopes results
  to that account. Empty or whitespace-only queries immediately return an empty result.

---

## 13. Dual-connection architecture for INBOX IDLE and background folder tracking

**Status: implemented.**
- **Connection 1 (Dedicated Push)**: Implemented in `src/worker/inbox_watcher.rs`. Spawns
  a dedicated concurrent task per account running its own IMAP connection permanently
  locked to `INBOX` in `IDLE`. Real-time incoming mail alerts (`NewMailReceived`,
  `FolderUnreadChanged`) and reconciliation trigger with zero latency regardless of
  what folder the user is currently browsing or backfilling.
- **Connection 2 (Worker)**: Handles active user views, lazy body downloads,
  action ledger draining, and global history backfill. When the user selects a folder,
  Connection 2 navigates to and IDLEs on that folder while Connection 1 continues
  watching `INBOX`.
- **Background Folder Tracking (Lightweight STATUS polling)**: In `do_periodic_resync`,
  Connection 2 queries `STATUS` (`UIDVALIDITY EXISTS UIDNEXT UNSEEN`) via `imap::folder_status`
  across all background account folders without opening individual connections or leaving
  the current folder. When `UIDNEXT` advances, it syncs the folder and updates unread counts.
- **Global Folder Backfill**: In `src/worker/backfill.rs`, `maybe_queue_next_backfill_folder`
  automatically advances through remaining account folders when the active folder's backfill
  reaches 0, ensuring offline caching across all mailboxes.

---

## 14. Durable action ledger for folder operations

**Status: ✅ done.** Flags, moves, and deletes are protected by the durable
`pending_actions` ledger (Gap #4). Folder mutations (`CreateFolder`, `RenameFolder`,
`DeleteFolder`) are now hedged by the ledger as well.

**Implementation.**
- Extended `ActionKind` with `CreateFolder`, `RenameFolder`, and `DeleteFolder` variants, along with `RenameFolderPayload` in `src/types.rs`.
- Added folder-specific permanent error detection strings to `is_permanent_error` in `src/actions.rs` ("already exists", "already in use", "invalid mailbox name", etc.).
- Implemented `Cache::rename_folder` in `src/cache.rs` with `PRAGMA defer_foreign_keys = ON;` in a transaction, atomically updating `folders`, `messages`, `message_headers`, `bodies`, `attachments`, `sync_failures`, `pending_actions`, and rewriting destination paths in queued `move` actions.
- Updated `Cache::delete_folder` in `src/cache.rs` to call `clear_folder_messages` to deindex FTS and clear messages, headers, bodies, attachments, and pending actions.
- Routed `handle_create_folder`, `handle_rename_folder`, and `handle_delete_folder` through `Worker::enqueue_and_process`.
- Handled folder action execution and local cache state application in `src/worker/actions.rs`.
- Added unit tests verifying atomic folder renaming, message/FTS cascading on delete, and action round-tripping.

**Files.** `src/types.rs`, `src/actions.rs`, `src/actions.test.rs`, `src/cache.rs`, `src/cache.test.rs`, `src/worker/actions.rs`, `src/worker/handlers.rs`.

---

## 15. Fix outgoing attachment MIME structure, filename headers, and recipient parsing

**Status: ✅ done.** Outgoing MIME structure, attachment headers, and recipient parsing
in `src/smtp.rs` now conform to RFC 2046 and RFC 5322.

**Implementation.**
- Extracted and structured message creation in `src/smtp.rs::build_message`.
- Wrapped `body_part` (`MultiPart::alternative()`) in `MultiPart::mixed()` whenever attachments are present, preventing email clients from treating attachments as alternative representations of the text body.
- Constructed each attachment using `lettre::message::Attachment::new(att.filename).body(data, content_type)`, properly generating `Content-Disposition: attachment; filename="..."`, `Content-Type`, and transfer encoding.
- Implemented `split_recipient_list` to parse `To`, `Cc`, and `Bcc` address lists while respecting quotes and angle brackets, preventing formatted display names like `"Doe, Jane" <jane@example.com>` from being split corruptedly across commas or semicolons.
- Added comprehensive unit tests in `src/smtp.test.rs` validating MIME boundary hierarchies, disposition headers, and display name parsing.

**Files.** `src/smtp.rs`, `src/smtp.test.rs`.

---

## 16. Outbox attachment and reply-header preservation across retries

**Status: proposed.** When an email fails to send via SMTP immediately (e.g.
offline during compose), `dbus::send_email` calls
`cache.enqueue_outbox(account_id, to, cc, "", "", subject, body)`. The `outbox`
table stores neither attachment paths nor reply threading headers (`in_reply_to`,
`references`). When `outbox_flusher` retries upon reconnect, it transmits the email
with an empty attachment list (`&[]`) and `None` for reply threading, permanently
stripping all user attachments and breaking the conversation thread chain.

**Proposed change.**
- Add `attachment_paths TEXT`, `in_reply_to TEXT`, and `references_ TEXT`
  columns to the `outbox` table via a numbered schema migration (`src/migrations.rs`).
- Update `cache.enqueue_outbox` and `load_pending_outbox` to store and load
  attachments and reply headers.
- Pass stored attachments and threading headers to `send_with_auth` in `src/outbox.rs`.

**Files.** `src/migrations.rs`, `src/cache.rs`, `src/types.rs`, `src/outbox.rs`, `src/dbus.rs`.

---

## 17. Resolve inline `cid:` images to displayable URLs for Qt view

**Status: proposed.** HTML emails often embed inline graphics (logos, avatars,
diagrams) referencing MIME parts via Content-ID (`<img src="cid:...">`). In
`src/imap.rs::parse_message_body`, these are parsed but the `cid:` references
remain untouched in the HTML string. Qt's `MessageView.qml` cannot resolve
`cid:` schemes, causing embedded graphics to render as broken image icons.

**Proposed change.**
- During `parse_message_body`, map each inline MIME part's `Content-ID` to its
  downloaded local path or base64 data URI (`data:image/...;base64,...`).
- Replace `src="cid:<id>"` in the sanitized HTML body before writing to the
  `bodies` table.

**Files.** `src/imap.rs`, `src/worker/prefetch.rs`.

---

## 18. IMAP `APPEND` to `Sent` folder on successful SMTP delivery

**Status: proposed.** When an email is sent via SMTP, Gmail automatically stores
a copy in `[Gmail]/Sent Mail`. However, standard IMAP/SMTP servers (Outlook,
Fastmail, Dovecot, Yahoo, cPanel) do not automatically save sent SMTP messages.
As a result, sent messages sent from Pelliper are absent from the `Sent` folder
for non-Gmail accounts.

**Proposed change.**
- After `send_with_auth` completes successfully (in `send_email` and
  `outbox_flusher`), issue an IMAP `APPEND` to the account's designated `Sent`
  special-use folder with the raw MIME bytes and `\Seen` flag.

**Files.** `src/imap.rs`, `src/worker.rs`, `src/outbox.rs`.

---

## 19. Deduplicate concurrent in-flight `LoadBody` requests

**Status: proposed.** When the user selects an email in `MainWindow.qml`, the UI
triggers `Pelliper.DaemonClient.loadBody(...)` and
`Pelliper.DaemonClient.listAttachments(...)` in parallel. If the body is not yet
cached, both D-Bus handlers enqueue a `WorkerRequest::LoadBody` for the exact
same UID, causing redundant IMAP body fetches.

**Proposed change.**
- Track in-flight `LoadBody` requests in `dbus.rs` or coalesce them in the
  worker so simultaneous calls for the same `(account, folder, uid)` share a
  single IMAP fetch and broadcast the result to all awaiting callers.

**Files.** `src/dbus.rs`, `src/worker.rs`.

---

## Suggested order

1. **8 (migrations)** ✅ done — everything else needs schema changes that apply
   cleanly to existing databases. In practice it landed *after* 1/2/4/10: those
   shipped through the `CREATE TABLE IF NOT EXISTS` + probe workaround first, and
   the runner absorbed that work as its `M001_INIT` baseline.
2. **1 (UIDVALIDITY)** and **6 (IDLE probe)** — small and directly prevent
   wrong behavior.
3. **2 (cursors)** then **3 (reconcile)** ⚠️ — core sync-quality; `delete_messages`
   primitive is ready in cache, worker reconciliation loop needs to be wired up.
4. **5 (don't write on failure)** ✅ done — the stopgap that landed before **4
   (durable actions)** ✅ done. Together they make flags/moves/deletes
   server-authoritative: cache writes only happen on success, and failures are
   queued and retried instead of lost.
5. **10 (failure ledger)** ✅ done, then **9 (threading)** ✅ done — the UI keeps
   ownership; the contract is now written down in the payload, the schema
   comment, and `message_refs`.
   **7 (FTS)** ✅ done — the trigger-driven index was replaced by explicit,
   testable indexing; it was moved ahead of #10 because it touched every writer
   and was worth landing while that code was fresh.
6. **15 (sent attachment MIME & filenames)** and **16 (outbox attachment retention)** —
   critical outgoing mail bugfixes to ensure recipients receive valid, named attachments.
7. **17 (cid: inline images)** and **19 (LoadBody deduplication)** — fixes broken
   newsletter images and double-fetching when selecting emails in the desktop UI.
8. **18 (IMAP APPEND to Sent)** — ensures outgoing emails appear in `Sent` on
   standard non-Gmail IMAP providers.
9. **11 (D-Bus signals)** and **12 (D-Bus FTS search)** — high-value desktop app
   responsiveness improvements (real-time push without polling, offline search API).
10. **14 (folder actions ledger)** and **13 (dual-connection INBOX IDLE)** —
    extended durability and real-time alerts while browsing non-inbox folders.

## Deliberately not proposed

- Gmail REST / JMAP backends — a scope change, not a sync fix.
- Calendar / contacts / files / AI / OpenPGP — out of scope for this daemon.
- Pulling in Flectar's separate reader/writer DB threads — the single
  `Arc<Mutex<Cache>>` is adequate until profiling shows contention.
- Bulk download of all historical message bodies — modern desktop clients (Apple
  Mail, Superhuman, Mailspring) sync 100% of headers for full offline list
  browsing and metadata search, but prefetch bodies only for recent windows
  (e.g., last 30–60 days). Indiscriminately fetching decades of old bodies and
  attachments bloats local SQLite databases into tens of gigabytes and triggers
  server bandwidth throttles (e.g. Gmail's 2.5 GB/day IMAP quota). Older bodies
  remain strictly on-demand on click (`load_body`).
