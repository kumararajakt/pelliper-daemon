CREATE TABLE IF NOT EXISTS accounts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    email              TEXT NOT NULL UNIQUE,
    display_name       TEXT NOT NULL,
    imap_host          TEXT NOT NULL,
    imap_port          INTEGER NOT NULL,
    imap_tls           TEXT NOT NULL,
    smtp_host          TEXT NOT NULL,
    smtp_port          INTEGER NOT NULL,
    smtp_tls           TEXT NOT NULL,
    auth_type          TEXT NOT NULL,
    oauth_provider     TEXT,
    sync_interval_secs INTEGER NOT NULL DEFAULT 300,
    enabled            INTEGER NOT NULL DEFAULT 1,
    last_error         TEXT
);

CREATE TABLE IF NOT EXISTS folders (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id     INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    full_path      TEXT NOT NULL,
    delimiter      TEXT NOT NULL DEFAULT '/',
    role           TEXT NOT NULL,
    priority       INTEGER NOT NULL,
    uid_validity   INTEGER,
    uid_next       INTEGER NOT NULL DEFAULT 1,
    highest_modseq INTEGER,
    total_count    INTEGER NOT NULL DEFAULT 0,
    unread_count   INTEGER NOT NULL DEFAULT 0,
    headers_synced INTEGER NOT NULL DEFAULT 0,
    bodies_synced  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(account_id, full_path)
);

CREATE TABLE IF NOT EXISTS messages (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id        INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    folder_id         INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
    uid               INTEGER NOT NULL,
    message_id        TEXT,
    in_reply_to       TEXT,
    references_header TEXT,
    thread_id         TEXT NOT NULL,
    subject           TEXT NOT NULL DEFAULT '',
    from_json         TEXT NOT NULL DEFAULT '[]',
    to_json           TEXT NOT NULL DEFAULT '[]',
    cc_json           TEXT NOT NULL DEFAULT '[]',
    bcc_json          TEXT NOT NULL DEFAULT '[]',
    date_ts           INTEGER NOT NULL,
    internal_date_ts  INTEGER NOT NULL,
    size_bytes        INTEGER NOT NULL DEFAULT 0,
    flags_bitmask     INTEGER NOT NULL DEFAULT 0,
    snippet           TEXT NOT NULL DEFAULT '',
    has_attachments   INTEGER NOT NULL DEFAULT 0,
    body_synced       INTEGER NOT NULL DEFAULT 0,
    UNIQUE(folder_id, uid)
);

CREATE TABLE IF NOT EXISTS message_bodies (
    message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    text_plain TEXT,
    text_html  TEXT,
    raw_size   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS attachments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id  INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    part_id     TEXT NOT NULL,
    content_id  TEXT,
    filename    TEXT NOT NULL,
    mime_type   TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL DEFAULT 0,
    is_inline   INTEGER NOT NULL DEFAULT 0,
    local_path  TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    message_id UNINDEXED,
    subject,
    from_text,
    to_text,
    body_text
);

CREATE TABLE IF NOT EXISTS outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id    INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    payload_json  TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'queued',
    retry_count   INTEGER NOT NULL DEFAULT 0,
    next_retry_ts INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT
);

CREATE TABLE IF NOT EXISTS pending_ops (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id    INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    folder_id     INTEGER REFERENCES folders(id) ON DELETE CASCADE,
    message_id    INTEGER,
    uid           INTEGER,
    op_type       TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    retry_count   INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_ts    INTEGER NOT NULL
);
