-- =========================================================
-- SQLite schema for Telegram bot
-- =========================================================
-- Notes:
-- - All timestamps are stored as ISO-8601 TEXT
-- - Timezone: UTC (converted from Europe/Warsaw in app code)
-- - Lists are stored as JSON TEXT
-- - Designed for WAL mode and concurrent access
-- =========================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
-- ---------------------------------------------------------
-- 1. Chat History
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS chat_history (
    message_id INTEGER PRIMARY KEY,
    [timestamp] TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    text TEXT,
    image_text TEXT,
    reaction_emojis TEXT,        -- JSON array
    reaction_user_ids TEXT,      -- JSON array
    message_type TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chat_history_user_id
    ON chat_history(user_id);

CREATE INDEX IF NOT EXISTS idx_chat_history_timestamp
    ON chat_history([timestamp]);

-- ---------------------------------------------------------
-- 2. Cleaned Chat History
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS cleaned_chat_history (
    message_id INTEGER PRIMARY KEY,
    [timestamp] TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    final_username TEXT NOT NULL,
    text TEXT,
    image_text TEXT,
    reaction_emojis TEXT,        -- JSON array
    reaction_user_ids TEXT,      -- JSON array
    message_type TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cleaned_chat_user_id
    ON cleaned_chat_history(user_id);

CREATE INDEX IF NOT EXISTS idx_cleaned_chat_timestamp
    ON cleaned_chat_history([timestamp]);

-- ---------------------------------------------------------
-- 3. Commands Usage
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS commands_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    [timestamp] TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    command_name TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_commands_usage_user_id
    ON commands_usage(user_id);

CREATE INDEX IF NOT EXISTS idx_commands_usage_timestamp
    ON commands_usage([timestamp]);

-- ---------------------------------------------------------
-- 4. Reactions
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS reactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    [timestamp] TEXT NOT NULL,
    reacted_to_username TEXT NOT NULL,
    reacting_username TEXT NOT NULL,
    text TEXT,
    emoji TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reactions_message_id
    ON reactions(message_id);

CREATE INDEX IF NOT EXISTS idx_reactions_timestamp
    ON reactions([timestamp]);

-- ---------------------------------------------------------
-- 5. Users
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    final_username TEXT NOT NULL,
    nicknames TEXT              -- JSON array
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_final_username
    ON users(final_username);

-- ---------------------------------------------------------
-- 6. Cwel
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS cwel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    receiver_username TEXT NOT NULL,
    giver_username TEXT NOT NULL,
    reply_message_id INTEGER NOT NULL,
    [value] INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cwel_timestamp
    ON cwel(timestamp);

CREATE INDEX IF NOT EXISTS idx_cwel_receiver_username
    ON cwel(receiver_username);

-- ---------------------------------------------------------
-- 7. Credit History
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS credit_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    [timestamp] TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    target_user_id INTEGER,
    credit_change INTEGER NOT NULL,
    action_type TEXT NOT NULL,
    bet_type TEXT,
    success INTEGER NOT NULL     -- 0 / 1
);

CREATE INDEX IF NOT EXISTS idx_credit_history_user_id
    ON credit_history(user_id);

CREATE INDEX IF NOT EXISTS idx_credit_history_timestamp
    ON credit_history(timestamp);

-- ---------------------------------------------------------
-- 8. Credits
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS credits (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    credits INTEGER NOT NULL
);

