CREATE TABLE legacy_warmup_settings (
 account_id INTEGER PRIMARY KEY REFERENCES accounts(id), enabled INTEGER NOT NULL DEFAULT 1,
 started_at REAL NOT NULL, stage INTEGER NOT NULL DEFAULT 1, day_index INTEGER NOT NULL DEFAULT 1,
 next_action_at REAL, last_action_at REAL, last_action TEXT, last_join_at REAL,
 channels_subscribed INTEGER NOT NULL DEFAULT 0, chats_joined INTEGER NOT NULL DEFAULT 0
);
INSERT INTO legacy_warmup_settings(account_id,started_at) SELECT id,onboarding_started_at FROM accounts WHERE account_health<>'REMOVED';
CREATE TRIGGER legacy_warmup_auto AFTER INSERT ON accounts BEGIN
 INSERT INTO legacy_warmup_settings(account_id,started_at) VALUES (NEW.id,NEW.onboarding_started_at);
END;
CREATE TABLE warmup_partners(account_id INTEGER PRIMARY KEY REFERENCES accounts(id), added_at REAL NOT NULL);
CREATE TABLE warmup_channels(id INTEGER PRIMARY KEY, username TEXT NOT NULL UNIQUE, title TEXT, added_at REAL NOT NULL);
CREATE TABLE legacy_runtime_state(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE legacy_rpc_journal(
 id INTEGER PRIMARY KEY, operation_key TEXT NOT NULL UNIQUE, account_id INTEGER NOT NULL REFERENCES accounts(id),
 method TEXT NOT NULL, status TEXT NOT NULL, result TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
 lease_owner TEXT, lease_until REAL, error TEXT, context TEXT NOT NULL DEFAULT '{}'
 ,dispatch_job_id INTEGER REFERENCES dispatch_jobs(id)
);
CREATE INDEX legacy_rpc_account ON legacy_rpc_journal(account_id,status,lease_until);
CREATE VIEW legacy_account_catalog AS SELECT id, CASE WHEN account_health='OK' AND paused=0 THEN 1 ELSE 0 END AS is_active,
 (SELECT min(telegram_id) FROM admins) AS user_id FROM accounts;
UPDATE campaigns SET group_reply_enabled=0;
UPDATE dispatch_jobs SET status='CANCELLED' WHERE kind='GROUP_REPLY' AND status='PENDING';
UPDATE group_reply_intents SET status='CANCELLED' WHERE status='WAITING';
ALTER TABLE campaigns ADD COLUMN chat_delay_seconds REAL NOT NULL DEFAULT 3;
ALTER TABLE campaigns ADD COLUMN skip_unseen INTEGER NOT NULL DEFAULT 1;
ALTER TABLE pairs ADD COLUMN next_message_at REAL NOT NULL DEFAULT 0;
ALTER TABLE pair_chats ADD COLUMN interval_seconds INTEGER;
ALTER TABLE pair_chats ADD COLUMN message_cursor INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pair_chats ADD COLUMN delivery_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pair_chats ADD COLUMN legacy_options TEXT NOT NULL DEFAULT '{}';
ALTER TABLE campaign_templates ADD COLUMN legacy_options TEXT NOT NULL DEFAULT '{}';
ALTER TABLE campaigns ADD COLUMN legacy_options TEXT NOT NULL DEFAULT '{}';
