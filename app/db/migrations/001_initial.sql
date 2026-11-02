CREATE TABLE admins (telegram_id INTEGER PRIMARY KEY);
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE campaigns (
 id INTEGER PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'DRAFT',
 version INTEGER NOT NULL DEFAULT 1, timezone TEXT NOT NULL, interval_seconds INTEGER NOT NULL DEFAULT 300 CHECK(interval_seconds>=1),
 chat_limit INTEGER NOT NULL DEFAULT 20 CHECK(chat_limit>0), selection TEXT NOT NULL DEFAULT 'sequential',
 greeting_enabled INTEGER NOT NULL DEFAULT 0, greeting_split INTEGER NOT NULL DEFAULT 0,
 group_reply_enabled INTEGER NOT NULL DEFAULT 0, dm_first_enabled INTEGER NOT NULL DEFAULT 0,
 dm_always_enabled INTEGER NOT NULL DEFAULT 0, notify_dm INTEGER NOT NULL DEFAULT 0,
 reply_cooldown INTEGER NOT NULL DEFAULT 300, created_at REAL NOT NULL
);
CREATE TABLE accounts (
 id INTEGER PRIMARY KEY, name TEXT NOT NULL, phone_mask TEXT NOT NULL, telegram_id INTEGER UNIQUE,
 onboarding_started_at REAL NOT NULL, onboarding_stage TEXT NOT NULL DEFAULT 'SILENT_24H',
 stage_started_at REAL NOT NULL, account_health TEXT NOT NULL DEFAULT 'DISCONNECTED',
 membership_status TEXT NOT NULL DEFAULT 'FREE', paused INTEGER NOT NULL DEFAULT 0,
 prepared_at REAL, joined_at REAL, ready_at REAL, next_action_at REAL NOT NULL DEFAULT 0,
 flood_until REAL NOT NULL DEFAULT 0, last_error TEXT, created_at REAL NOT NULL
);
CREATE TABLE account_sessions (
 account_id INTEGER PRIMARY KEY REFERENCES accounts(id), encrypted_session TEXT NOT NULL,
 encrypted_phone TEXT NOT NULL, updated_at REAL NOT NULL
);
CREATE TABLE campaign_members (
 account_id INTEGER PRIMARY KEY REFERENCES accounts(id), campaign_id INTEGER NOT NULL REFERENCES campaigns(id)
);
CREATE TABLE pairs (
 id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
 account_a_id INTEGER UNIQUE REFERENCES accounts(id), account_b_id INTEGER UNIQUE REFERENCES accounts(id),
 chat_limit INTEGER NOT NULL CHECK(chat_limit>0), status TEXT NOT NULL DEFAULT 'WAITING_PARTNER',
 pair_dispatch_started_at REAL, active_dispatch_seconds REAL NOT NULL DEFAULT 0,
 active_since REAL, waiting_since REAL, created_at REAL NOT NULL,
 CHECK(account_a_id<>account_b_id)
);
CREATE TABLE chat_pool (
 id INTEGER PRIMARY KEY, chat_id INTEGER NOT NULL, topic_id INTEGER NOT NULL DEFAULT 0,
 title TEXT NOT NULL, link TEXT NOT NULL, approved INTEGER NOT NULL DEFAULT 0,
 greeting_allowed INTEGER NOT NULL DEFAULT 0, split_allowed INTEGER NOT NULL DEFAULT 0,
 join_allowed INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'FREE',
 created_at REAL NOT NULL, UNIQUE(chat_id,topic_id)
);
CREATE TABLE pair_chats (
 id INTEGER PRIMARY KEY, pair_id INTEGER NOT NULL REFERENCES pairs(id), chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id),
 status TEXT NOT NULL DEFAULT 'ASSIGNED', assigned_at REAL NOT NULL, removed_at REAL,
 last_sent_at REAL, cooldown_until REAL NOT NULL DEFAULT 0, last_result TEXT, last_error TEXT
);
CREATE UNIQUE INDEX one_active_assignment ON pair_chats(chat_pool_id) WHERE removed_at IS NULL;
CREATE INDEX pair_chat_order ON pair_chats(pair_id,id);
CREATE TABLE account_chat_access (
 account_id INTEGER NOT NULL REFERENCES accounts(id), chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id),
 joined INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, manual_required INTEGER NOT NULL DEFAULT 0,
 last_checked_at REAL, last_error TEXT, PRIMARY KEY(account_id,chat_pool_id)
);
CREATE TABLE campaign_templates (
 id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaigns(id), version INTEGER NOT NULL,
 kind TEXT NOT NULL, position INTEGER NOT NULL, text TEXT NOT NULL, media_path TEXT, media_kind TEXT,
 UNIQUE(campaign_id,version,kind,position)
);
CREATE VIEW greeting_templates AS SELECT * FROM campaign_templates WHERE kind='greeting';
CREATE VIEW autoresponder_templates AS SELECT * FROM campaign_templates WHERE kind IN ('group','dm_first','dm_always');
CREATE TABLE account_onboarding_events (
 id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL REFERENCES accounts(id), stage TEXT NOT NULL,
 created_at REAL NOT NULL, UNIQUE(account_id,stage)
);
CREATE TABLE dispatch_jobs (
 id INTEGER PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
 pair_id INTEGER NOT NULL REFERENCES pairs(id), account_id INTEGER REFERENCES accounts(id),
 chat_pool_id INTEGER REFERENCES chat_pool(id), kind TEXT NOT NULL, payload TEXT NOT NULL,
 version INTEGER NOT NULL, planned_at REAL NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
 lease_owner TEXT, lease_until REAL, started_at REAL, completed_at REAL, last_error TEXT,
 created_at REAL NOT NULL
);
CREATE INDEX job_queue ON dispatch_jobs(status,planned_at,id);
CREATE UNIQUE INDEX one_inflight_account ON dispatch_jobs(account_id) WHERE status='INFLIGHT';
CREATE UNIQUE INDEX one_inflight_chat ON dispatch_jobs(pair_id,chat_pool_id) WHERE status='INFLIGHT' AND chat_pool_id IS NOT NULL;
CREATE TABLE message_attempts (
 id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES dispatch_jobs(id), account_id INTEGER NOT NULL REFERENCES accounts(id),
 started_at REAL NOT NULL, outcome TEXT NOT NULL DEFAULT 'STARTED', error TEXT
);
CREATE TABLE message_results (
 id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL UNIQUE REFERENCES dispatch_jobs(id), attempt_id INTEGER NOT NULL REFERENCES message_attempts(id),
 api_message_id INTEGER, confirmed_at REAL NOT NULL, result TEXT NOT NULL
);
CREATE TABLE greeting_progress (
 campaign_id INTEGER NOT NULL REFERENCES campaigns(id), chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id),
 emoji_done INTEGER NOT NULL DEFAULT 0, phrase_done INTEGER NOT NULL DEFAULT 0, main_done INTEGER NOT NULL DEFAULT 0,
 chosen TEXT NOT NULL, PRIMARY KEY(campaign_id,chat_pool_id)
);
CREATE TABLE incoming_events (
 id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL REFERENCES accounts(id), peer_id INTEGER NOT NULL,
 message_id INTEGER NOT NULL, sender_id INTEGER NOT NULL, private INTEGER NOT NULL,
 created_at REAL NOT NULL, UNIQUE(account_id,peer_id,message_id)
);
CREATE TABLE reply_history (
 campaign_id INTEGER NOT NULL REFERENCES campaigns(id), peer_id INTEGER NOT NULL, kind TEXT NOT NULL,
 last_replied_at REAL, reserved_job_id INTEGER REFERENCES dispatch_jobs(id),
 PRIMARY KEY(campaign_id,peer_id,kind)
);
CREATE TABLE chat_failures (
 pair_id INTEGER NOT NULL REFERENCES pairs(id), chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id),
 account_id INTEGER NOT NULL REFERENCES accounts(id), first_at REAL NOT NULL, last_at REAL NOT NULL, reason TEXT NOT NULL,
 PRIMARY KEY(pair_id,chat_pool_id,account_id)
);
CREATE TABLE chat_issues (
 id INTEGER PRIMARY KEY, pair_id INTEGER NOT NULL REFERENCES pairs(id), chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id),
 reason TEXT NOT NULL, first_at REAL NOT NULL, last_at REAL NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN'
);
CREATE UNIQUE INDEX one_open_issue ON chat_issues(pair_id,chat_pool_id) WHERE status='OPEN';
CREATE TABLE chat_issue_decisions (
 issue_id INTEGER PRIMARY KEY REFERENCES chat_issues(id), action TEXT NOT NULL, admin_id INTEGER NOT NULL, decided_at REAL NOT NULL,
 replacement_chat_id INTEGER REFERENCES chat_pool(id)
);
CREATE TABLE notifications_outbox (
 id INTEGER PRIMARY KEY, dedup_key TEXT NOT NULL UNIQUE, admin_id INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
 kind TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
 attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at REAL NOT NULL, lease_until REAL, lease_owner TEXT,
 created_at REAL NOT NULL, sent_at REAL, last_error TEXT
);
CREATE TABLE audit_events (
 id INTEGER PRIMARY KEY, event TEXT NOT NULL, campaign_id INTEGER, pair_id INTEGER, account_id INTEGER,
 chat_pool_id INTEGER, detail TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL
);
CREATE INDEX audit_time ON audit_events(created_at,id);
CREATE TABLE ui_states (admin_id INTEGER PRIMARY KEY, state TEXT NOT NULL, data TEXT NOT NULL, expires_at REAL NOT NULL);
CREATE TABLE ui_actions (
 token TEXT PRIMARY KEY, admin_id INTEGER NOT NULL, action TEXT NOT NULL, data TEXT NOT NULL,
 expires_at REAL NOT NULL, once_only INTEGER NOT NULL, consumed_at REAL
);
CREATE TABLE worker_leases (name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at REAL NOT NULL);
