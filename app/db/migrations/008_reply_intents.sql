CREATE TABLE group_reply_intents (
 id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
 pair_id INTEGER NOT NULL REFERENCES pairs(id), account_id INTEGER NOT NULL REFERENCES accounts(id),
 chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id), peer_id INTEGER NOT NULL,
 payload TEXT NOT NULL, version INTEGER NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
 status TEXT NOT NULL DEFAULT 'WAITING', job_id INTEGER REFERENCES dispatch_jobs(id), created_at REAL NOT NULL
);
CREATE UNIQUE INDEX one_waiting_group_reply ON group_reply_intents(campaign_id,peer_id) WHERE status='WAITING';
CREATE INDEX group_reply_plan ON group_reply_intents(pair_id,status,id);
CREATE INDEX attempt_account_time ON message_attempts(account_id,started_at,job_id);
