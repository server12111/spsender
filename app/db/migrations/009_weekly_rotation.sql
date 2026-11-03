ALTER TABLE campaigns ADD COLUMN rotation_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE campaigns ADD COLUMN rotation_next_at REAL;
ALTER TABLE campaigns ADD COLUMN rotation_lead_days INTEGER NOT NULL DEFAULT 3;
ALTER TABLE campaigns ADD COLUMN rotation_messages INTEGER NOT NULL DEFAULT 2;
ALTER TABLE campaigns ADD COLUMN auto_subscribe INTEGER NOT NULL DEFAULT 1;
CREATE TABLE rotation_cycles (
 id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaigns(id), due_at REAL NOT NULL,
 status TEXT NOT NULL DEFAULT 'PREPARING', created_at REAL NOT NULL, completed_at REAL,
 plan_cursor INTEGER NOT NULL DEFAULT 0, pending_pair_id INTEGER REFERENCES pairs(id), planning_done INTEGER NOT NULL DEFAULT 0,
 UNIQUE(campaign_id,due_at)
);
CREATE TABLE rotation_exchanges (
 id INTEGER PRIMARY KEY, cycle_id INTEGER NOT NULL REFERENCES rotation_cycles(id),
 left_pair_id INTEGER NOT NULL REFERENCES pairs(id), right_pair_id INTEGER NOT NULL REFERENCES pairs(id),
 left_a INTEGER NOT NULL REFERENCES accounts(id), left_b INTEGER NOT NULL REFERENCES accounts(id),
 right_a INTEGER NOT NULL REFERENCES accounts(id), right_b INTEGER NOT NULL REFERENCES accounts(id),
 status TEXT NOT NULL DEFAULT 'PREPARING', last_error TEXT, applied_at REAL, draining_at REAL
);
CREATE INDEX rotation_exchange_cycle ON rotation_exchanges(cycle_id,status,id);
CREATE INDEX rotation_left_state ON rotation_exchanges(status,left_pair_id);
CREATE INDEX rotation_right_state ON rotation_exchanges(status,right_pair_id);
CREATE TABLE rotation_targets (
 id INTEGER PRIMARY KEY, exchange_id INTEGER NOT NULL REFERENCES rotation_exchanges(id),
 pair_id INTEGER NOT NULL REFERENCES pairs(id), source_pair_id INTEGER NOT NULL REFERENCES pairs(id),
 assignment_id INTEGER NOT NULL REFERENCES pair_chats(id), chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id),
 UNIQUE(exchange_id,pair_id,chat_pool_id)
);
CREATE TABLE rotation_progress (
 id INTEGER PRIMARY KEY, target_id INTEGER NOT NULL REFERENCES rotation_targets(id),
 account_id INTEGER NOT NULL REFERENCES accounts(id), joined INTEGER NOT NULL DEFAULT 0,
 can_write INTEGER NOT NULL DEFAULT 0, message_index INTEGER NOT NULL DEFAULT 0,
 next_action_at REAL NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0,
 check_count INTEGER NOT NULL DEFAULT 0,
 verified_at REAL NOT NULL DEFAULT 0,
 last_error TEXT, UNIQUE(target_id,account_id)
);
CREATE INDEX rotation_progress_target ON rotation_progress(target_id,account_id);
CREATE INDEX rotation_progress_account ON rotation_progress(account_id,target_id);
CREATE INDEX rotation_target_chat ON rotation_targets(chat_pool_id,pair_id,exchange_id);
CREATE TABLE membership_requests (
 id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaigns(id), pair_id INTEGER NOT NULL REFERENCES pairs(id),
 account_id INTEGER NOT NULL REFERENCES accounts(id), chat_pool_id INTEGER NOT NULL REFERENCES chat_pool(id),
 source_peer_id INTEGER NOT NULL, source_message_id INTEGER NOT NULL, source_sender_id INTEGER NOT NULL,
 action TEXT NOT NULL, link TEXT NOT NULL, payload TEXT NOT NULL, context_key TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'WAITING', job_id INTEGER REFERENCES dispatch_jobs(id),
 resolved_peer_id INTEGER, created_at REAL NOT NULL, last_error TEXT,
 UNIQUE(account_id,chat_pool_id,context_key,action,link)
);
CREATE INDEX membership_plan ON membership_requests(pair_id,status,id);
CREATE INDEX membership_parent ON membership_requests(account_id,chat_pool_id,status);
CREATE INDEX membership_bot_source ON membership_requests(account_id,resolved_peer_id,action,status);
CREATE INDEX global_chat_cooldown ON pair_chats(chat_pool_id,cooldown_until);
CREATE UNIQUE INDEX one_inflight_global_chat ON dispatch_jobs(chat_pool_id) WHERE status='INFLIGHT' AND chat_pool_id IS NOT NULL;
CREATE INDEX dispatch_global_chat ON dispatch_jobs(chat_pool_id,status);
CREATE TABLE account_entity_cache (
 account_id INTEGER NOT NULL REFERENCES accounts(id), peer_id INTEGER NOT NULL,
 encrypted_entity TEXT NOT NULL, updated_at REAL NOT NULL, PRIMARY KEY(account_id,peer_id)
);
CREATE INDEX entity_cache_age ON account_entity_cache(account_id,updated_at);
