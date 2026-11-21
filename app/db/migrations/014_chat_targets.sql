-- Symbolic chat references are durable even before the first account is ready.
-- Only verified Telegram peers enter chat_pool and the dispatch allocation.
CREATE TABLE chat_targets (
 id INTEGER PRIMARY KEY,
 link TEXT NOT NULL,
 topic_id INTEGER NOT NULL DEFAULT 0 CHECK(topic_id>=0),
 approved INTEGER NOT NULL DEFAULT 0,
 join_allowed INTEGER NOT NULL DEFAULT 0,
 greeting_allowed INTEGER NOT NULL DEFAULT 0,
 split_allowed INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'PENDING'
   CHECK(status IN ('PENDING','RESOLVING','REVIEW','RESOLVED','REMOVED')),
 attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt_at REAL NOT NULL DEFAULT 0,
 lease_owner TEXT,
 lease_until REAL,
 last_error TEXT,
 chat_pool_id INTEGER REFERENCES chat_pool(id),
 created_at REAL NOT NULL,
 UNIQUE(link,topic_id)
);
CREATE INDEX chat_target_queue ON chat_targets(status,next_attempt_at,id);
CREATE INDEX chat_pool_link ON chat_pool(link,topic_id);
