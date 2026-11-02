ALTER TABLE campaigns ADD COLUMN dm_greeting_phrase TEXT NOT NULL DEFAULT 'Привет!';
ALTER TABLE campaigns ADD COLUMN dm_sticker_emoji TEXT NOT NULL DEFAULT '👋';
CREATE TABLE dm_reply_sequences (
 id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
 pair_id INTEGER NOT NULL REFERENCES pairs(id), account_id INTEGER NOT NULL REFERENCES accounts(id),
 peer_id INTEGER NOT NULL, reply_to INTEGER NOT NULL, version INTEGER NOT NULL,
 steps TEXT NOT NULL, step_index INTEGER NOT NULL DEFAULT 0, next_step_at REAL NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'ACTIVE', current_job_id INTEGER REFERENCES dispatch_jobs(id), created_at REAL NOT NULL
);
CREATE UNIQUE INDEX dm_one_active_peer ON dm_reply_sequences(campaign_id,peer_id) WHERE status='ACTIVE';
CREATE INDEX dm_sequence_pair ON dm_reply_sequences(pair_id,status,id);
UPDATE greeting_progress SET next_step_at=0;
