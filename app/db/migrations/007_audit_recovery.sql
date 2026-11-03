CREATE TABLE campaign_creation_receipts (
 creation_key TEXT PRIMARY KEY, campaign_id INTEGER NOT NULL UNIQUE REFERENCES campaigns(id)
);
CREATE INDEX dm_campaign_state ON dm_reply_sequences(campaign_id,status,peer_id,id);
ALTER TABLE account_chat_access ADD COLUMN verification_generation INTEGER NOT NULL DEFAULT 0;
UPDATE accounts SET telegram_id=NULL WHERE account_health='REMOVED';
UPDATE dm_reply_sequences SET status='FAILED' WHERE status='ACTIVE' AND current_job_id IN
 (SELECT id FROM dispatch_jobs WHERE status='FAILED');
UPDATE dm_reply_sequences SET status='CANCELLED' WHERE status IN ('ACTIVE','FAILED')
 AND NOT EXISTS(SELECT 1 FROM dispatch_jobs j WHERE j.id=dm_reply_sequences.current_job_id AND j.status IN ('INFLIGHT','UNKNOWN'))
 AND (NOT EXISTS(SELECT 1 FROM campaign_members m WHERE m.account_id=dm_reply_sequences.account_id AND m.campaign_id=dm_reply_sequences.campaign_id)
 OR NOT EXISTS(SELECT 1 FROM pairs p WHERE p.id=dm_reply_sequences.pair_id AND dm_reply_sequences.account_id IN (p.account_a_id,p.account_b_id))
 OR EXISTS(SELECT 1 FROM campaigns ca WHERE ca.id=dm_reply_sequences.campaign_id AND ca.status='REMOVED'));
UPDATE reply_history SET reserved_job_id=NULL WHERE reserved_job_id IN
 (SELECT current_job_id FROM dm_reply_sequences WHERE status IN ('CANCELLED','FAILED'));
UPDATE dispatch_jobs SET status='CANCELLED' WHERE status='PENDING' AND account_id IS NOT NULL
 AND (NOT EXISTS(SELECT 1 FROM campaign_members m WHERE m.account_id=dispatch_jobs.account_id AND m.campaign_id=dispatch_jobs.campaign_id)
 OR NOT EXISTS(SELECT 1 FROM pairs p WHERE p.id=dispatch_jobs.pair_id AND dispatch_jobs.account_id IN (p.account_a_id,p.account_b_id)));
