ALTER TABLE greeting_progress ADD COLUMN next_step_at REAL NOT NULL DEFAULT 0;

UPDATE greeting_progress SET next_step_at=COALESCE((
 SELECT max(r.confirmed_at)+4 FROM message_results r
 JOIN dispatch_jobs j ON j.id=r.job_id
 WHERE j.campaign_id=greeting_progress.campaign_id AND j.chat_pool_id=greeting_progress.chat_pool_id
 AND j.kind='GREETING'
),0) WHERE main_done=0 AND (emoji_done=1 OR phrase_done=1);

UPDATE dispatch_jobs SET planned_at=max(planned_at,COALESCE((
 SELECT g.next_step_at FROM greeting_progress g
 WHERE g.campaign_id=dispatch_jobs.campaign_id AND g.chat_pool_id=dispatch_jobs.chat_pool_id
),0)) WHERE kind='GREETING' AND status='PENDING';
