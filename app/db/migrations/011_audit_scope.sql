-- Pair identities retain their campaign throughout their history.
-- Restore scope for earlier events so campaign error history includes them.
UPDATE audit_events SET campaign_id=(SELECT campaign_id FROM pairs WHERE pairs.id=audit_events.pair_id)
WHERE campaign_id IS NULL AND pair_id IS NOT NULL;
