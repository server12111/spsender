-- Per-account device fingerprint (device_model/system_version/app_version as JSON).
-- Empty string means "legacy account": keep the old default device to avoid changing
-- the device of an already-authorized session.
ALTER TABLE accounts ADD COLUMN device_profile TEXT NOT NULL DEFAULT '';
