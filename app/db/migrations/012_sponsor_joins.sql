CREATE TABLE legacy_sponsor_joins (
 account_id INTEGER NOT NULL REFERENCES accounts(id), chat_id INTEGER NOT NULL,
 joined_at REAL NOT NULL, PRIMARY KEY(account_id,chat_id)
);
CREATE INDEX sponsor_join_account ON legacy_sponsor_joins(account_id);
