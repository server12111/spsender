CREATE TABLE api_credentials (
 id INTEGER PRIMARY KEY, name TEXT NOT NULL, api_id INTEGER NOT NULL UNIQUE CHECK(api_id>0),
 encrypted_hash TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)), created_at REAL NOT NULL
);
CREATE TABLE account_api_bindings (
 account_id INTEGER PRIMARY KEY REFERENCES accounts(id), credential_id INTEGER NOT NULL REFERENCES api_credentials(id),
 created_at REAL NOT NULL
);
CREATE INDEX api_bindings_credential ON account_api_bindings(credential_id);
