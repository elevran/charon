package sqlite

const schema = `
CREATE TABLE IF NOT EXISTS responses (
	id                   TEXT    PRIMARY KEY,
	previous_response_id TEXT,
	chain_root_id        TEXT    NOT NULL,
	position             INTEGER NOT NULL,
	is_checkpoint        INTEGER NOT NULL DEFAULT 0,
	owner_principal      TEXT    NOT NULL DEFAULT '',
	model                TEXT    NOT NULL DEFAULT '',
	status               TEXT    NOT NULL,
	created_at           INTEGER NOT NULL,
	expires_at           INTEGER,
	payload_key          TEXT    NOT NULL,
	checkpoint_key       TEXT,
	background           INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_responses_chain
	ON responses(chain_root_id, position);

CREATE INDEX IF NOT EXISTS idx_responses_expires
	ON responses(expires_at)
	WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS write_intents (
	intent_id      TEXT    PRIMARY KEY,
	response_id    TEXT    NOT NULL,
	reservation_id TEXT    NOT NULL DEFAULT '',
	payload_key    TEXT    NOT NULL,
	phase          TEXT    NOT NULL,
	created_at     INTEGER NOT NULL,
	updated_at     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_write_intents_phase
	ON write_intents(updated_at)
	WHERE phase NOT IN ('committed', 'failed');
`
