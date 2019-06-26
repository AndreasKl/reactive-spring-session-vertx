CREATE TABLE IF NOT EXISTS session (
    id UUID PRIMARY KEY,
    session_id text NOT NULL,
    session_data bytea,
    creation_time INT8 NOT NULL,
    last_accessed_time INT8  NOT NULL,
    expiry_time INT8 NOT NULL,
    max_inactive_interval INT4 NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS session_session_id_idx ON session (session_id);
CREATE INDEX IF NOT EXISTS session_expiry_time_idx ON session (expiry_time) WHERE expiry_time >= 0;