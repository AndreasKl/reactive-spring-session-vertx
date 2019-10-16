CREATE TABLE session (
  id VARCHAR(36) PRIMARY KEY,
  session_id VARCHAR(36) NOT NULL,
  session_data BLOB,
  creation_time BIGINT NOT NULL,
  last_accessed_time BIGINT  NOT NULL,
  expiry_time BIGINT NOT NULL,
  max_inactive_interval INTEGER NOT NULL
);
CREATE UNIQUE INDEX session_session_id_idx ON session (session_id);
CREATE INDEX session_expiry_time_idx ON session (expiry_time);