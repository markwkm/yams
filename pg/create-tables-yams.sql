CREATE TABLE systems (
  name VARCHAR(64) NOT NULL,
  plugins VARCHAR(64)[] NOT NULL DEFAULT '{}',
  lprocs SMALLINT NOT NULL,
  interfaces VARCHAR(64)[] NOT NULL DEFAULT '{"lo"}',
  disks VARCHAR(64)[] NOT NULL DEFAULT '{}'
);
