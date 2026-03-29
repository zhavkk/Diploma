-- Init script: create replication user for streaming replication
-- This runs on first database initialization via /docker-entrypoint-initdb.d/
CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator';

-- Grant replication slot usage
GRANT pg_use_reserved_connections TO replicator;

-- Grant privileges required by pg_rewind --source-server
GRANT pg_read_server_files TO replicator;
GRANT pg_monitor TO replicator;
