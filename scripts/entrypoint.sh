#!/usr/bin/env bash
set -e

# 1) Environment setup
export HIVE_HOME=/opt/hive-metastore
export HADOOP_HOME=/opt/hadoop
export PATH="$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH"
export JAVA_HOME=${JAVA_HOME:-$(readlink -f /usr/bin/java | sed "s:bin/java::")}

echo "[entrypoint] HIVE_HOME=$HIVE_HOME"
echo "[entrypoint] HADOOP_HOME=$HADOOP_HOME"
echo "[entrypoint] JAVA_HOME=$JAVA_HOME"

# 2) Wait for Postgres
echo "[entrypoint] waiting for Postgres at $DB_HOST:$DB_PORT…"
until PGPASSWORD="$DB_PASSWORD" psql \
     -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c '\q' 2>/dev/null; do
  echo "[entrypoint] Postgres unavailable, sleeping 5s…"
  sleep 5
done
echo "[entrypoint] Postgres is up"

# 3) Test database connectivity
echo "[entrypoint] Testing database connectivity..."
PGPASSWORD="$DB_PASSWORD" psql \
  -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" \
  -c "SELECT version();" || {
    echo "[entrypoint] ERROR: Cannot query database"
    exit 1
  }

# 4) Schema initialization check
TABLE_COUNT=$(PGPASSWORD="$DB_PASSWORD" psql \
  -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" \
  -tAc "SELECT count(*) FROM pg_tables WHERE schemaname='public';")

if [[ "$TABLE_COUNT" -lt 5 ]]; then
  echo "[entrypoint] initializing Hive Metastore schema…"
  /opt/hive-metastore/bin/schematool \
    -dbType postgres \
    -initSchema \
    --verbose
else
  echo "[entrypoint] schema already initialized ($TABLE_COUNT tables)"
fi

# 5) Start metastore
echo "[entrypoint] starting Hive Metastore on port $HIVE_METASTORE_PORT"

# Start in background first to check for immediate failures
/opt/hive-metastore/bin/start-metastore -p "$HIVE_METASTORE_PORT" &
METASTORE_PID=$!

# Wait a few seconds and check if it's still running
sleep 10
if ! kill -0 $METASTORE_PID 2>/dev/null; then
    echo "[entrypoint] ERROR: Metastore failed to start"
    wait $METASTORE_PID
    exit 1
fi

echo "[entrypoint] Metastore appears to be running (PID: $METASTORE_PID)"
echo "[entrypoint] Waiting for metastore to fully initialize..."

# Wait longer for full initialization (metastore can take time to start)
sleep 30

# Test if we can connect to the port
if timeout 5 bash -c "</dev/tcp/localhost/$HIVE_METASTORE_PORT" 2>/dev/null; then
    echo "[entrypoint] SUCCESS: Metastore is accepting connections on port $HIVE_METASTORE_PORT"
else
    echo "[entrypoint] WARNING: Cannot connect to port $HIVE_METASTORE_PORT yet, but process is running"
    echo "[entrypoint] This might be normal - metastore can take several minutes to fully initialize"
fi

echo "[entrypoint] Metastore startup process complete. Container will remain running."

# Keep the container running by waiting for the metastore process
wait $METASTORE_PID