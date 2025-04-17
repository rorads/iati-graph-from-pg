#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

DB_DUMP_FILE="/tmp/pg_dump/iati.dump.gz"

echo "Checking for database dump file at ${DB_DUMP_FILE}..."

if [ -f "${DB_DUMP_FILE}" ]; then
    echo "Database dump found. Restoring database '${POSTGRES_DB}' from ${DB_DUMP_FILE}..."
    # Attempt restore using psql assuming it's a plain SQL dump (common for .gz)
    # The entrypoint runs this script as the POSTGRES_USER
    gunzip -c "${DB_DUMP_FILE}" | psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}"
    echo "Database restore completed successfully."
else
    echo "Database dump file not found. Skipping restore."
fi

echo "PostgreSQL initialization script finished." 