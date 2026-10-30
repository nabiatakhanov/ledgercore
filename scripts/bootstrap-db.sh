#!/bin/sh
set -eu
# Only the initial cluster bootstrap login can create roles. Runtime roles never
# inherit the owner role. psql variables quote passwords as SQL string literals.
psql -X --set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=db="$POSTGRES_DB" \
  --set=migrator_password="$LEDGER_MIGRATOR_PASSWORD" \
  --set=app_password="$LEDGER_APP_PASSWORD" \
  --set=fixture_password="$LEDGER_FIXTURE_PASSWORD" \
  --set=publisher_password="${LEDGER_PUBLISHER_PASSWORD:-ledger-local-publisher-only}" \
  --set=activity_migrator_password="${ACTIVITY_MIGRATOR_PASSWORD:-activity-local-migrator-only}" \
  --set=activity_password="${ACTIVITY_APP_PASSWORD:-activity-local-app-only}" <<'SQL'
CREATE ROLE ledger_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'migrator_password';
CREATE ROLE ledger_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'app_password';
CREATE ROLE ledger_fixture LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'fixture_password';
CREATE ROLE ledger_publisher LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'publisher_password';
CREATE ROLE activity_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'activity_migrator_password';
CREATE ROLE activity_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'activity_password';
ALTER DATABASE :"db" OWNER TO ledger_migrator;
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO ledger_migrator, ledger_app, ledger_fixture, ledger_publisher;
SELECT format('CREATE DATABASE %I OWNER ledger_migrator', name)
FROM (VALUES ('ledgercore_test'), ('ledgercore_events_test'), ('ledger_events_e2e_test')) AS d(name)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname=d.name) \gexec
REVOKE ALL ON DATABASE ledgercore_test,ledgercore_events_test,ledger_events_e2e_test FROM PUBLIC;
GRANT CONNECT ON DATABASE ledgercore_test,ledgercore_events_test,ledger_events_e2e_test TO ledger_migrator,ledger_app,ledger_fixture,ledger_publisher;
SELECT format('CREATE DATABASE %I OWNER activity_migrator', name)
FROM (VALUES ('activity'), ('activity_events_test'), ('activity_events_e2e_test')) AS d(name)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname=d.name) \gexec
REVOKE ALL ON DATABASE activity,activity_events_test,activity_events_e2e_test FROM PUBLIC;
GRANT CONNECT ON DATABASE activity,activity_events_test,activity_events_e2e_test TO activity_migrator,activity_app;
SQL
