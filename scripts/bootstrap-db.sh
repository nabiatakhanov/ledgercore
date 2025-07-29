#!/bin/sh
set -eu
# Only the initial cluster bootstrap login can create roles. Runtime roles never
# inherit the owner role. psql variables quote passwords as SQL string literals.
psql -X --set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=db="$POSTGRES_DB" \
  --set=migrator_password="$LEDGER_MIGRATOR_PASSWORD" \
  --set=app_password="$LEDGER_APP_PASSWORD" \
  --set=fixture_password="$LEDGER_FIXTURE_PASSWORD" <<'SQL'
CREATE ROLE ledger_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'migrator_password';
CREATE ROLE ledger_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'app_password';
CREATE ROLE ledger_fixture LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'fixture_password';
ALTER DATABASE :"db" OWNER TO ledger_migrator;
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO ledger_migrator, ledger_app, ledger_fixture;
SQL
