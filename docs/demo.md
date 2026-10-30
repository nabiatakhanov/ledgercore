# Events demonstration

From the repository root, with Docker Desktop/Engine and Compose v2 available:

```sh
make up
make demo
make reconcile
```

PowerShell equivalent (no Make installation required):

```powershell
docker compose up --build -d --wait --wait-timeout 180
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/demo-events.ps1
docker compose --profile tools run --rm --no-deps ledgerctl reconcile-activity
```

`make demo` selects the PowerShell or Bash implementation for the host. It uses
only the `ledgercore-events` Compose project. Public Gateway ports are18080 and18081.
Default local credentials are in Compose and `.env.example`; use private overrides
for anything beyond this local demonstration. Keep private service ports private.

The script checks health with bounded waits and records each command's exit code,
duration, stdout and stderr. A normal run takes several minutes, including actual
Redis window expiry and broker recovery. The default quota is60 requests per
first-request1-minute window. Set `RATE_LIMIT=60` and `RATE_WINDOW=1m` if your
environment overrides them. No Redis keys, Kafka offsets or SQL rows are deleted.

The sequence is:

1. Build/start the stack; observe any previous quota window expiring, and make a
   one-request limiter probe.
2. Reuse two deterministic wallets, fund each once with1,000,000 kopecks, transfer
   2,500 kopecks and fully reverse it. Replay the transfer key and verify the same
   operation ID. Wait for both inbox IDs, then read Activity through Gateway/gRPC
   and run both financial and projection audits.
3. Stop Kafka, accept20 new1-kopeck transfers, and assert at least20 unpublished
   outbox events. Hold the outage for10 seconds by default. Stop publisher during
   that outage and require clean bounded exit. Restart Kafka/publisher, wait for
   complete projection, and audit again. The measured outage includes Docker stop
   and broker readiness time as well as the explicit hold.
4. Stop Activity, accept20 more transfers, assert at least20 unprojected events,
   restart Activity, and measure catch-up. Financial writes remain available.
5. Replay retained Kafka records with a new consumer-group ID into the existing
   projection. Unique inbox, entry and summary counts must remain unchanged.
6. After the existing quota window expires, concurrently send80 requests through
   two Gateways with one client credential and one operation key. Require60
   successful responses and20 HTTP429 responses, with valid Retry-After.
7. Stop Redis. Require HTTP503 `rate_limiter_unavailable` and no saved SQL outcome
   for that request key. Restart Redis.
8. Send SIGTERM to Ledger, both Gateways, Activity and publisher; require exit0
   within the bounded Docker stop. Restore the stack and run both audits again.

The consumer deliberately stops after a persistent fetch/SQL/offset failure. The
Kafka recovery step therefore explicitly starts Activity as well as publisher
after broker readiness; it is an operator recovery action recorded in the report.
Offsets are retained, and replay protects the SQL effects. There is no claim that
an exited process restarts itself. Each CLI phase has a2-minute default deadline,
adjustable with `--timeout` within1s..10m.

Stable fixture funding keys prevent repeated demonstrations from funding the same
wallets again. Each full run uses a new run ID for the outage transfers and limiter
operation, so balances and history intentionally accumulate those real operations.
The stable normal transfer and reversal retain their IDs. Activity summaries count
incoming/outgoing operations including reversal postings; they are not wallet balances.

Reports are written to `.runtime/events-demo/<run-id>/report.json`, alongside raw
step output and source provenance (HEAD, dirty status and a SHA256 content manifest).
They are private runtime files and are ignored by Git. Failure logs are captured
before restoration; the final report records whether services were restored. A
failed run is not accepted as successful because cleanup succeeded.

For individual steps, run the CLI through the tools profile:

```sh
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase normal
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase status
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase verify
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase replay
```

`--no-deps` is essential during an intentional outage so Compose does not restart
the stopped dependency. The full script supplies both Gateway URLs and effective
limiter settings. `enqueue` and limiter checks accept `--run-id` and `--count`;
prefer the script for their coordinated assertions.

Run the crash-window, rebalance, DLQ and temporary SQL-failure tests with
`make test-integration`. These use separate test databases/topics and test-only
subprocess barriers. See [failure scenarios](failure-scenarios.md),
[runbook](runbook.md) and [acceptance](events/acceptance.md) for performed checks.

`make down` preserves volumes. `make destroy-data` is a separate explicit destructive
command and is unnecessary for repeatable demonstrations.
