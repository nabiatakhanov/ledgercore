# Reproducible local experiment

Archived baseline HTTP/PostgreSQL measurement at base SHA
`4a4ebd17158341f4740c31377a6d15951bc8e46f`. It does not measure the new
Gateway/gRPC/Redis path; see [current measurements](benchmarks.md).

Measured on **2026-10-29** (Europe/Moscow). This is one short local run, not a
production capacity claim. Raw evidence is in [benchmark-results.json](benchmark-results.json),
[benchmark-resources.json](benchmark-resources.json), [explain.txt](explain.txt),
and [explain-analyzed.txt](explain-analyzed.txt).

## Conditions and commands

- Windows amd64, AMD Ryzen 7 5800HS, 16 logical processors, 16,539,389,952 bytes
  physical RAM; other desktop processes were not controlled.
- Go 1.26.2; ordinary `go build -trimpath`, with no race instrumentation.
- PostgreSQL 17.9, native Windows/MSVC build; same host as API and load generator.
  `shared_buffers=128MB`, `max_connections=100`, `fsync=on`,
  `synchronous_commit=on`, `full_page_writes=on`, `wal_level=replica`.
- API pool: 16 connections; CLI pool: 4 connections. API at loopback port 18080,
  metrics at 19090, database at 55432. A different demonstration API remained
  idle on the same PostgreSQL instance. This was not a dedicated benchmark host.
- Fresh isolated database `ledgercore_bench_test`, initially just the migration's
  system account. Each case uses fresh wallets and unique idempotency keys.
  Both wallets in each pair receive 1,025 test kopecks. Each transfer sends one
  kopeck. Every case performs exactly 1,024 requests.
- Cases run in the listed order. Connections and caches therefore warm over the
  run. Cases at 8/32 independent clients last less than one second; repeat larger
  samples before treating small differences as stable measurements.

Native reproduction after preparing separate owner/app/fixture roles and setting
the corresponding URLs to an **isolated benchmark database**:

```powershell
go build -trimpath -o .runtime/api.exe ./cmd/api
go build -trimpath -o .runtime/ledgerctl.exe ./cmd/ledgerctl
.runtime/ledgerctl.exe migrate
$env:HTTP_ADDR='127.0.0.1:18080'
$env:METRICS_ADDR='127.0.0.1:19090'
$env:DB_MAX_CONNS='16'
$env:API_URL='http://127.0.0.1:18080'
$env:METRICS_URL='http://127.0.0.1:19090/metrics'
# Terminal 1, with the environment above:
.runtime/api.exe
# Terminal 2, with the same environment:
.runtime/ledgerctl.exe bench -n 1024 > docs/benchmark-results.json
.runtime/ledgerctl.exe explain > docs/explain.txt
.runtime/ledgerctl.exe reconcile
```

In the extended checkout, `make up` followed by `make bench-legacy` runs the
retained six-case generator through the private compatibility HTTP adapter;
`make bench` now measures the new Gateway path. Use a separate explicitly isolated
Compose project/volume for a fresh dataset; neither command deletes existing
data. The native run above is the measurement reported here. Its Docker equivalent
was **NOT RUN at the time of this baseline measurement** because Docker Desktop
failed to start (see [environment-notes.md](environment-notes.md)).

## Measured HTTP results

Latency uses nearest-rank p50/p95/p99 across all completed requests in the case.
Throughput is completed requests divided by elapsed wall time of the worker loop;
fixture creation, metrics scraping and post-case reconciliation are outside that
interval. Each row has **1,024 successes, 0 business refusals and 0 infrastructure
errors**. All six independent snapshot reconciliations passed.

| Scenario | Clients | Requests/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| One hot pair | 1 | 242.92 | 3.82 | 5.39 | 7.26 |
| One hot pair | 8 | 285.21 | 11.02 | 108.82 | 361.32 |
| One hot pair | 32 | 344.27 | 86.10 | 113.40 | 298.59 |
| Independent pairs | 1 | 264.77 | 3.58 | 4.47 | 4.96 |
| Independent pairs | 8 | 1,616.64 | 4.87 | 5.83 | 6.64 |
| Independent pairs | 32 | 2,247.54 | 13.68 | 16.98 | 20.96 |

The hot-pair results are consistent with the intended serialization of updates to
the same account rows. Independent pairs permit more parallel progress. The 32
clients share a 16-connection API pool. This experiment alone does not isolate
every source of latency; no schema or locking strategy change was made from it.

## CPU and memory

Per-case API process counters are read from its Prometheus endpoint immediately
before and after the worker loop. CPU is the cumulative process CPU difference;
RSS is the end-of-case resident set, **not a peak**. Values below use 1 MiB =
1,048,576 bytes. Raw before/after counters are preserved in the results JSON.

| Scenario | Clients | API CPU seconds | API end RSS MiB |
|---|---:|---:|---:|
| One hot pair | 1 | 1.875000 | 64.05 |
| One hot pair | 8 | 1.953125 | 65.80 |
| One hot pair | 32 | 2.187500 | 68.61 |
| Independent pairs | 1 | 1.671875 | 67.19 |
| Independent pairs | 8 | 1.937500 | 67.71 |
| Independent pairs | 32 | 2.062500 | 70.06 |

A concurrent Windows `Get-Process` sampler targeted 100 ms intervals and recorded
135 samples over **16.7752444 s**, including fixture setup and between-case work.
Overall API CPU delta was **11.71875 s**. Its maximum sampled working set was
**74,219,520 bytes** (also the OS process peak reported at the end); maximum sampled
private bytes were **64,823,296**.

PostgreSQL's observed aggregate CPU delta was **20.671875 s**; maximum sampled
aggregate working set was **917,438,464 bytes**, and maximum sampled aggregate
private bytes were **221,536,256**. These cover the entire shared native instance.
The sampler tracks maximum observed CPU per PID/start time, subtracting initial
baselines; short-lived processes and CPU between a backend's last sample and its
exit can be missed. Aggregate working sets can count shared pages more than once
and do not represent unique physical RAM. All samples and method details are
preserved in `benchmark-resources.json`; no missing counters were fabricated.

The counter capture is reproducible with `scripts/benchmark-windows.ps1`. Start the
API first, obtain its process ID, then replace the direct `ledgerctl bench`
command with the following (the script runs the load generator itself):

```powershell
pwsh -File scripts/benchmark-windows.ps1 -ApiProcessId 12345 -LedgerctlPath .runtime/ledgerctl.exe -Requests 1024 -OutputDirectory .runtime/benchmark
```

Replace `12345` with the actual API process ID. Output defaults to a separate
`.runtime/benchmark` directory, so recorded evidence in `docs` is preserved.
Preserve host/server configuration
and before/after dataset counts alongside new results; the reusable sampler
collects process counters and HTTP results, while the recorded experiment also
contains SQL environment facts.

## Dataset and final audit

The final database contained **89 accounts**, **6,232 journal transactions**,
**12,464 postings**, and **6,232 idempotency records**. There are 6,144 ordinary
transfers plus 88 fixture funding transactions. PostgreSQL reported total database
size **20,330,163 bytes** at collection time, including catalog/index overhead.
Final reconciliation passed with no issues. Test money stayed in the balanced
journal; funding was not implemented as projection-only balance updates.

## Query plans and statistics maintenance

`ledgerctl explain` records `EXPLAIN (ANALYZE, BUFFERS)` for the ordered lock query,
history keyset query and independent journal aggregation.

The initial history plan estimated 22 rows but actually visited 1,025. It used the
existing composite unique index through a bitmap scan followed by a top-N sort:
33 shared buffer hits and observed execution time 0.811 ms. After explicit owner
maintenance on this isolated database:

```sql
ANALYZE accounts;
ANALYZE postings;
ANALYZE journal_transactions;
```

the history plan used **Index Scan Backward** on
`postings_account_id_account_sequence_key`, returned 51 rows with four shared
buffer hits, and observed execution time 0.094 ms. Both plans are retained. These
are individual plan executions, not a statistical latency improvement claim.
The six load results above were collected **before** this maintenance and were
not replaced or rerun.

The lock query sorted the two selected rows before `LockRows`, preserving the
chosen ID order. Journal reconciliation used a sequential scan and hash aggregate
over all 12,464 postings (observed 6.094 ms before / 6.172 ms after ANALYZE), as
expected for a full independent audit. No additional index or schema optimization
was justified by this small run.
