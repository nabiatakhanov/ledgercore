# Local execution evidence

These files preserve the selected original evidence bytes from local runs on
2026-10-30. JSON was copied without reformatting or changing numbers. SHA-256
digests of all copied artifacts are in [SHA256SUMS](SHA256SUMS). The original
reports retain their local log paths; the portable mapping below identifies the
files included here. No environment/configuration dump was copied. Selected
content, credential patterns and JSON keys were inspected before copying; no
credentials were found in these artifacts.

Source commits:

- Both demos: `0400fcff1ed0a5fd56409572660b47cd9c3f5679` (clean working tree).
- Benchmark/dataset captures and final integration/vet/Staticcheck:
  `05ecfef624e3ae36d200efd402fac5fa1dd78875` (benchmark source clean).
- The Linux proto reproduction used input `0400fcf`; contracts/generated files
  were unchanged in `05ecfef`, as stated in the execution receipt.

Source-directory aliases:

| Alias | Original repository-relative directory |
| --- | --- |
| D1 | `.runtime/events-demo/37d7ec83-3bb3-4d49-8a76-b11d50318e33/` |
| D2 | `.runtime/events-demo/3e49a6f8-a35b-4d37-a411-130efaf940a2/` |
| B | `.runtime/events-bench/f48768cb-e751-4377-87e0-279e6fc2edbc/` |
| F | `.runtime/events-final-checks/` |

| Included file | Original source | Contents |
| --- | --- | --- |
| [demo-1.json](demo-1.json) | D1 `report.json` | First completed fault demo, phase results/exit codes and timings |
| [demo-2.json](demo-2.json) | D2 `report.json` | Repeated completed demo; stable fixtures/normal operation IDs |
| [benchmark-report.json](benchmark-report.json) | B `report.json` | Orchestration, source/content hashes, resource limits, phases and benchmark payload |
| [benchmark-raw.json](benchmark-raw.json) | B `08-events-bench.stdout.log` | Original JSON stdout, including individual HTTP and commit timestamps |
| [sample-audit.json](sample-audit.json) | `.runtime/events-bench/independent-sample-audit.json` | Independent recalculation of all case percentiles, IDs and timestamp deltas |
| [dataset-before.json](dataset-before.json) | `.runtime/events-dataset/before-benchmark.json` | Pre-benchmark database counts/sizes and stable demo IDs |
| [dataset-after.json](dataset-after.json) | `.runtime/events-dataset/after-benchmark.json` | Post-benchmark counts/sizes and stable demo IDs |
| [stats-before.log](stats-before.log) | B `07-stats-before.stdout.log` | Docker stats JSON lines before load |
| [stats-after.log](stats-after.log) | B `10-stats-after.stdout.log` | Docker stats JSON lines after load |
| [integration.log](integration.log) | F `integration.stdout.log` | Actual Compose build/test stdout, full real-infrastructure race suite |
| [integration-stderr.log](integration-stderr.log) | F `integration.stderr.log` | Compose dependency startup stderr from that execution |
| [proto-linux.log](proto-linux.log) | F `proto-linux.stdout.log` | Pinned generator versions and generated-file SHA-256 output |
| [checks-receipt.json](checks-receipt.json) | F `checks-receipt.json` | Manually assembled receipt of exit codes observed in completed execution-tool results |

The receipt is execution metadata, not raw process stdout. Empty vet/Staticcheck
stdout/stderr files were not used to infer success and are not included; the
observed exit codes and exact commands are recorded in the receipt. No hosted CI
execution is established by these local artifacts.

## Raw fields and recalculation

`benchmark-raw.json` has nine `cases`: 1, 8 and 32 clients, three independent
repetitions each, 256 requests per case. Each case retains:

- `http_samples`: request index, HTTP status, transaction ID and `latency_ms`.
- `commit_to_projection_samples`: event ID, `source_committed_at`,
  `projection_committed_at` and `latency_ms` from PostgreSQL commit timestamps.
- `seconds`, `requests_per_second`, success/refusal/limiter/infrastructure counts,
  reported distributions and independent audit status.

For any distribution, sort the individual latency values ascending. For p50,
p95 or p99, select zero-based index `ceil(n * p) - 1`; maximum is the final
element. Recalculate throughput as `requests / seconds`. Recalculate event delay
as `(projection_committed_at - source_committed_at)` in milliseconds, matching by
event/transaction ID. Commit timestamp strings should be parsed as timestamps,
not compared lexicographically. Both databases share the same PostgreSQL host
clock with `track_commit_timestamp=on`.

The independent sample audit checks 2304 HTTP samples and 2304 projection
samples, unique IDs, HTTP/commit ID-set equality, every case percentile and
timestamp delta. Its `client_summaries` distinguish median case throughput,
pooled sample percentiles and median case p95; these are different aggregations.
Normal binary floating-point rounding can affect the final sub-microsecond
comparison digits; raw values remain unchanged.

## Limits of the evidence

This is one short local experiment on a shared Docker Desktop host. Before/after
Docker stats are samples, not peak CPU/RAM or a continuous resource profile.
HTTP throughput excludes setup and post-case catch-up; projection delay is
reported separately. Benchmark write quota was temporarily raised to 1000000
per minute to measure successful commands; limiter behavior is demonstrated by
the separate default-quota demos. Error and limiter counts remain explicit.

The data does not establish a speed improvement over the old HTTP/PostgreSQL
path, production capacity, an SLA, or Kafka HA at replication factor 1. The raw
reports, source hashes, dataset snapshots and final audits bound the claims.
Interpretation and complete run conditions are in [benchmarks](../../benchmarks.md)
and [extension acceptance](../acceptance.md).
