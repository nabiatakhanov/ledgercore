# ADR004: Snapshot audit and account-local history cursors

Status: accepted.

Reconciliation opens a REPEATABLE READ READ ONLY transaction. Comparing a journal aggregate and a projection using separate READ COMMITTED snapshots would create false mismatches under writes. Audit SQL is independent of the writer's Go transition and uses NUMERIC sums.

History uses account-local sequences assigned under the account row lock, plus an upper bound captured on the first page. A global sequence or `(created_at,id)` can be allocated before commit and then appear behind a cursor after another transaction commits. Per-account lock/sequence coupling avoids that ordering hole. Cursor pages are a stable bounded prefix, not an indefinitely live stream. Balance GET and history GET are individually committed reads, not a joint snapshot. An external caller needing a coherent full audit uses the CLI audit.
