# ADR003: Ordered account locks and transactional idempotency

Status: accepted.

Use READ COMMITTED with pessimistic row locks ordered by UUID for transfers, funding and reversals. This serializes only operations sharing accounts and makes the insufficient-funds decision against the latest locked balance. SERIALIZABLE would require more retries without improving this fixed two-account invariant. The same order for opposite transfers avoids a routine deadlock cycle.

Reserve `(kind,key)` before taking account locks. The unique index is the arbiter, including concurrent requests. Store status and semantic response in that same short transaction. Repeated terminal business refusals remain refusals after funding changes. Different normalized parameters get409. Parse and infrastructure errors are not committed outcomes. The fingerprint includes every transfer field, metadata and original reversal ID. Amount/UUID/currency spelling differences normalize before hashing.

Retry only40001/40P01, with three whole-transaction attempts, using the unchanged key and bounded context-aware backoff. Concurrent reservations wait within DB/request deadlines; failure yields temporary503 with Retry-After1. A connection loss during commit has an unknown outcome. Clients must repeat the same operation/key rather than create a replacement key. Account creation is not idempotent and clients must not assume this protection for it.
