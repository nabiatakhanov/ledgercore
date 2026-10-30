# Versioned private RPC contracts

`ledger.v1` preserves the original six HTTP application operations; `activity.v1`
exposes an eventually consistent page and cumulative statistics. Proto operation
amounts are positive int64 RUB kopecks; signed deltas/balances are int64. HTTP
continues to encode money as decimal strings. Activity cumulative sums and counts
are unbounded NUMERIC decimal strings, never floating point or available balance.

Generate from the repository root with `sh scripts/generate.sh` (Linux x86_64,
requires curl/unzip) or `powershell -File scripts/generate.ps1` (Windows x64).
Pinned protoc 36.2, protoc-gen-go v1.36.12, protoc-gen-go-grpc v1.6.2; generated Go
is checked in under `gen/`. Official protoc release archives have pinned SHA256
checksums in both scripts. Use additive fields; reserve removed field names and
numbers. Never reuse numbers. No generated files are written by hand.

Private plaintext gRPC is acceptable only inside the local Docker network.
Application calls require `INTERNAL_RPC_TOKEN`, separate from external `API_TOKEN`.
Ports 50051/50052 must not be published as unauthenticated Gateway bypasses.
Standard unary `grpc.health.v1.Health/Check` is unauthenticated on that private
network, evaluates the component dependency with a one-second timeout; streaming
Watch is not provided. RPC messages are capped at 1 MiB both directions.
Each connection permits at most 64 concurrent streams, and a service-wide
64-call application admission budget rejects excess work with RESOURCE_EXHAUSTED
(HTTP 503). The ledger's bounded DB pool remains the final SQL concurrency limit.

Interceptors validate/propagate a UUID x-request-id, cap application contexts at
four seconds, authenticate internal calls, and record only fixed methods, codes
and timing. Request IDs, keys, payloads, tokens and raw SQL errors are never metric
labels. Interceptors do not log message bodies or raw error text.

| Domain code | gRPC status | External HTTP |
| --- | --- | --- |
| invalid_request | INVALID_ARGUMENT + ErrorInfo | 400 |
| protected_account | PERMISSION_DENIED + ErrorInfo | 403 |
| not_found | NOT_FOUND + ErrorInfo | 404 |
| idempotency_conflict, already_reversed | ALREADY_EXISTS + ErrorInfo | 409 |
| insufficient_funds, balance_overflow, sequence_overflow, not_reversible | FAILED_PRECONDITION + ErrorInfo | 409 |
| connection/resource/lock temporary failure | UNAVAILABLE, sanitized | 503 |
| context deadline/cancel | DEADLINE_EXCEEDED / CANCELLED, sanitized | 503 or disconnected client |
| unclassified internal error | INTERNAL, sanitized | 503 |

ErrorInfo preserves the bounded domain code, HTTP status and replay flag. A
successful OperationResponse also preserves its replay flag. Gateway retries
only UNAVAILABLE once (at most two calls) within one four-second overall deadline,
with the same immutable request/key and request ID. Reads may retry; unkeyed
CreateAccount never retries its application call. gRPC service-config retries
are disabled. Permanent internal/SQL constraint failures are not retried by the
Gateway. The original writer retains its own bounded whole-transaction retry.

DEADLINE_EXCEEDED, disconnection or a 503 does not establish whether commit
succeeded. Repeat the same key/parameters or read a known operation ID. A new key
would ask for a new transfer. Activity being unavailable does not undo a committed
ledger operation; an empty Activity page does not prove an account is absent.
