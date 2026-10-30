# ADR 003: key by operation; accept projection reordering

Status: accepted. Date: 2026-10-30.

## Context

A transfer has two accounts, so choosing one account key cannot establish a total order for both. A global single-partition order would constrain parallelism. Publisher scheduling also need not follow SQL commit order.

## Decision

Use `journal_transaction_id` as key of `ledger.operations.v1`, with three local partitions. Promise order only within a partition. Process each assigned partition sequentially, with bounded parallel handlers and ownership held until successful prefixes commit.

Every transfer/reversal independently contributes two immutable activity entries and commutative incoming/outgoing totals/counts. A reversal links to its original ID without requiring prior arrival. Activity account sequence records projection application order and supports stable bounded cursors; it does not represent global financial commit order.

## Consequences

Late reversals, independent-event permutations and group rebalances retain the same totals/ID sets. Funds decisions use Ledger. New pagination can show late arrivals; an existing cursor retains its initial upper bound. Partition/key changes need a separate ordering/replay review. The dev RF=1 broker provides no node/disk redundancy.
