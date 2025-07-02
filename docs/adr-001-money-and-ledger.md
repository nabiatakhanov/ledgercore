# ADR001: Integral minor units and a normalized signed journal

Status: accepted.

Use BIGINT/checked Go int64 for individual kopeck amounts and decimal strings at the HTTP boundary. A transfer is exactly two signed deltas. This avoids floating point rounding and permits local direction checks. Positive delta means an increase of the internal wallet balance; the relation between debit/credit and increase depends on the accounting account type. Client wallet liabilities increase on credit.

Initial funds debit a protected balancing system account and credit the client. It is the only account allowed negative; it is bounded by int64 like every other individual balance. Funding is not an UPDATE shortcut. A full reversal creates a new opposite operation with one unique original reference. No partial reversals, holds, exchange or bank integration are included.

Aggregate reconciliation uses PostgreSQL NUMERIC, since adding many valid int64 postings can exceed int64 even when the final balance fits. Overflow of either new balance is a saved business refusal.
