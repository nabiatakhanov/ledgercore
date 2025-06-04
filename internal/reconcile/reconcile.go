// Package reconcile audits the ledger independently of the transaction writer.
package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Issue describes an invariant violation. IDs are diagnostic output, never labels.
type Issue struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Detail string `json:"detail"`
}

// Report contains bounded examples and the total number of issues discovered.
type Report struct {
	OK           bool    `json:"ok"`
	Accounts     int64   `json:"accounts"`
	Transactions int64   `json:"transactions"`
	IssueCount   int64   `json:"issue_count"`
	Issues       []Issue `json:"issues"`
}

func (r *Report) add(kind, id, detail string) {
	r.IssueCount++
	if len(r.Issues) < 100 {
		r.Issues = append(r.Issues, Issue{Kind: kind, ID: id, Detail: detail})
	}
}

// Run compares journal aggregates with projections in one consistent snapshot.
// All money aggregation happens in PostgreSQL NUMERIC: even an invalid journal
// whose total exceeds BIGINT is reportable without wraparound or SUM overflow.
func Run(ctx context.Context, pool *pgxpool.Pool) (Report, error) {
	report := Report{Issues: []Issue{}}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return report, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM accounts),
		(SELECT count(*) FROM journal_transactions)`).Scan(&report.Accounts, &report.Transactions); err != nil {
		return report, err
	}
	rows, err := tx.Query(ctx, `
		WITH aggregate_postings AS (
			SELECT account_id, sum(delta_minor::numeric) AS balance,
			       count(*) AS n, max(account_sequence) AS last_sequence
			FROM postings GROUP BY account_id
		)
		SELECT a.id::text, a.balance_minor::text, COALESCE(p.balance,0)::text,
		       a.balance_minor < 0 AND a.kind='client',
		       a.entry_sequence <> COALESCE(p.n,0)
		       OR a.entry_sequence <> COALESCE(p.last_sequence,0),
		       a.currency <> 'RUB' OR a.kind NOT IN ('system','client')
		       OR (a.kind='system' AND a.id<>'00000000-0000-0000-0000-000000000001')
		FROM accounts a LEFT JOIN aggregate_postings p ON p.account_id=a.id
		ORDER BY a.id`)
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var id, projection, journal string
		var negative, sequence, shape bool
		if err = rows.Scan(&id, &projection, &journal, &negative, &sequence, &shape); err != nil {
			rows.Close()
			return report, err
		}
		if projection != journal {
			report.add("balance_mismatch", id, fmt.Sprintf("projection=%s journal=%s", projection, journal))
		}
		if negative {
			report.add("negative_client", id, "client balance is below zero")
		}
		if sequence {
			report.add("sequence_mismatch", id, "projection sequence differs from contiguous posting sequence")
		}
		if shape {
			report.add("account_shape", id, "invalid account kind, currency or system identity")
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, err
	}
	rows, err = tx.Query(ctx, `
		WITH posted AS (
			SELECT t.id, count(p.transaction_id) AS n,
			       COALESCE(sum(p.delta_minor::numeric),0) AS total,
			       count(*) FILTER (WHERE
			           (p.account_id=t.from_account_id AND p.delta_minor::numeric=-t.amount_minor::numeric)
			           OR (p.account_id=t.to_account_id AND p.delta_minor=t.amount_minor)) AS expected
			FROM journal_transactions t LEFT JOIN postings p ON p.transaction_id=t.id GROUP BY t.id
		)
		SELECT t.id::text,
		       p.n <> 2 OR p.total <> 0 OR p.expected <> 2
		       OR t.amount_minor<=0 OR t.from_account_id=t.to_account_id OR t.currency<>'RUB',
		       CASE WHEN t.kind='funding' THEN
		           COALESCE(src.kind,'')<>'system' OR COALESCE(dst.kind,'')<>'client' OR t.reversal_of IS NOT NULL
		       WHEN t.kind IN ('transfer','reversal') THEN
		           COALESCE(src.kind,'')<>'client' OR COALESCE(dst.kind,'')<>'client'
		       ELSE true END,
		       CASE WHEN t.kind='reversal' THEN
		           original.id IS NULL OR original.kind<>'transfer'
		           OR original.from_account_id<>t.to_account_id
		           OR original.to_account_id<>t.from_account_id
		           OR original.amount_minor<>t.amount_minor OR original.currency<>t.currency
		       ELSE t.reversal_of IS NOT NULL END
		FROM journal_transactions t JOIN posted p ON p.id=t.id
		LEFT JOIN accounts src ON src.id=t.from_account_id
		LEFT JOIN accounts dst ON dst.id=t.to_account_id
		LEFT JOIN journal_transactions original ON original.id=t.reversal_of
		ORDER BY t.id`)
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var id string
		var shape, accounts, reversal bool
		if err = rows.Scan(&id, &shape, &accounts, &reversal); err != nil {
			rows.Close()
			return report, err
		}
		if shape {
			report.add("transaction_shape", id, "expected exactly two balanced postings matching amount and accounts")
		}
		if accounts {
			report.add("transaction_accounts", id, "invalid account kinds or transaction kind")
		}
		if reversal {
			report.add("invalid_reversal", id, "reversal must fully reverse an existing ordinary transfer")
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, err
	}
	// These checks remain independent even if a privileged test disables constraints.
	rows, err = tx.Query(ctx, `
		SELECT 'duplicate_reversal', reversal_of::text, 'more than one reversal'
		FROM journal_transactions WHERE reversal_of IS NOT NULL GROUP BY reversal_of HAVING count(*)>1
		UNION ALL
		SELECT 'orphan_posting', p.transaction_id::text, 'posting references absent account or transaction'
		FROM postings p LEFT JOIN accounts a ON a.id=p.account_id
		LEFT JOIN journal_transactions t ON t.id=p.transaction_id WHERE a.id IS NULL OR t.id IS NULL
		UNION ALL
		SELECT 'incomplete_idempotency', kind, 'reservation has no final status or response'
		FROM idempotency_records WHERE status IS NULL OR response IS NULL`)
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var kind, id, detail string
		if err = rows.Scan(&kind, &id, &detail); err != nil {
			rows.Close()
			return report, err
		}
		report.add(kind, id, detail)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, err
	}
	if err = tx.Commit(ctx); err != nil {
		return report, err
	}
	report.OK = report.IssueCount == 0
	return report, nil
}
