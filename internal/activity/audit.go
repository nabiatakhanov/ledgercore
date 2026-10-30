package activity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Issue struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Detail string `json:"detail"`
}
type Report struct {
	OK         bool    `json:"ok"`
	Events     int64   `json:"events"`
	Entries    int64   `json:"entries"`
	Accounts   int64   `json:"accounts"`
	IssueCount int64   `json:"issue_count"`
	Issues     []Issue `json:"issues"`
}

func (r *Report) add(kind, id, detail string) {
	r.IssueCount++
	if len(r.Issues) < 100 {
		r.Issues = append(r.Issues, Issue{kind, id, detail})
	}
}

// Audit independently checks the Activity database in one consistent read-only
// snapshot. It neither reads LedgerCore tables nor repairs the projection.
func Audit(ctx context.Context, p *pgxpool.Pool) (Report, error) {
	r := Report{Issues: []Issue{}}
	tx, err := p.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return r, err
	}
	defer rollback(tx)
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM activity_inbox),(SELECT count(*) FROM activity_entries),(SELECT count(*) FROM activity_summaries)`).Scan(&r.Events, &r.Entries, &r.Accounts); err != nil {
		return r, err
	}
	rows, err := tx.Query(ctx, `SELECT i.event_id::text FROM activity_inbox i LEFT JOIN activity_entries p ON p.event_id=i.event_id GROUP BY i.event_id HAVING count(p.event_id)<>2 OR count(*) FILTER(WHERE p.amount_minor=i.amount_minor AND ((p.account_id=i.from_account_id AND p.direction='outgoing') OR(p.account_id=i.to_account_id AND p.direction='incoming')))<>2 OR i.schema_version<>1 OR i.currency<>'RUB' OR i.amount_minor<=0 OR i.event_id<>i.journal_transaction_id OR i.from_account_id=i.to_account_id OR i.type NOT IN('transfer.created','transfer.reversed') OR ((i.type='transfer.reversed')<>(i.reversal_of IS NOT NULL)) OR i.reversal_of=i.event_id`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return r, err
		}
		r.add("event_shape", id, "event requires two exact activity entries and a supported event shape")
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	rows, err = tx.Query(ctx, `WITH aggregated AS(SELECT account_id,sum(CASE WHEN direction='incoming' THEN amount_minor::numeric ELSE 0 END) incoming_minor,sum(CASE WHEN direction='outgoing' THEN amount_minor::numeric ELSE 0 END) outgoing_minor,count(*) FILTER(WHERE direction='incoming')::numeric incoming_count,count(*) FILTER(WHERE direction='outgoing')::numeric outgoing_count,count(*) n,min(account_sequence) first_sequence,max(account_sequence) last_sequence FROM activity_entries GROUP BY account_id) SELECT COALESCE(s.account_id,a.account_id)::text,COALESCE(s.incoming_minor::text,'missing'),COALESCE(a.incoming_minor,0)::text FROM activity_summaries s FULL JOIN aggregated a ON a.account_id=s.account_id WHERE s.account_id IS NULL OR s.incoming_minor<>COALESCE(a.incoming_minor,0) OR s.outgoing_minor<>COALESCE(a.outgoing_minor,0) OR s.incoming_count<>COALESCE(a.incoming_count,0) OR s.outgoing_count<>COALESCE(a.outgoing_count,0) OR s.entry_sequence<>COALESCE(a.n,0) OR s.entry_sequence<>COALESCE(a.last_sequence,0) OR COALESCE(a.first_sequence,1)<1`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var id, actual, expected string
		if err = rows.Scan(&id, &actual, &expected); err != nil {
			rows.Close()
			return r, err
		}
		r.add("summary_mismatch", id, fmt.Sprintf("summary/sequence differs from entries; incoming actual=%s expected=%s", actual, expected))
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	rows, err = tx.Query(ctx, `SELECT p.event_id::text FROM activity_entries p LEFT JOIN activity_inbox i ON i.event_id=p.event_id WHERE i.event_id IS NULL OR p.direction NOT IN('incoming','outgoing') OR p.amount_minor<=0`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return r, err
		}
		r.add("orphan_or_invalid_entry", id, "entry has no event or invalid direction/amount")
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	if err = tx.Commit(ctx); err != nil {
		return r, err
	}
	r.OK = r.IssueCount == 0
	return r, nil
}
