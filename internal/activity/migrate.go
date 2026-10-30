package activity

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 001_activity.sql
var schema string

// Migrate applies Activity's schema using activity_migrator credentials. It never
// opens the LedgerCore database. Line endings are canonicalized before hashing.
func Migrate(ctx context.Context, p *pgxpool.Pool) error {
	tx, err := p.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(734255201)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS activity_schema_migrations(version integer PRIMARY KEY,checksum text NOT NULL)`); err != nil {
		return err
	}
	canonical := strings.ReplaceAll(schema, "\r\n", "\n")
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(canonical)))
	var saved string
	err = tx.QueryRow(ctx, `SELECT checksum FROM activity_schema_migrations WHERE version=1`).Scan(&saved)
	if err == nil {
		if saved != digest {
			return fmt.Errorf("activity schema checksum mismatch")
		}
		return tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return err
	}
	if _, err = tx.Exec(ctx, canonical); err != nil {
		return fmt.Errorf("activity migration: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO activity_schema_migrations(version,checksum) VALUES(1,$1)`, digest); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
