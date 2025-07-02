// ledgerctl is the explicit administrative and experimental ledger companion.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/ledger"
	"ledgercore/internal/reconcile"
	"ledgercore/internal/store"
	"ledgercore/migrations"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: ledgerctl migrate | fixture --allow-test-funds | demo | reconcile | bench | explain")
	}
	switch args[0] {
	case "migrate":
		p, err := open(ctx, "MIGRATION_DATABASE_URL")
		if err != nil {
			return err
		}
		defer p.Close()
		return migrations.Apply(ctx, p)
	case "fixture":
		f := flag.NewFlagSet("fixture", flag.ContinueOnError)
		allow := f.Bool("allow-test-funds", false, "explicitly authorize balanced test funding")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if !*allow {
			return errors.New("fixture requires --allow-test-funds and FIXTURE_DATABASE_URL")
		}
		p, err := open(ctx, "FIXTURE_DATABASE_URL")
		if err != nil {
			return err
		}
		defer p.Close()
		ids, err := fixture(ctx, p, "demo-v1", 100000)
		if err != nil {
			return err
		}
		return output(ids)
	case "demo":
		return demo(ctx)
	case "reconcile":
		p, err := open(ctx, "DATABASE_URL")
		if err != nil {
			return err
		}
		defer p.Close()
		return audit(ctx, p)
	case "bench":
		return bench(ctx, args[1:])
	case "explain":
		p, err := open(ctx, "DATABASE_URL")
		if err != nil {
			return err
		}
		defer p.Close()
		return explain(ctx, p)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func open(ctx context.Context, key string) (*pgxpool.Pool, error) {
	dsn := os.Getenv(key)
	if dsn == "" {
		return nil, fmt.Errorf("%s is required", key)
	}
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid %s", key)
	}
	c.MaxConns = 4
	c.ConnConfig.ConnectTimeout = 5 * time.Second
	c.ConnConfig.RuntimeParams["statement_timeout"] = "60000"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, errors.New("open database pool failed")
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, errors.New("database connection failed")
	}
	return p, nil
}
func output(v any) error { e := json.NewEncoder(os.Stdout); e.SetIndent("", "  "); return e.Encode(v) }
func audit(ctx context.Context, p *pgxpool.Pool) error {
	r, err := reconcile.Run(ctx, p)
	if err != nil {
		return err
	}
	if err = output(r); err != nil {
		return err
	}
	if !r.OK {
		return errors.New("reconciliation found invariant violations")
	}
	return nil
}
func stableID(s string) string {
	h := sha256.Sum256([]byte(s))
	h[6] = (h[6] & 15) | 64
	h[8] = (h[8] & 63) | 128
	b := hex.EncodeToString(h[:16])
	return b[:8] + "-" + b[8:12] + "-" + b[12:16] + "-" + b[16:20] + "-" + b[20:]
}
func fixture(ctx context.Context, p *pgxpool.Pool, namespace string, amount ledger.Money) ([]string, error) {
	s := store.New(p, nil)
	ids := []string{stableID(namespace + "/alice"), stableID(namespace + "/bob")}
	for i, id := range ids {
		if _, err := p.Exec(ctx, `INSERT INTO accounts(id,name) VALUES($1,$2) ON CONFLICT(id) DO NOTHING`, id, fmt.Sprintf("%s-wallet-%d", namespace, i)); err != nil {
			return nil, err
		}
		o, err := s.Funding(ctx, namespace+fmt.Sprintf("/fund/%d", i), id, amount)
		if err != nil {
			return nil, err
		}
		if o.Error != nil {
			return nil, o.Error
		}
	}
	return ids, nil
}

type client struct {
	base, token string
	http        *http.Client
}

func newClient() (*client, error) {
	token := os.Getenv("API_TOKEN")
	if len(token) < 16 {
		return nil, errors.New("API_TOKEN at least16 bytes is required")
	}
	base := os.Getenv("API_URL")
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	return &client{base: strings.TrimRight(base, "/"), token: token, http: &http.Client{Timeout: 7 * time.Second}}, nil
}
func (c *client) call(ctx context.Context, method, path, key string, body any) (int, []byte, error) {
	var b bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&b).Encode(body); err != nil {
			return 0, nil, err
		}
	}
	r, err := http.NewRequestWithContext(ctx, method, c.base+path, &b)
	if err != nil {
		return 0, nil, err
	}
	r.Header.Set("Authorization", "Bearer "+c.token)
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(r)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65536))
	return resp.StatusCode, data, err
}
func demo(ctx context.Context) error {
	p, err := open(ctx, "FIXTURE_DATABASE_URL")
	if err != nil {
		return err
	}
	defer p.Close()
	ids, err := fixture(ctx, p, "demo-v1", 100000)
	if err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	request := ledger.TransferRequest{FromAccountID: ids[0], ToAccountID: ids[1], AmountMinor: "2500", Currency: "RUB", Metadata: map[string]string{"purpose": "demo"}}
	status, b, err := c.call(ctx, "POST", "/v1/transfers", "demo-v1/transfer", request)
	if err != nil {
		return err
	}
	if status != 201 {
		return fmt.Errorf("demo transfer: status %d %s", status, b)
	}
	var out ledger.Outcome
	if err = json.Unmarshal(b, &out); err != nil {
		return err
	}
	if out.Transaction == nil {
		return errors.New("missing demo transaction")
	}
	fmt.Println("Transfer and reversal (stable keys; reruns replay):")
	if err = output(out); err != nil {
		return err
	}
	status, b, err = c.call(ctx, "POST", "/v1/transfers/"+out.Transaction.ID+"/reversal", "demo-v1/reversal", nil)
	if err != nil {
		return err
	}
	if status != 201 {
		return fmt.Errorf("demo reversal: status %d %s", status, b)
	}
	fmt.Println(string(b))
	request.AmountMinor = "100"
	request.Metadata = nil
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	txIDs := make(chan string, 16)
	for range 16 {
		wg.Go(func() {
			status, b, e := c.call(ctx, "POST", "/v1/transfers", "demo-v1/concurrent", request)
			if e != nil {
				errs <- e
				return
			}
			if status != 201 {
				errs <- fmt.Errorf("concurrent status %d", status)
				return
			}
			var o ledger.Outcome
			if e = json.Unmarshal(b, &o); e != nil || o.Transaction == nil {
				errs <- errors.New("invalid concurrent response")
				return
			}
			txIDs <- o.Transaction.ID
		})
	}
	wg.Wait()
	close(errs)
	close(txIDs)
	for e := range errs {
		return e
	}
	unique := map[string]bool{}
	for id := range txIDs {
		unique[id] = true
	}
	if len(unique) != 1 {
		return fmt.Errorf("expected one transaction; got %d", len(unique))
	}
	fmt.Println("16 concurrent requests, one journal transaction")
	for _, id := range ids {
		status, b, err = c.call(ctx, "GET", "/v1/accounts/"+id, "", nil)
		if err != nil {
			return err
		}
		if status != 200 {
			return fmt.Errorf("account status %d", status)
		}
		fmt.Println(string(b))
	}
	app, err := open(ctx, "DATABASE_URL")
	if err != nil {
		return err
	}
	defer app.Close()
	return audit(ctx, app)
}
