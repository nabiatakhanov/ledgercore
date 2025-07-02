package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/ledger"
	"ledgercore/internal/reconcile"
)

type measurement struct {
	Scenario             string             `json:"scenario"`
	Clients              int                `json:"clients"`
	Requests             int                `json:"requests"`
	Success              int                `json:"success"`
	BusinessRefusals     int                `json:"business_refusals"`
	InfrastructureErrors int                `json:"infrastructure_errors"`
	Seconds              float64            `json:"seconds"`
	Throughput           float64            `json:"requests_per_second"`
	P50                  float64            `json:"p50_ms"`
	P95                  float64            `json:"p95_ms"`
	P99                  float64            `json:"p99_ms"`
	ReconcileOK          bool               `json:"reconcile_ok"`
	MetricsBefore        map[string]float64 `json:"metrics_before"`
	MetricsAfter         map[string]float64 `json:"metrics_after"`
}

func bench(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("bench", flag.ContinueOnError)
	n := f.Int("n", 1024, "requests per scenario, at least32")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *n < 32 || *n > 100000 {
		return errors.New("n must be32..100000")
	}
	p, err := open(ctx, "FIXTURE_DATABASE_URL")
	if err != nil {
		return err
	}
	defer p.Close()
	app, err := open(ctx, "DATABASE_URL")
	if err != nil {
		return err
	}
	defer app.Close()
	c, err := newClient()
	if err != nil {
		return err
	}
	defer c.http.CloseIdleConnections()
	results := []measurement{}
	for _, scenario := range []string{"hot", "independent"} {
		for _, clients := range []int{1, 8, 32} {
			namespace := "bench-" + ledger.NewID()
			pairs := make([][]string, clients)
			for i := range pairs {
				if scenario == "hot" && i > 0 {
					pairs[i] = pairs[0]
					continue
				}
				pairs[i], err = fixture(ctx, p, fmt.Sprintf("%s-%d", namespace, i), ledger.Money(*n+1))
				if err != nil {
					return err
				}
			}
			m := measurement{Scenario: scenario, Clients: clients, Requests: *n, MetricsBefore: sampleMetrics(ctx)}
			latencies := make([]time.Duration, *n)
			statuses := make([]int, *n)
			jobs := make(chan int)
			var wg sync.WaitGroup
			start := time.Now()
			for worker := range clients {
				wg.Go(func() {
					for i := range jobs {
						pair := pairs[worker]
						req := ledger.TransferRequest{FromAccountID: pair[0], ToAccountID: pair[1], AmountMinor: "1", Currency: "RUB"}
						t := time.Now()
						status, body, e := c.call(ctx, "POST", "/v1/transfers", fmt.Sprintf("%s/%d", namespace, i), req)
						latencies[i] = time.Since(t)
						if e != nil {
							statuses[i] = 0
						} else {
							statuses[i] = classifyResponse(status, body, req)
						}
					}
				})
			}
			for i := range *n {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			m.Seconds = time.Since(start).Seconds()
			for _, status := range statuses {
				switch {
				case status == 201:
					m.Success++
				case status == 409:
					m.BusinessRefusals++
				default:
					m.InfrastructureErrors++
				}
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			percentile := func(p int) float64 { return float64(latencies[((*n*p+99)/100)-1]) / float64(time.Millisecond) }
			m.P50 = percentile(50)
			m.P95 = percentile(95)
			m.P99 = percentile(99)
			m.Throughput = float64(*n) / m.Seconds
			m.MetricsAfter = sampleMetrics(ctx)
			r, e := reconcile.Run(ctx, app)
			if e != nil {
				return e
			}
			m.ReconcileOK = r.OK
			results = append(results, m)
			fmt.Fprintf(os.Stderr, "%s clients=%d success=%d refusals=%d infra=%d reconcile=%t\n", scenario, clients, m.Success, m.BusinessRefusals, m.InfrastructureErrors, r.OK)
		}
	}
	if err = output(results); err != nil {
		return err
	}
	for _, r := range results {
		if !r.ReconcileOK || r.InfrastructureErrors > 0 {
			return errors.New("benchmark failed reconciliation or infrastructure requests")
		}
	}
	return nil
}

// Only expected ledger results count. Authentication/configuration failures must
// fail the experiment, rather than look like valid financial business refusals.
func classifyResponse(status int, body []byte, req ledger.TransferRequest) int {
	var out ledger.Outcome
	if json.Unmarshal(body, &out) != nil {
		return 0
	}
	if status == 201 && out.Transaction != nil {
		t := out.Transaction
		if t.Kind == "transfer" && t.FromAccountID == req.FromAccountID && t.ToAccountID == req.ToAccountID && t.AmountMinor == req.AmountMinor && t.Currency == req.Currency && len(t.Postings) == 2 {
			if _, err := ledger.NormalizeID(t.ID); err == nil {
				return 201
			}
		}
	}
	if status == 409 && out.Error != nil {
		switch out.Error.Code {
		case "insufficient_funds", "balance_overflow", "sequence_overflow":
			return 409
		}
	}
	return 0
}

// Missing process metrics remain absent, never substituted with invented values.
func sampleMetrics(ctx context.Context) map[string]float64 {
	values := map[string]float64{}
	url := os.Getenv("METRICS_URL")
	if url == "" {
		url = "http://127.0.0.1:9090/metrics"
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return values
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return values
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return values
	}
	for _, line := range strings.Split(string(b), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "process_cpu_seconds_total", "process_resident_memory_bytes", "go_memstats_heap_alloc_bytes", "go_goroutines":
			if n, e := strconv.ParseFloat(parts[1], 64); e == nil {
				values[parts[0]] = n
			}
		}
	}
	return values
}
func explain(ctx context.Context, p *pgxpool.Pool) error {
	var id string
	if err := p.QueryRow(ctx, `SELECT id::text FROM accounts WHERE entry_sequence>0 ORDER BY entry_sequence DESC LIMIT 1`).Scan(&id); err != nil {
		return err
	}
	queries := []struct {
		name, sql string
		args      []any
	}{{"ordered account locks", `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) SELECT id,kind,balance_minor,entry_sequence FROM accounts WHERE id=$1 OR id=$2 ORDER BY id FOR UPDATE`, []any{id, ledger.SystemAccountID}}, {"history keyset", `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) SELECT transaction_id,account_sequence,delta_minor FROM postings WHERE account_id=$1 AND account_sequence<=$2 ORDER BY account_sequence DESC LIMIT 51`, []any{id, int64(9223372036854775807)}}, {"journal reconciliation", `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) SELECT account_id,sum(delta_minor::numeric) FROM postings GROUP BY account_id`, nil}}
	for _, q := range queries {
		fmt.Println(q.name)
		rows, err := p.Query(ctx, q.sql, q.args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var line string
			if err = rows.Scan(&line); err != nil {
				rows.Close()
				return err
			}
			fmt.Println(line)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
