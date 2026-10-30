package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/eventaudit"
	"ledgercore/internal/ledger"
)

type distribution struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

func latencyDistribution(values []float64) distribution {
	d := distribution{Count: len(values)}
	if len(values) == 0 {
		return d
	}
	sort.Float64s(values)
	at := func(p float64) float64 { return values[int(math.Ceil(float64(len(values))*p))-1] }
	d.P50 = at(.50)
	d.P95 = at(.95)
	d.P99 = at(.99)
	d.Max = values[len(values)-1]
	return d
}

type eventMeasurement struct {
	Clients              int          `json:"clients"`
	Repeat               int          `json:"repeat"`
	Requests             int          `json:"requests"`
	Success              int          `json:"success"`
	BusinessRefusals     int          `json:"business_refusals"`
	RateLimited          int          `json:"rate_limited"`
	InfrastructureErrors int          `json:"infrastructure_errors"`
	Seconds              float64      `json:"seconds"`
	Throughput           float64      `json:"requests_per_second"`
	HTTP                 distribution `json:"http_latency"`
	CommitToProjection   distribution `json:"commit_to_projection"`
	CatchUpSeconds       float64      `json:"post_requests_catch_up_seconds"`
	Audited              bool         `json:"audited"`
}

func eventsBench(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("events-bench", flag.ContinueOnError)
	n := f.Int("n", 256, "requests per case")
	repeats := f.Int("repeats", 3, "independent repetitions per client count")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *n < 32 || *n > 100000 || *repeats < 2 || *repeats > 10 {
		return errors.New("n must be32..100000 and repeats2..10")
	}
	l, err := open(ctx, "DATABASE_URL")
	if err != nil {
		return err
	}
	defer l.Close()
	a, err := open(ctx, "ACTIVITY_DATABASE_URL")
	if err != nil {
		return err
	}
	defer a.Close()
	fund, err := open(ctx, "FIXTURE_DATABASE_URL")
	if err != nil {
		return err
	}
	defer fund.Close()
	base, err := newClient()
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 128
	transport.MaxConnsPerHost = 128
	defer transport.CloseIdleConnections()
	base.http = &http.Client{Transport: transport, Timeout: 7 * time.Second}
	urls := strings.Split(envValue("GATEWAY_URLS", base.base), ",")
	clients := make([]*client, len(urls))
	for i, url := range urls {
		clients[i] = &client{base: strings.TrimRight(url, "/"), token: base.token, http: base.http}
	}
	results := []eventMeasurement{}
	for _, concurrency := range []int{1, 8, 32} {
		for repeat := 1; repeat <= *repeats; repeat++ {
			prefix := "event-bench-" + ledger.NewID()
			pairs := make([][]string, concurrency)
			for i := range concurrency {
				pairs[i], err = fixture(ctx, fund, fmt.Sprintf("%s/%d", prefix, i), ledger.Money(*n+1))
				if err != nil {
					return err
				}
			}
			m := eventMeasurement{Clients: concurrency, Repeat: repeat, Requests: *n}
			statuses := make([]int, *n)
			delays := make([]float64, *n)
			ids := make([]string, *n)
			jobs := make(chan int)
			var wg sync.WaitGroup
			started := time.Now()
			for worker := range concurrency {
				wg.Go(func() {
					for i := range jobs {
						pair := pairs[worker]
						req := ledger.TransferRequest{FromAccountID: pair[0], ToAccountID: pair[1], AmountMinor: "1", Currency: "RUB"}
						start := time.Now()
						status, body, e := clients[i%len(clients)].call(ctx, "POST", "/v1/transfers", fmt.Sprintf("%s/%d", prefix, i), req)
						delays[i] = float64(time.Since(start)) / float64(time.Millisecond)
						if e != nil {
							continue
						}
						if status == 429 {
							statuses[i] = 429
							continue
						}
						statuses[i] = classifyResponse(status, body, req)
						if statuses[i] == 201 {
							var out ledger.Outcome
							if json.Unmarshal(body, &out) == nil && out.Transaction != nil {
								ids[i] = out.Transaction.ID
							}
						}
					}
				})
			}
			for i := range *n {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			m.Seconds = time.Since(started).Seconds()
			m.Throughput = float64(*n) / m.Seconds
			m.HTTP = latencyDistribution(delays)
			successful := []string{}
			for i, status := range statuses {
				switch status {
				case 201:
					m.Success++
					successful = append(successful, ids[i])
				case 409:
					m.BusinessRefusals++
				case 429:
					m.RateLimited++
				default:
					m.InfrastructureErrors++
				}
			}
			catchUp := time.Now()
			if err = waitIDs(ctx, a, successful); err != nil {
				return err
			}
			m.CatchUpSeconds = time.Since(catchUp).Seconds()
			m.CommitToProjection, err = commitVisibility(ctx, l, a, successful)
			if err != nil {
				return err
			}
			report, e := eventaudit.Run(ctx, l, a, true)
			if e != nil {
				return e
			}
			m.Audited = report.OK
			results = append(results, m)
			fmt.Fprintf(os.Stderr, "events clients=%d repeat=%d success=%d limited=%d infra=%d audited=%t\n", concurrency, repeat, m.Success, m.RateLimited, m.InfrastructureErrors, m.Audited)
		}
	}
	report := map[string]any{"measured_at": time.Now().UTC(), "source_commit": os.Getenv("SOURCE_COMMIT_SHA"), "go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "load_generator_cpus": runtime.NumCPU(), "gateway_count": len(urls), "rate_limit": os.Getenv("RATE_LIMIT"), "rate_window": os.Getenv("RATE_WINDOW"), "requests_per_case": *n, "repeats_per_client_count": *repeats, "method": "HTTP through Gateway -> Redis admission -> gRPC Ledger -> PostgreSQL. Independent wallet pairs. HTTP includes rate admission and commit response. Event delay is PostgreSQL commit timestamp of immutable inbox insert minus ledger header creation transaction commit, track_commit_timestamp required; DBs share host clock. Setup/catch-up excluded from HTTP throughput. Each repetition uses new wallets/keys. No production SLA claim.", "cases": results}
	if err = output(report); err != nil {
		return err
	}
	for _, r := range results {
		if r.InfrastructureErrors > 0 || !r.Audited {
			return errors.New("benchmark infrastructure/audit failure")
		}
	}
	return nil
}

func commitVisibility(ctx context.Context, l, a *pgxpool.Pool, ids []string) (distribution, error) {
	commits := map[string]time.Time{}
	rows, err := l.Query(ctx, `SELECT id::text,pg_xact_commit_timestamp(creation_xid::text::xid) FROM journal_transactions WHERE id::text=ANY($1::text[])`, ids)
	if err != nil {
		return distribution{}, err
	}
	for rows.Next() {
		var id string
		var at *time.Time
		if err = rows.Scan(&id, &at); err != nil {
			rows.Close()
			return distribution{}, err
		}
		if at == nil {
			rows.Close()
			return distribution{}, errors.New("commit timestamp unavailable; enable track_commit_timestamp for experiment")
		}
		commits[id] = *at
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return distribution{}, err
	}
	rows, err = a.Query(ctx, `SELECT event_id::text,pg_xact_commit_timestamp(xmin) FROM activity_inbox WHERE event_id::text=ANY($1::text[])`, ids)
	if err != nil {
		return distribution{}, err
	}
	values := []float64{}
	for rows.Next() {
		var id string
		var at *time.Time
		if err = rows.Scan(&id, &at); err != nil {
			rows.Close()
			return distribution{}, err
		}
		source, ok := commits[id]
		if at == nil || !ok {
			rows.Close()
			return distribution{}, errors.New("missing source/projection commit timestamp")
		}
		delay := float64(at.Sub(source)) / float64(time.Millisecond)
		if delay < 0 {
			rows.Close()
			return distribution{}, errors.New("negative commit lag: clocks differ")
		}
		values = append(values, delay)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return distribution{}, err
	}
	if len(values) != len(ids) {
		return distribution{}, errors.New("missing visibility samples")
	}
	return latencyDistribution(values), nil
}
