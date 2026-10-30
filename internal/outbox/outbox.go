// Package outbox leases durable events and publishes without SQL locks held.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"ledgercore/internal/events"
	"ledgercore/internal/ledger"
)

type Claim struct {
	EventID, Owner string
	Payload        []byte
	Attempts       int64
}
type Config struct {
	Topic                            string
	BatchSize                        int
	Lease, SendTimeout, PollInterval time.Duration
}

func DefaultConfig(topic string) Config {
	return Config{Topic: topic, BatchSize: 32, Lease: 30 * time.Second, SendTimeout: 10 * time.Second, PollInterval: 100 * time.Millisecond}
}
func (c Config) Validate() error {
	if c.Topic == "" || c.BatchSize < 1 || c.BatchSize > 256 || c.SendTimeout < time.Second || c.Lease < c.SendTimeout+5*time.Second || c.Lease > 5*time.Minute || c.PollInterval < 10*time.Millisecond || c.PollInterval > 10*time.Second {
		return errors.New("invalid publisher settings")
	}
	return nil
}

type Publisher struct {
	Pool    *pgxpool.Pool
	Kafka   *kgo.Client
	Config  Config
	Metrics *Metrics
}

func New(pool *pgxpool.Pool, kafka *kgo.Client, c Config, m *Metrics) (*Publisher, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &Publisher{pool, kafka, c, m}, nil
}

func NewProducer(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(5*time.Millisecond), kgo.ProduceRequestTimeout(8*time.Second), kgo.RecordDeliveryTimeout(10*time.Second), kgo.ProducerBatchMaxBytes(2<<20), kgo.MaxBufferedRecords(4096), kgo.RetryBackoffFn(func(n int) time.Duration { return retryDelay(int64(n + 1)) }))
}

// ClaimBatch commits ownership before returning. A fresh fencing UUID is used for
// every batch; a delayed old owner can never acknowledge a renewed lease.
func (p *Publisher) ClaimBatch(ctx context.Context) ([]Claim, error) {
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	owner := ledger.NewID()
	rows, err := tx.Query(ctx, `WITH batch AS (SELECT event_id FROM outbox_events WHERE published_at IS NULL AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY created_at,event_id FOR UPDATE SKIP LOCKED LIMIT $1) UPDATE outbox_events e SET lease_owner=$2,lease_until=clock_timestamp()+($3::bigint*interval '1 millisecond'),attempts=attempts+1 FROM batch WHERE e.event_id=batch.event_id RETURNING e.event_id::text,e.payload,e.attempts`, p.Config.BatchSize, owner, p.Config.Lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	claims := []Claim{}
	for rows.Next() {
		c := Claim{Owner: owner}
		if err = rows.Scan(&c.EventID, &c.Payload, &c.Attempts); err != nil {
			rows.Close()
			return nil, err
		}
		claims = append(claims, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claims, nil
}

// Deliver performs only network I/O. Callers may stop between delivery and Ack;
// that expected crash window replays the exact same stable event ID.
func (p *Publisher) Deliver(ctx context.Context, claims []Claim) []error {
	errs := make([]error, len(claims))
	if len(claims) == 0 {
		return errs
	}
	records := make([]*kgo.Record, len(claims))
	for i, c := range claims {
		var e events.Event
		if err := json.Unmarshal(c.Payload, &e); err != nil {
			errs[i] = err
			continue
		}
		records[i] = &kgo.Record{Topic: p.Config.Topic, Key: []byte(e.JournalTransactionID), Value: c.Payload}
	}
	valid := make([]*kgo.Record, 0, len(records))
	indexes := make(map[*kgo.Record]int, len(records))
	for i, r := range records {
		if r != nil {
			valid = append(valid, r)
			indexes[r] = i
		}
	}
	send, cancel := context.WithTimeout(ctx, p.Config.SendTimeout)
	defer cancel()
	results := p.Kafka.ProduceSync(send, valid...)
	applyDeliveryResults(errs, indexes, results)
	return errs
}

// ProduceSync collects callbacks in completion order. Correlate record identity
// explicitly: partial success across partitions must never acknowledge another
// event's failed delivery.
func applyDeliveryResults(errs []error, indexes map[*kgo.Record]int, results kgo.ProduceResults) {
	for _, result := range results {
		if index, ok := indexes[result.Record]; ok {
			errs[index] = result.Err
		}
	}
}

func (p *Publisher) Ack(ctx context.Context, c Claim, deliveryErr error) (bool, error) {
	var tag int64
	if deliveryErr == nil {
		result, err := p.Pool.Exec(ctx, `UPDATE outbox_events SET published_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL,last_error_code=NULL WHERE event_id=$1 AND lease_owner=$2 AND published_at IS NULL`, c.EventID, c.Owner)
		if err != nil {
			return false, err
		}
		tag = result.RowsAffected()
	} else {
		result, err := p.Pool.Exec(ctx, `UPDATE outbox_events SET lease_owner=NULL,lease_until=NULL,next_attempt_at=clock_timestamp()+($3::bigint*interval '1 millisecond'),last_error_code='publish_failed' WHERE event_id=$1 AND lease_owner=$2 AND published_at IS NULL`, c.EventID, c.Owner, retryDelay(c.Attempts).Milliseconds())
		if err != nil {
			return false, err
		}
		tag = result.RowsAffected()
	}
	if p.Metrics != nil {
		outcome := "published"
		if deliveryErr != nil {
			outcome = "failed"
			p.Metrics.Retries.Inc()
		}
		if tag == 0 {
			outcome = "stale_owner"
		}
		p.Metrics.Publications.WithLabelValues(outcome).Inc()
	}
	return tag == 1, nil
}

func retryDelay(attempt int64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 9 {
		attempt = 9
	}
	d := 100 * time.Millisecond * time.Duration(1<<uint(attempt-1))
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}
func (p *Publisher) RunOnce(ctx context.Context) (int, error) {
	claims, err := p.ClaimBatch(ctx)
	if err != nil {
		return 0, err
	}
	results := p.Deliver(ctx, claims)
	for i, c := range claims {
		if _, err = p.Ack(ctx, c, results[i]); err != nil {
			return 0, err
		}
	}
	return len(claims), nil
}
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.Config.PollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		n, err := p.RunOnce(ctx)
		if err != nil {
			return fmt.Errorf("publisher state update failed: %w", err)
		}
		if err = p.ObserveBacklog(ctx); err != nil {
			return err
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}
}
func (p *Publisher) ObserveBacklog(ctx context.Context) error {
	if p.Metrics == nil {
		return nil
	}
	var count int64
	var oldest float64
	err := p.Pool.QueryRow(ctx, `SELECT count(*),COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)),0)::double precision FROM outbox_events WHERE published_at IS NULL`).Scan(&count, &oldest)
	if err == nil {
		p.Metrics.Backlog.Set(float64(count))
		p.Metrics.Oldest.Set(oldest)
		stats := p.Pool.Stat()
		p.Metrics.PoolConnections.Set(float64(stats.AcquiredConns()))
	}
	return err
}

type Metrics struct {
	Backlog, Oldest, PoolConnections prometheus.Gauge
	Publications                     *prometheus.CounterVec
	Retries                          prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{Backlog: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_outbox_backlog", Help: "Unpublished logical events."}), Oldest: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_outbox_oldest_seconds", Help: "Age of the oldest unpublished event."}), PoolConnections: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_publisher_db_acquired_connections", Help: "Currently acquired DB connections."}), Publications: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ledger_outbox_publications_total", Help: "Publish attempts by bounded outcome."}, []string{"outcome"}), Retries: prometheus.NewCounter(prometheus.CounterOpts{Name: "ledger_outbox_retries_total", Help: "Failed publishes scheduled for retry."})}
	reg.MustRegister(m.Backlog, m.Oldest, m.PoolConnections, m.Publications, m.Retries)
	return m
}
