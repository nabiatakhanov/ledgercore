package activity

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"ledgercore/internal/events"
)

type ConsumerConfig struct {
	Brokers       []string
	Topic         string
	DLQTopic      string
	Group         string
	MaxPartitions int
	RecordTimeout time.Duration
}
type Consumer struct {
	Kafka      *kgo.Client
	Store      *Store
	config     ConsumerConfig
	processed  *prometheus.CounterVec
	duplicates prometheus.Counter
	visibility prometheus.Histogram
	lag        *prometheus.GaugeVec
	active     prometheus.Gauge
}

// NewConsumer uses an idempotent producer (franz-go default) and acks=all for
// DLQ writes. Offset auto-commit is disabled. BlockRebalanceOnPoll preserves
// partition ownership until every fetched partition's successful prefix is
// committed. Dev replication factor 1 does not provide node/disk redundancy.
func NewConsumer(c ConsumerConfig, pool *pgxpool.Pool, register prometheus.Registerer) (*Consumer, error) {
	if len(c.Brokers) == 0 || c.Topic == "" || c.DLQTopic == "" || c.Topic == c.DLQTopic || c.Group == "" || c.MaxPartitions < 1 || c.MaxPartitions > 64 || c.RecordTimeout < time.Second || c.RecordTimeout > time.Minute {
		return nil, errors.New("invalid activity consumer configuration")
	}
	k, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.ConsumerGroup(c.Group), kgo.ConsumeTopics(c.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.FetchMaxBytes(8<<20), kgo.FetchMaxPartitionBytes(2<<20), kgo.MaxBufferedRecords(1000), kgo.ProducerBatchMaxBytes(2<<20), kgo.ProduceRequestTimeout(5*time.Second), kgo.RecordDeliveryTimeout(10*time.Second), kgo.RequestTimeoutOverhead(2*time.Second), kgo.SessionTimeout(10*time.Second), kgo.HeartbeatInterval(3*time.Second), kgo.RebalanceTimeout(60*time.Second))
	if err != nil {
		return nil, err
	}
	x := &Consumer{Kafka: k, Store: New(pool), config: c}
	x.processed = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "activity_processed_total", Help: "Projection records by bounded outcome."}, []string{"outcome"})
	x.duplicates = prometheus.NewCounter(prometheus.CounterOpts{Name: "activity_duplicates_total", Help: "Inbox replays with no repeated effect."})
	x.visibility = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "activity_visibility_seconds", Help: "Occurred-at (SQL time before source commit) to successful projection commit; includes source SQL, delivery and projection.", Buckets: prometheus.ExponentialBuckets(.001, 4, 10)})
	x.lag = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "activity_consumer_lag", Help: "Last fetched partition high watermark minus last processed offset; fixed configured topic only."}, []string{"partition"})
	x.active = prometheus.NewGauge(prometheus.GaugeOpts{Name: "activity_active_partitions", Help: "Partition handlers currently holding bounded concurrency slots."})
	if register != nil {
		collectors := []prometheus.Collector{x.processed, x.duplicates, x.visibility, x.lag, x.active}
		if pool != nil {
			collectors = append(collectors, prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "activity_db_connections", Help: "Total Activity pool connections."}, func() float64 { return float64(pool.Stat().TotalConns()) }), prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "activity_db_acquired_connections", Help: "Activity pool connections currently acquired."}, func() float64 { return float64(pool.Stat().AcquiredConns()) }))
		}
		for _, collector := range collectors {
			if err = register.Register(collector); err != nil {
				k.Close()
				return nil, err
			}
		}
	}
	return x, nil
}

type DLQEnvelope struct {
	SourceID       string `json:"source_id"`
	Topic          string `json:"topic"`
	Partition      int32  `json:"partition"`
	Offset         int64  `json:"offset"`
	Classification string `json:"classification"`
	Key            []byte `json:"key"`
	Value          []byte `json:"value"`
	PayloadBytes   int    `json:"payload_bytes"`
	PayloadSHA256  string `json:"payload_sha256"`
	Truncated      bool   `json:"truncated"`
}

func (c *Consumer) deadLetter(ctx context.Context, r *kgo.Record, code string) error {
	value := r.Value
	truncated := false
	if len(value) > events.MaxBytes {
		value = value[:events.MaxBytes]
		truncated = true
	}
	key := r.Key
	if len(key) > 4096 {
		key = key[:4096]
	}
	id := fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
	envelope := DLQEnvelope{SourceID: id, Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Classification: code, Key: key, Value: value, PayloadBytes: len(r.Value), PayloadSHA256: fmt.Sprintf("%x", sha256.Sum256(r.Value)), Truncated: truncated}
	b, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	if err = c.Kafka.ProduceSync(ctx, &kgo.Record{Topic: c.config.DLQTopic, Key: []byte(id), Value: b}).FirstErr(); err != nil {
		c.processed.WithLabelValues("dlq_error").Inc()
		return fmt.Errorf("DLQ delivery failed: %w", err)
	}
	c.processed.WithLabelValues("dlq").Inc()
	return nil
}

// ProcessRecord commits SQL (or confirms DLQ delivery) but deliberately does not
// commit Kafka offsets. The caller must only commit the source record after this
// method succeeds. This boundary is also usable by a crash-test subprocess.
func (c *Consumer) ProcessRecord(ctx context.Context, r *kgo.Record) error {
	e, err := events.Decode(r.Value)
	if err != nil {
		var invalid *events.InvalidEvent
		if errors.As(err, &invalid) {
			return c.deadLetter(ctx, r, invalid.Code)
		}
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		duplicate, applyErr := c.Store.Apply(ctx, e)
		if applyErr == nil {
			if duplicate {
				c.duplicates.Inc()
				c.processed.WithLabelValues("duplicate").Inc()
			} else {
				c.processed.WithLabelValues("applied").Inc()
				age := time.Since(e.OccurredAt).Seconds()
				if age < 0 {
					age = 0
				}
				c.visibility.Observe(age)
			}
			return nil
		}
		var invalid *events.InvalidEvent
		if errors.As(applyErr, &invalid) {
			return c.deadLetter(ctx, r, invalid.Code)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == 2 {
			c.processed.WithLabelValues("database_error").Inc()
			return fmt.Errorf("activity database processing failed after three attempts: %w", applyErr)
		}
		c.processed.WithLabelValues("database_retry").Inc()
		timer := time.NewTimer(time.Duration(25*(1<<attempt)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("unreachable activity retry state")
}

// Run processes each partition sequentially and bounds concurrent partitions.
// A persistent record/commit error stops the process. It never advances across
// a failed record; a restart resumes that group's uncommitted prefix.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.Kafka.PollRecords(ctx, 500)
		if ctx.Err() != nil {
			c.Kafka.AllowRebalance()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			c.Kafka.AllowRebalance()
			return fmt.Errorf("activity Kafka fetch failed: %w", errs[0].Err)
		}
		workCtx, cancel := context.WithCancel(ctx)
		sem := make(chan struct{}, c.config.MaxPartitions)
		var wg sync.WaitGroup
		errs := make(chan error, 1)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-workCtx.Done():
					return
				}
				defer func() { <-sem }()
				c.active.Inc()
				defer c.active.Dec()
				for _, r := range p.Records {
					recordCtx, done := context.WithTimeout(workCtx, c.config.RecordTimeout)
					err := c.ProcessRecord(recordCtx, r)
					if err == nil {
						err = c.Kafka.CommitRecords(recordCtx, r)
					}
					done()
					if err != nil {
						select {
						case errs <- err:
						default:
						}
						cancel()
						return
					}
					lag := p.HighWatermark - r.Offset - 1
					if lag < 0 {
						lag = 0
					}
					c.lag.WithLabelValues(fmt.Sprint(p.Partition)).Set(float64(lag))
				}
			}()
		})
		wg.Wait()
		cancel()
		c.Kafka.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
		select {
		case err := <-errs:
			return err
		default:
		}
	}
}
func (c *Consumer) Close() { c.Kafka.CloseAllowingRebalance() }
