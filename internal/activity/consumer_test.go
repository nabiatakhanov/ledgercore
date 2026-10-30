package activity

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"ledgercore/internal/ledger"
)

func kafka(t *testing.T, d *testDB) (*kgo.Client, ConsumerConfig) {
	t.Helper()
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		if os.Getenv("LEDGER_REQUIRE_EVENTS_INTEGRATION") == "1" {
			t.Fatal("KAFKA_TEST_BROKERS required")
		}
		t.Skip("NOT RUN: Kafka test brokers not configured")
	}
	k, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(5*time.Second), kgo.ProducerBatchMaxBytes(2<<20))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	c := ConsumerConfig{Brokers: strings.Split(brokers, ","), Topic: "activity-test-" + ledger.NewID(), DLQTopic: "activity-test-dlq-" + ledger.NewID(), Group: "activity-test-group-" + ledger.NewID(), MaxPartitions: 3, RecordTimeout: 3 * time.Second}
	admin := kadm.NewClient(k)
	for _, name := range []string{c.Topic, c.DLQTopic} {
		res, err := admin.CreateTopics(d.ctx, 3, 1, nil, name)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range res {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
		}
	}
	return k, c
}
func wait(t *testing.T, ctx context.Context, f func() bool) {
	t.Helper()
	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	for {
		if f() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("condition deadline: ", ctx.Err())
		case <-timer.C:
		}
	}
}
func caughtUp(ctx context.Context, k *kgo.Client, topic, group string) bool {
	admin := kadm.NewClient(k)
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return false
	}
	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return false
	}
	for partition, end := range ends[topic] {
		if end.Offset == 0 {
			continue
		}
		offset, ok := offsets.Lookup(topic, partition)
		if !ok || offset.Err != nil || offset.At < end.Offset {
			return false
		}
	}
	return true
}
func startConsumer(t *testing.T, d *testDB, cfg ConsumerConfig) (*Consumer, context.CancelFunc, <-chan error) {
	t.Helper()
	c, err := NewConsumer(cfg, d.app, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(d.ctx)
	done := make(chan error, 1)
	go func() { err := c.Run(ctx); c.Close(); done <- err }()
	t.Cleanup(func() { cancel(); c.Close() })
	return c, cancel, done
}

func TestKafkaConsumerReplayRebalanceAndDLQ(t *testing.T) {
	d := database(t)
	k, cfg := kafka(t, d)
	a, b := ledger.NewID(), ledger.NewID()
	for i := 0; i < 24; i++ {
		e := event(a, b, "3")
		data, _ := json.Marshal(e)
		if err := k.ProduceSync(d.ctx, &kgo.Record{Topic: cfg.Topic, Key: []byte(e.EventID), Value: data}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	_, stop1, done1 := startConsumer(t, d, cfg)
	wait(t, d.ctx, func() bool {
		p, err := d.s.GetAccountActivity(d.ctx, a, "", 100)
		return err == nil && p.Summary.OutgoingCount == "24"
	})
	// A second instance joins the same group; both can process only assigned
	// partitions, and later records remain complete through that rebalance.
	_, stop2, done2 := startConsumer(t, d, cfg)
	wait(t, d.ctx, func() bool {
		groups, err := kadm.NewClient(k).DescribeGroups(d.ctx, cfg.Group)
		if err != nil {
			return false
		}
		g, ok := groups[cfg.Group]
		return ok && g.Err == nil && len(g.Members) == 2 && g.State == "Stable"
	})
	for i := 0; i < 24; i++ {
		e := event(a, b, "3")
		data, _ := json.Marshal(e)
		if err := k.ProduceSync(d.ctx, &kgo.Record{Topic: cfg.Topic, Key: []byte(e.EventID), Value: data}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range [][]byte{[]byte("{malformed"), []byte(`{"schema_version":999}`)} {
		if err := k.ProduceSync(d.ctx, &kgo.Record{Topic: cfg.Topic, Value: raw}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	wait(t, d.ctx, func() bool { return caughtUp(d.ctx, k, cfg.Topic, cfg.Group) })
	p, err := d.s.GetAccountActivity(d.ctx, a, "", 100)
	if err != nil || len(p.Entries) != 48 || p.Summary.OutgoingMinor != "144" {
		t.Fatalf("rebalance projection %+v %v", p, err)
	}
	ends, err := kadm.NewClient(k).ListEndOffsets(d.ctx, cfg.DLQTopic)
	if err != nil {
		t.Fatal(err)
	}
	var dlqCount int64
	for _, offset := range ends[cfg.DLQTopic] {
		dlqCount += offset.Offset
	}
	if dlqCount != 2 {
		t.Fatalf("DLQ count %d", dlqCount)
	}
	dlqReader, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.DLQTopic: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer dlqReader.Close()
	dlqIDs := map[string]bool{}
	for len(dlqIDs) < 2 {
		fetches := dlqReader.PollRecords(d.ctx, 2)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs[0].Err)
		}
		if d.ctx.Err() != nil {
			t.Fatal(d.ctx.Err())
		}
		fetches.EachRecord(func(record *kgo.Record) {
			var envelope DLQEnvelope
			if err := json.Unmarshal(record.Value, &envelope); err != nil {
				t.Fatal(err)
			}
			expected := fmt.Sprintf("%s/%d/%d", envelope.Topic, envelope.Partition, envelope.Offset)
			if envelope.SourceID != expected || string(record.Key) != expected || (envelope.Classification != "malformed" && envelope.Classification != "unsupported_schema") || len(envelope.Value) == 0 || envelope.Truncated {
				t.Fatalf("DLQ envelope %+v", envelope)
			}
			dlqIDs[envelope.SourceID] = true
		})
	}
	stop1()
	stop2()
	if err = <-done1; err != nil {
		t.Fatal(err)
	}
	if err = <-done2; err != nil {
		t.Fatal(err)
	}
	cfg.Group = "activity-replay-" + ledger.NewID()
	_, stopReplay, doneReplay := startConsumer(t, d, cfg)
	wait(t, d.ctx, func() bool { return caughtUp(d.ctx, k, cfg.Topic, cfg.Group) })
	stopReplay()
	if err = <-doneReplay; err != nil {
		t.Fatal(err)
	}
	p, err = d.s.GetAccountActivity(d.ctx, a, "", 100)
	if err != nil || len(p.Entries) != 48 || p.Summary.OutgoingCount != "48" || p.Summary.OutgoingMinor != "144" {
		t.Fatalf("replay duplicated effect %+v %v", p, err)
	}
	d.audit(t)
}

func TestKafkaDatabaseFailureAndDLQFailureDoNotCommit(t *testing.T) {
	d := database(t)
	k, cfg := kafka(t, d)
	a, b := ledger.NewID(), ledger.NewID()
	e := event(a, b, "7")
	raw, _ := json.Marshal(e)
	if err := k.ProduceSync(d.ctx, &kgo.Record{Topic: cfg.Topic, Key: []byte(e.EventID), Value: raw}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	// This is an explicitly isolated test-only permission fault; restore before
	// restarting the same group. The failed consumer must never commit its offset.
	if _, err := d.admin.Exec(d.ctx, `REVOKE INSERT ON activity_inbox FROM activity_app`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = d.admin.Exec(context.Background(), `GRANT INSERT ON activity_inbox TO activity_app`) }()
	_, _, done := startConsumer(t, d, cfg)
	if err := <-done; err == nil {
		t.Fatal("persistent DB error ignored")
	}
	offsets, err := kadm.NewClient(k).FetchOffsets(d.ctx, cfg.Group)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range offsets[cfg.Topic] {
		if o.At > 0 {
			t.Fatalf("failed DB advanced offset %d", o.At)
		}
	}
	if _, err = d.admin.Exec(d.ctx, `GRANT INSERT ON activity_inbox TO activity_app`); err != nil {
		t.Fatal(err)
	}
	_, stop, done := startConsumer(t, d, cfg)
	wait(t, d.ctx, func() bool { return caughtUp(d.ctx, k, cfg.Topic, cfg.Group) })
	stop()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	// Point DLQ at an unreachable broker in a separate client while retaining
	// source coordinates; ProcessRecord cannot acknowledge or discard it.
	badCfg := cfg
	badCfg.Brokers = []string{"127.0.0.1:1"}
	badCfg.Group = "unreachable-dlq-test"
	bad, err := NewConsumer(badCfg, d.app, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	deadline, cancel := context.WithTimeout(d.ctx, 300*time.Millisecond)
	defer cancel()
	if err = bad.ProcessRecord(deadline, &kgo.Record{Topic: cfg.Topic, Partition: 0, Offset: 999, Value: []byte("bad")}); err == nil {
		t.Fatal("unconfirmed DLQ treated as success")
	}
	// A real broker-side DLQ write rejection must also preserve the source
	// offset. Restore the topic limit and restart the same group to recover it.
	admin := kadm.NewClient(k)
	tiny := "1"
	responses, err := admin.AlterTopicConfigs(d.ctx, []kadm.AlterConfig{{Name: "max.message.bytes", Value: &tiny}}, cfg.DLQTopic)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range responses {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	invalidRecord := &kgo.Record{Topic: cfg.Topic, Key: []byte(e.EventID), Value: []byte("{invalid")}
	if err = k.ProduceSync(d.ctx, invalidRecord).FirstErr(); err != nil {
		t.Fatal(err)
	}
	_, _, failedDLQ := startConsumer(t, d, cfg)
	if err = <-failedDLQ; err == nil {
		t.Fatal("broker rejected DLQ without stopping consumer")
	}
	offsets, err = admin.FetchOffsets(d.ctx, cfg.Group)
	if err != nil {
		t.Fatal(err)
	}
	if offset, ok := offsets.Lookup(cfg.Topic, invalidRecord.Partition); ok && offset.At > invalidRecord.Offset {
		t.Fatal("rejected DLQ advanced source offset")
	}
	normal := "2097152"
	responses, err = admin.AlterTopicConfigs(d.ctx, []kadm.AlterConfig{{Name: "max.message.bytes", Value: &normal}}, cfg.DLQTopic)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range responses {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	_, stopRecovered, doneRecovered := startConsumer(t, d, cfg)
	wait(t, d.ctx, func() bool { return caughtUp(d.ctx, k, cfg.Topic, cfg.Group) })
	stopRecovered()
	if err = <-doneRecovered; err != nil {
		t.Fatal(err)
	}
	d.audit(t)
}

func TestActivityCrashHelper(t *testing.T) {
	if os.Getenv("ACTIVITY_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	d := database(t)
	var cfg ConsumerConfig
	if err := json.Unmarshal([]byte(os.Getenv("ACTIVITY_CRASH_CONFIG")), &cfg); err != nil {
		t.Fatal(err)
	}
	c, err := NewConsumer(cfg, d.app, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fetches := c.Kafka.PollRecords(d.ctx, 1)
	if fetches.NumRecords() == 0 {
		t.Fatal("helper got no source record")
	}
	var record *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) { record = r })
	if err = c.ProcessRecord(d.ctx, record); err != nil {
		t.Fatal(err)
	}
	fmt.Println("ACTIVITY_SQL_COMMITTED")
	// A pipe handshake makes the crash window exact: no source offset commit
	// runs in this helper. The parent kills it after seeing this line.
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	t.Fatal("parent must kill helper before offset commit")
}

func TestKafkaActivityProcessDeathAfterSQLBeforeOffset(t *testing.T) {
	d := database(t)
	k, cfg := kafka(t, d)
	a, b := ledger.NewID(), ledger.NewID()
	e := event(a, b, "13")
	raw, _ := json.Marshal(e)
	if err := k.ProduceSync(d.ctx, &kgo.Record{Topic: cfg.Topic, Key: []byte(e.EventID), Value: raw}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(cfg)
	cmd := exec.CommandContext(d.ctx, binary, "-test.run=^TestActivityCrashHelper$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "ACTIVITY_CRASH_HELPER=1", "ACTIVITY_CRASH_CONFIG="+string(encoded))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "ACTIVITY_SQL_COMMITTED" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("helper did not reach committed SQL barrier")
		}
	case <-d.ctx.Done():
		t.Fatal("helper barrier deadline")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("helper exited without crash")
	}
	_, stop, done := startConsumer(t, d, cfg)
	wait(t, d.ctx, func() bool { return caughtUp(d.ctx, k, cfg.Topic, cfg.Group) })
	stop()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	p, err := d.s.GetAccountActivity(d.ctx, a, "", 10)
	if err != nil || len(p.Entries) != 1 || p.Summary.OutgoingMinor != "13" || p.Summary.OutgoingCount != "1" {
		t.Fatalf("crash replay %+v %v", p, err)
	}
	d.audit(t)
}
