package outbox

import (
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
	"time"
)

func TestDeliveryResultsFollowRecordNotCompletionOrder(t *testing.T) {
	first, second := &kgo.Record{Key: []byte("first")}, &kgo.Record{Key: []byte("second")}
	failure := errors.New("partition failed")
	errs := make([]error, 2)
	applyDeliveryResults(errs, map[*kgo.Record]int{first: 0, second: 1}, kgo.ProduceResults{{Record: second, Err: failure}, {Record: first}})
	if errs[0] != nil || !errors.Is(errs[1], failure) {
		t.Fatalf("acknowledged wrong event: %v", errs)
	}
}

func TestRetryBounded(t *testing.T) {
	last := time.Duration(0)
	for i := int64(0); i < 100; i++ {
		d := retryDelay(i)
		if d < last || d > 30*time.Second {
			t.Fatal(i, d)
		}
		last = d
	}
	c := DefaultConfig("test")
	if c.Validate() != nil {
		t.Fatal("defaults")
	}
	c.Lease = time.Second
	if c.Validate() == nil {
		t.Fatal("lease shorter than send")
	}
}
