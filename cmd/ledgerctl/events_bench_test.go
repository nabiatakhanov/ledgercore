package main

import "testing"

func TestEventLatencyNearestRank(t *testing.T) {
	d := latencyDistribution([]float64{100, 1, 2})
	if d.Count != 3 || d.P50 != 2 || d.P95 != 100 || d.P99 != 100 || d.Max != 100 {
		t.Fatalf("wrong nearest-rank report: %+v", d)
	}
	if empty := latencyDistribution(nil); empty.Count != 0 {
		t.Fatal(empty)
	}
}
