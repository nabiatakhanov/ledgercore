package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCloseWithinReturnsWhenCloserIsBlocked(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- closeWithin(ctx, func() {
			close(started)
			<-release
			close(closed)
		})
	}()
	// Observe the blocked closer itself before canceling; no timing race or
	// arbitrary sleep decides whether cancellation interrupts the wait.
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("closer did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want canceled shutdown, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked closer ignored shutdown cancellation")
	}
	close(release)
	<-closed
}

func TestCloseWithinHonorsExpiredBudget(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	release := make(chan struct{})
	finished := make(chan struct{})
	err := closeWithin(ctx, func() { <-release; close(finished) })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired budget was reset: %v", err)
	}
	close(release)
	<-finished
}

func TestCloseWithinFinishesBeforeBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	called := false
	if err := closeWithin(ctx, func() { called = true }); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("closer was not called")
	}
}
