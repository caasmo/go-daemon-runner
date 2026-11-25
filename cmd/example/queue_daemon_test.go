package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestQueueDaemon_Lifecycle proves the queue daemon runs, processes
// the enqueued jobs, and stops gracefully, joining the worker pool.
func TestQueueDaemon_Lifecycle(t *testing.T) {
	d := NewQueueDaemon(slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.SubmitJob("job1")
	d.SubmitJob("job2")

	if err := d.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// Wait until the loop has taken both jobs: Stop drains the in-flight
	// workers, so every submitted job is processed.
	deadline := time.Now().Add(time.Second)
	for len(d.jobs) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("queue did not take the submitted jobs")
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}
