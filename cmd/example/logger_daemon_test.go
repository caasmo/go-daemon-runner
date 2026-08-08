package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestLoggerDaemon_Lifecycle proves the consumer daemon runs and stops
// gracefully: a message enqueued before Run is drained on shutdown.
func TestLoggerDaemon_Lifecycle(t *testing.T) {
	d := NewLoggerDaemon(slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.Chan() <- "hello" // enqueued before Run; drained on shutdown

	if err := d.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}
