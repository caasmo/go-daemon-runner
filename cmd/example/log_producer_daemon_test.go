package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestLogProducerDaemon_Lifecycle proves the producer daemon runs and
// stops gracefully; a one-hour interval keeps any tick out of the test.
func TestLogProducerDaemon_Lifecycle(t *testing.T) {
	messages := make(chan string, 64)
	d := NewLogProducerDaemon(messages, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := d.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}
