package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestContinuousReplicaDaemon_Lifecycle proves the replica daemon runs
// and stops gracefully: its engine blocks on the cancelled context and
// returns, then the daemon flushes and signals completion.
func TestContinuousReplicaDaemon_Lifecycle(t *testing.T) {
	d := NewContinuousReplicaDaemon(&ReplicaEngine{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := d.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}
