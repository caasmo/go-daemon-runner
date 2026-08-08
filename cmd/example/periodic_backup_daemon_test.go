package main

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// TestPeriodicBackupDaemon_Lifecycle proves the backup daemon runs and
// stops gracefully; a one-hour interval keeps any tick out of the test.
func TestPeriodicBackupDaemon_Lifecycle(t *testing.T) {
	d := NewPeriodicBackupDaemon(time.Hour, &atomic.Bool{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := d.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}
