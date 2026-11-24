package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestPeriodicBackupDaemon_Lifecycle proves the backup daemon runs and
// stops gracefully; a one-hour interval keeps any tick out of the test.
func TestPeriodicBackupDaemon_Lifecycle(t *testing.T) {
	d := NewPeriodicBackupDaemon(time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := d.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

// TestPeriodicBackupDaemon_Reload proves reload toggles the pause flag
// in place and returns the context error when the deadline expires.
func TestPeriodicBackupDaemon_Reload(t *testing.T) {
	d := NewPeriodicBackupDaemon(time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := d.reload(context.Background()); err != nil {
		t.Fatalf("reload returned error: %v", err)
	}
	if !d.paused {
		t.Fatal("reload did not pause the backup")
	}

	if err := d.reload(context.Background()); err != nil {
		t.Fatalf("reload returned error: %v", err)
	}
	if d.paused {
		t.Fatal("reload did not resume the backup")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.reload(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reload returned %v, want context.Canceled", err)
	}
}
