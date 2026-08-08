// Package daemon tests the daemon contract helpers: NewBase and Base.
// No signals are involved, so all tests may run in parallel.
package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// discardLogger silences lifecycle logging in tests.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// TestBase_NewBase proves NewBase initializes every field: the name is
// stored, and Ctx, Cancel, ShutdownDone, and Logger are non-nil.
func TestBase_NewBase(t *testing.T) {
	t.Parallel()
	b := NewBase("d1", discardLogger)

	if b.name != "d1" {
		t.Fatalf("name = %q, want %q", b.name, "d1")
	}
	if b.Ctx == nil {
		t.Fatal("Ctx is nil")
	}
	if b.Cancel == nil {
		t.Fatal("Cancel is nil")
	}
	if b.ShutdownDone == nil {
		t.Fatal("ShutdownDone is nil")
	}
	if b.Logger != discardLogger {
		t.Fatal("Logger is not the provided logger")
	}
}

// TestBase_Name proves Name returns the name passed to NewBase.
func TestBase_Name(t *testing.T) {
	t.Parallel()
	b := NewBase("d1", discardLogger)

	if got := b.Name(); got != "d1" {
		t.Fatalf("Name() = %q, want %q", got, "d1")
	}
}

// TestBase_StopGraceful proves Stop returns nil when the daemon's
// goroutine closes ShutdownDone after the context is cancelled — the
// canonical daemon shutdown path.
func TestBase_StopGraceful(t *testing.T) {
	t.Parallel()
	b := NewBase("d1", discardLogger)

	go func() {
		defer close(b.ShutdownDone)
		<-b.Ctx.Done() // unblocked by Stop's Cancel
	}()

	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

// TestBase_StopCancelsContext proves Stop invokes Cancel: the daemon's
// goroutine observes Ctx.Done() and reports it, then Stop returns nil.
func TestBase_StopCancelsContext(t *testing.T) {
	t.Parallel()
	b := NewBase("d1", discardLogger)

	cancelled := make(chan bool, 1)
	go func() {
		defer close(b.ShutdownDone)
		<-b.Ctx.Done() // returns only when Stop cancels Ctx
		cancelled <- true
	}()

	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(1 * time.Second):
		t.Fatal("Ctx was not cancelled by Stop")
	}
}

// TestBase_StopDeadlineExceeded proves the deadline path: when the
// daemon never closes ShutdownDone, Stop cancels Ctx and returns
// context.DeadlineExceeded once the caller's context expires.
func TestBase_StopDeadlineExceeded(t *testing.T) {
	t.Parallel()
	b := NewBase("d1", discardLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := b.Stop(ctx) // ShutdownDone is never closed

	select {
	case <-b.Ctx.Done():
	case <-time.After(1 * time.Second):
		t.Fatal("Ctx was not cancelled by Stop")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop returned %v, want context.DeadlineExceeded", err)
	}
}

// TestNewBase_NilLogger proves a nil logger falls back to slog.Default()
// instead of being stored as nil.
func TestNewBase_NilLogger(t *testing.T) {
	t.Parallel()
	b := NewBase("d1", nil)

	if b.Logger == nil {
		t.Fatal("nil logger was not replaced with slog.Default()")
	}
}
