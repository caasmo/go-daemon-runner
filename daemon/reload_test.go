package daemon

import (
	"context"
	"errors"
	"testing"
	"time"
)

// reloadTestDaemon is a minimal daemon that supports reload: it embeds
// both bases, runs the reload case in its loop, and records the context
// it receives.
type reloadTestDaemon struct {
	Base
	ReloadBase
	reloadErr     error
	reloadCtxChan chan context.Context
}

// newReloadTestDaemon returns a reloadTestDaemon with its reporting
// channel created and reloadErr at nil (reload succeeds).
func newReloadTestDaemon() *reloadTestDaemon {
	return &reloadTestDaemon{
		Base:          NewBase("reloadTestDaemon", discardLogger),
		ReloadBase:    NewReloadBase(),
		reloadCtxChan: make(chan context.Context, 1),
	}
}

// Run spawns the daemon's loop, which handles shutdown and reload
// requests.
func (d *reloadTestDaemon) Run() error {
	go func() {
		defer close(d.ShutdownDone)

		for {
			select {
			case <-d.Ctx.Done():
				return
			case reloadCtx := <-d.ReloadCh:
				reloadErr := d.reload(reloadCtx)
				d.ReloadDone <- reloadErr
			}
		}
	}()
	return nil
}

// reload records the caller's context and returns reloadErr.
func (d *reloadTestDaemon) reload(ctx context.Context) error {
	d.reloadCtxChan <- ctx
	return d.reloadErr
}

var (
	_ Daemon   = (*reloadTestDaemon)(nil)
	_ Reloader = (*reloadTestDaemon)(nil)
)

// TestReloadBase_NewReloadBase proves NewReloadBase creates the two
// channels with the documented buffer sizes: ReloadCh unbuffered
// (cap 0) so a send waits for the loop to read the request, and
// ReloadDone unbuffered so the loop hands the result only to a
// waiting caller.
func TestReloadBase_NewReloadBase(t *testing.T) {
	t.Parallel()
	b := NewReloadBase()

	if b.ReloadCh == nil {
		t.Fatal("ReloadCh is nil")
	}
	if cap(b.ReloadCh) != 0 {
		t.Fatalf("cap(ReloadCh) = %d, want 0", cap(b.ReloadCh))
	}
	if b.ReloadDone == nil {
		t.Fatal("ReloadDone is nil")
	}
	if cap(b.ReloadDone) != 0 {
		t.Fatalf("cap(ReloadDone) = %d, want 0", cap(b.ReloadDone))
	}
}

// TestReloadBase_ReloadContextCancelled proves Reload returns the
// caller's context error when the loop is not reading: ReloadCh is
// unbuffered, so the send cannot proceed and the context deadline
// ends the wait.
func TestReloadBase_ReloadContextCancelled(t *testing.T) {
	t.Parallel()
	b := NewReloadBase()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	reloadErrChan := make(chan error, 1)
	go func() {
		reloadErrChan <- b.Reload(ctx)
	}()

	select {
	case err := <-reloadErrChan:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Reload returned %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Reload did not return for a context deadline")
	}
}

// TestReloadBase_ReloadContextAlreadyCancelled proves Reload checks
// the context before sending: an already-canceled context returns its
// error and leaves ReloadCh empty.
func TestReloadBase_ReloadContextAlreadyCancelled(t *testing.T) {
	t.Parallel()
	b := NewReloadBase()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reloadErrChan := make(chan error, 1)
	go func() {
		reloadErrChan <- b.Reload(ctx)
	}()

	select {
	case err := <-reloadErrChan:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Reload returned %v, want context.Canceled", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Reload did not return for a canceled context")
	}

	select {
	case <-b.ReloadCh:
		t.Fatal("Reload sent a request with a canceled context")
	default:
	}
}

// TestReloadBase_ReloadHandoff proves Reload sends the caller's
// context to the daemon's loop and returns the loop's result.
func TestReloadBase_ReloadHandoff(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("reload failed")
	d := newReloadTestDaemon()
	d.reloadErr = wantErr

	if err := d.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reloadErrChan := make(chan error, 1)
	go func() {
		reloadErrChan <- d.Reload(ctx)
	}()

	select {
	case err := <-reloadErrChan:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Reload returned %v, want %v", err, wantErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Reload did not return")
	}

	select {
	case receivedCtx := <-d.reloadCtxChan:
		if receivedCtx != ctx {
			t.Fatal("the loop did not receive the caller's context")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("the loop did not perform the reload")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
	defer cancelStop()
	if err := d.Stop(stopCtx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}
