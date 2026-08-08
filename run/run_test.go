// Package run tests the Runner lifecycle: sequential start, signal
// handling (SIGINT/SIGQUIT/SIGTERM/SIGHUP), and concurrent graceful
// shutdown. Tests drive the real Run with real OS signals sent to the
// test process; signal.Notify is process-global, so the tests are
// sequential (no t.Parallel).
package run

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"syscall"
	"testing"
	"time"
)

// discardLogger silences lifecycle logging in tests.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// testDaemon is a scriptable stand-in for a daemon in runner tests.
// It implements the Daemon contract (Name, Run, Stop). Two kinds of
// fields:
//
//   - Injection knobs script the behavior: daemonRunErr/stopErr set
//     the error Run/Stop return (nil = succeed), runDelay/stopDelay
//     sleep before the call reports (ordering control; stopDelay
//     larger than the runner's shutdown timeout exercises the
//     deadline-bounded shutdown).
//   - Introspection channels report lifecycle calls: runCalledChan
//     receives when Run is called, stopCalledChan when Stop is
//     called. Both are buffered (cap 1) so the daemon never blocks
//     on reporting.
//
// Guarantees that make the introspection reliable:
//
//   - One send per lifecycle call: Run and Stop each send exactly once
//     per invocation; cap 1 means the test's read of the channel is the
//     observation — nothing is dropped, nothing queues.
//   - The daemon never blocks on reporting: buffered channels, so a
//     test that never reads cannot deadlock the runner.
//   - Delays precede the send: a delayed call reports only after its
//     delay, so runDelay orders observations deterministically and
//     stopDelay makes the deadline test honest.
//   - Not concurrent-safe, by contract: the runner calls Run/Stop from
//     its own goroutines; the test only reads channels.
type testDaemon struct {
	// name is returned by Name(); it identifies the daemon in runner
	// and error output.
	name string

	// daemonRunErr is the error Run returns; nil runs successfully.
	daemonRunErr error
	// stopErr is the error Stop returns; nil stops successfully.
	stopErr error

	// runDelay sleeps before Run reports the call; it orders run
	// observations deterministically in sequential-run tests.
	runDelay time.Duration
	// stopDelay holds Stop open (or until ctx expires) before it
	// reports; larger than the runner's shutdown timeout it exercises
	// the deadline-bounded shutdown.
	stopDelay time.Duration

	// runCalledChan receives (true) each time Run is called.
	runCalledChan chan bool
	// stopCalledChan receives (true) each time Stop is called.
	stopCalledChan chan bool
}

// newTestDaemon returns a testDaemon with its introspection channels
// created and every knob at its zero value (Run succeeds, Stop
// succeeds, no delays).
func newTestDaemon(name string) *testDaemon {
	return &testDaemon{
		name:           name,
		runCalledChan:  make(chan bool, 1),
		stopCalledChan: make(chan bool, 1),
	}
}

// Name returns the daemon's constant name.
func (d *testDaemon) Name() string {
	return d.name
}

// Run waits out runDelay (if any), reports the call on
// runCalledChan, then returns daemonRunErr. A nil daemonRunErr means
// Run succeeded; a non-nil one fails and triggers rollback.
func (d *testDaemon) Run() error {
	if d.runDelay > 0 {
		time.Sleep(d.runDelay)
	}
	d.runCalledChan <- true
	return d.daemonRunErr
}

// Stop waits out stopDelay (if any) or until ctx expires — whichever
// comes first — then reports the call on stopCalledChan. A stopDelay
// larger than the runner's shutdown timeout lets ctx expire first, so
// Stop returns ctx.Err() (context.DeadlineExceeded), exercising the
// deadline-bounded shutdown. A nil stopDelay returns immediately.
func (d *testDaemon) Stop(ctx context.Context) error {
	if d.stopDelay > 0 {
		select {
		case <-time.After(d.stopDelay):
		case <-ctx.Done():
			d.stopCalledChan <- true
			return ctx.Err()
		}
	}
	d.stopCalledChan <- true
	return d.stopErr
}

// sendSignal delivers sig to the current process — the process the
// runner lives in. This is how the tests drive Run's signal loop.
//
// It works because Run() has already called signal.Notify (run.go),
// which registers the process-global signal handler: the runtime
// routes the signal to the runner's channel instead of running its
// default disposition (which would terminate the test process). So
// killing the test's own PID is the honest end-to-end path — the real
// OS signal machinery is exercised, not a stub.
//
// We are deliberately testing against process-global state. Hoisting
// signal.Notify out of Run() (e.g. taking the signal channel as a
// parameter) was considered, because it would let tests inject
// signals instead of killing the process. But graceful shutdown on
// signals is the whole point of this package — the runner owns signal
// handling as its documented feature. We do not compromise the design
// to make tests easier; the tests adapt to the real design.
//
// Because signal.Notify is process-global, signal tests cannot run in
// parallel: two tests killing the shared process would deliver each
// other's signals.
func sendSignal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	err := syscall.Kill(syscall.Getpid(), sig)
	if err != nil {
		t.Fatalf("failed to send %v: %v", sig, err)
	}
}

// TestRunner_FullLifecycle drives the whole lifecycle through the real
// Run: two daemons run, a SIGINT shuts them down, and Run returns nil.
func TestRunner_FullLifecycle(t *testing.T) {
	d1 := newTestDaemon("d1")
	d2 := newTestDaemon("d2")

	r, err := NewRunner(
		WithLogger(discardLogger),
		WithShutdownTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(d1)
	r.Add(d2)

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- r.Run()
	}()

	// Both daemons start; start is sequential, so d1 precedes d2.
	// runCalledChan firing also proves signal.Notify has already run
	// (it registers before daemons start, run.go:139-140), so the
	// SIGINT below is captured, not fatal.
	select {
	case <-d1.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not started")
	}
	select {
	case <-d2.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d2 was not started")
	}

	sendSignal(t, syscall.SIGINT)

	// Both daemons are stopped as part of graceful shutdown.
	select {
	case <-d1.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not stopped")
	}
	select {
	case <-d2.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d2 was not stopped")
	}

	// A clean signal shutdown returns nil.
	select {
	case runErr := <-runErrChan:
		if runErr != nil {
			t.Fatalf("Run returned error: %v", runErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestRunner_StartupFailureRollback proves rollback: when the second
// daemon's Run fails, the first (already running) daemon is stopped and
// Run returns that daemon's error; the failed daemon is never stopped.
func TestRunner_StartupFailureRollback(t *testing.T) {
	wantRunErr := errors.New("run failed")
	d1 := newTestDaemon("d1")
	d2 := newTestDaemon("d2")
	d2.daemonRunErr = wantRunErr

	r, err := NewRunner(
		WithLogger(discardLogger),
		WithShutdownTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(d1)
	r.Add(d2)

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- r.Run()
	}()

	// d1 starts first; then d2's Run is attempted and fails.
	select {
	case <-d1.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not started")
	}
	select {
	case <-d2.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d2 was not started")
	}

	// Rollback: d1, already started, is stopped; d2 never started so it
	// is not stopped.
	select {
	case <-d1.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not rolled back")
	}
	select {
	case <-d2.stopCalledChan:
		t.Fatal("d2 was stopped despite failing to start")
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case runErr := <-runErrChan:
		if !errors.Is(runErr, wantRunErr) {
			t.Fatalf("Run returned %v, want the daemon's Run error", runErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestRunner_SequentialStartOrder proves daemons run one after another:
// d1's Run must report before d2's, so a reversed order would time out
// on d1's channel. A SIGINT then shuts the runner down cleanly.
func TestRunner_SequentialStartOrder(t *testing.T) {
	d1 := newTestDaemon("d1")
	d2 := newTestDaemon("d2")

	r, err := NewRunner(
		WithLogger(discardLogger),
		WithShutdownTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(d1)
	r.Add(d2)

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- r.Run()
	}()

	// Start is sequential: d1's Run must report before d2's. A
	// reversed start order would time out on d1's channel.
	select {
	case <-d1.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not started first")
	}
	select {
	case <-d2.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d2 was not started second")
	}

	// Clean up: stop the runner.
	sendSignal(t, syscall.SIGINT)

	select {
	case <-d1.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not stopped")
	}
	select {
	case <-d2.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d2 was not stopped")
	}

	select {
	case runErr := <-runErrChan:
		if runErr != nil {
			t.Fatalf("Run returned error: %v", runErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestRunner_DeadlineExceeded proves the deadline-bounded shutdown: a
// daemon that stops too slowly makes the graceful-shutdown context
// expire, and Run returns an error wrapping context.DeadlineExceeded.
func TestRunner_DeadlineExceeded(t *testing.T) {
	// stopDelay (500ms) far exceeds the 200ms shutdown timeout, so the
	// graceful shutdown context expires while Stop still blocks; Stop
	// reports the deadline error.
	d1 := newTestDaemon("d1")
	d1.stopDelay = 500 * time.Millisecond

	r, err := NewRunner(
		WithLogger(discardLogger),
		WithShutdownTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(d1)

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- r.Run()
	}()

	select {
	case <-d1.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not started")
	}

	sendSignal(t, syscall.SIGINT)

	// Stop reports once the context expires (~200ms), well inside the
	// 1s test timeout.
	select {
	case <-d1.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 Stop did not report")
	}

	select {
	case runErr := <-runErrChan:
		if !errors.Is(runErr, context.DeadlineExceeded) {
			t.Fatalf("Run returned %v, want context.DeadlineExceeded", runErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestRunner_SIGHUPReload proves the reload hook: SIGHUP fires the
// configured reload function while the runner keeps running; a later
// SIGINT then shuts it down cleanly.
func TestRunner_SIGHUPReload(t *testing.T) {
	reloadCalled := make(chan bool, 1)
	reloadFunc := func() error {
		reloadCalled <- true
		return nil
	}

	d1 := newTestDaemon("d1")

	r, err := NewRunner(
		WithLogger(discardLogger),
		WithShutdownTimeout(200*time.Millisecond),
		WithReloadFunc(reloadFunc),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(d1)

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- r.Run()
	}()

	select {
	case <-d1.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not started")
	}

	// SIGHUP triggers the reload function, not a shutdown.
	sendSignal(t, syscall.SIGHUP)

	select {
	case <-reloadCalled:
	case <-time.After(1 * time.Second):
		t.Fatal("reload function was not called")
	}

	// The runner keeps running after the reload: no shutdown yet.
	select {
	case err := <-runErrChan:
		t.Fatalf("Run returned after SIGHUP: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// SIGINT for a clean shutdown.
	sendSignal(t, syscall.SIGINT)

	select {
	case <-d1.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not stopped")
	}

	select {
	case runErr := <-runErrChan:
		if runErr != nil {
			t.Fatalf("Run returned error: %v", runErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestRunner_SIGHUPNoReload proves SIGHUP without a reload function is
// ignored: the runner keeps running and no daemon is stopped; a later
// SIGINT then shuts it down cleanly.
func TestRunner_SIGHUPNoReload(t *testing.T) {
	d1 := newTestDaemon("d1")

	r, err := NewRunner(
		WithLogger(discardLogger),
		WithShutdownTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(d1)

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- r.Run()
	}()

	select {
	case <-d1.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not started")
	}

	// Without a reload function, SIGHUP is logged and ignored: the
	// runner keeps running and the daemon is not stopped.
	sendSignal(t, syscall.SIGHUP)

	select {
	case <-d1.stopCalledChan:
		t.Fatal("daemon stopped on SIGHUP without reload function")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case err := <-runErrChan:
		t.Fatalf("Run returned after SIGHUP: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// SIGINT for a clean shutdown.
	sendSignal(t, syscall.SIGINT)

	select {
	case <-d1.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not stopped")
	}

	select {
	case runErr := <-runErrChan:
		if runErr != nil {
			t.Fatalf("Run returned error: %v", runErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestNewRunner_InvalidShutdownTimeout proves NewRunner rejects a
// non-positive shutdown timeout with an error wrapping ErrRunner.
func TestNewRunner_InvalidShutdownTimeout(t *testing.T) {
	_, err := NewRunner(WithShutdownTimeout(0))
	if !errors.Is(err, ErrRunner) {
		t.Fatalf("NewRunner returned %v, want ErrRunner", err)
	}
}

// TestNewRunner_NilLogger proves a nil logger falls back to
// slog.Default() instead of being stored as nil.
func TestNewRunner_NilLogger(t *testing.T) {
	r, err := NewRunner(WithLogger(nil))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if r.logger == nil {
		t.Fatal("nil logger was not replaced with slog.Default()")
	}
}

// TestAddDaemon_Nil proves Add(nil) does not panic: it logs a warning
// and leaves the daemon list untouched.
func TestAddDaemon_Nil(t *testing.T) {
	r, err := NewRunner(WithLogger(discardLogger))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(nil) // logs a warning, must not panic
}

// TestRunner_SIGHUPReloadError proves a failing reload function is
// logged, not fatal: SIGHUP runs the reload (which returns an error),
// the runner keeps running, and a later SIGINT shuts it down cleanly.
func TestRunner_SIGHUPReloadError(t *testing.T) {
	reloadErr := errors.New("reload failed")
	reloadCalled := make(chan bool, 1)
	reloadFunc := func() error {
		reloadCalled <- true
		return reloadErr
	}

	d1 := newTestDaemon("d1")

	r, err := NewRunner(
		WithLogger(discardLogger),
		WithShutdownTimeout(200*time.Millisecond),
		WithReloadFunc(reloadFunc),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.Add(d1)

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- r.Run()
	}()

	select {
	case <-d1.runCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not started")
	}

	// The failing reload still runs: the reload function is called and
	// the runner keeps running instead of shutting down.
	sendSignal(t, syscall.SIGHUP)

	select {
	case <-reloadCalled:
	case <-time.After(1 * time.Second):
		t.Fatal("reload function was not called")
	}
	select {
	case err := <-runErrChan:
		t.Fatalf("Run returned after SIGHUP: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// SIGINT for a clean shutdown.
	sendSignal(t, syscall.SIGINT)

	select {
	case <-d1.stopCalledChan:
	case <-time.After(1 * time.Second):
		t.Fatal("d1 was not stopped")
	}

	select {
	case runErr := <-runErrChan:
		if runErr != nil {
			t.Fatalf("Run returned error: %v", runErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return")
	}
}
