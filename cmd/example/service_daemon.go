package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/caasmo/go-daemon-runner/daemon"
)

// ServiceDaemon simulates a daemon that wraps a context-aware
// service — typically a library that already runs its own loop and
// knows how to stop on a cancelled context, e.g. an embedded store,
// a sync engine, or a metrics exporter.
//
// Its real-world counterpart: the daemon owns no loop of its own;
// it delegates to the service and, after a clean shutdown, flushes
// any remaining state (persisting buffers, closing files) before
// signaling completion.
//
// Blocking pattern 3 — delegation to a context-aware call: the
// goroutine contains no blocking point of its own construction. It
// blocks on store.Sync, a library call that runs its loop in its
// own goroutine observing d.Ctx and returns once cancelled. The
// daemon's goroutine only does cleanup work, so Stop's wait on
// ShutdownDone is naturally bounded by the library's shutdown.
type ServiceDaemon struct {
	daemon.Base        // README rule 4: Stop inherited from Base, waits on ShutdownDone
	store       *Store // context-aware service
}

func NewServiceDaemon(store *Store, logger *slog.Logger) *ServiceDaemon {
	return &ServiceDaemon{
		Base:  daemon.NewBase("ServiceDaemon", logger),
		store: store,
	}
}

func (d *ServiceDaemon) Run() error {
	go func() { // README rule 1: Run spawns the background goroutine
		defer close(d.ShutdownDone) // README rule 3: first defer, signals completion to Stop

		// Sync is a library call: it runs the store's loop in its
		// own goroutine, observing d.Ctx (cancelled by Stop()), and
		// returns once the loop has exited.
		if err := d.store.Sync(d.Ctx); err != nil { // README rule 2: blocking point — a context-aware library call
			d.Logger.Error("service failed", "error", err)
			return // a failed service is not flushed
		}
		d.doShutdownCleanup() // flush remaining state after a clean shutdown
	}()
	return nil
}

// doShutdownCleanup runs after the service has shut down cleanly:
// in a real daemon it would persist buffered writes or release
// resources acquired while the service was running.
func (d *ServiceDaemon) doShutdownCleanup() {
	d.Logger.Info("flushing remaining state after a clean shutdown")
}

// Store is the external library the daemon wraps — the kind of
// context-aware service you would pull in as a dependency rather
// than manage yourself: it runs its own loop and knows how to stop
// on a cancelled context. It simulates an embedded database that
// periodically syncs its data to disk. It is not a daemon: it has
// no lifecycle of its own to manage — the daemon only delegates to
// it and waits.
type Store struct {
	Logger *slog.Logger
}

// Sync runs the store's sync loop until ctx is cancelled, then
// returns. It is a blocking library call: Sync spawns the loop in
// its own goroutine and blocks on <-ctx.Done(), so it returns
// exactly when the library has stopped — no earlier, no later.
func (s *Store) Sync(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.Logger.Info("store tick: syncing data to disk")
			}
		}
	}()
	<-ctx.Done() // return once the service has shut down
	return nil
}
