// Package daemon defines the contract and base lifecycle for background
// components managed by a runner.
package daemon

import (
	"context"
	"log/slog"
)

// Daemon defines the contract for background components managed
// by the runner's lifecycle (Run/Stop).
type Daemon interface {
	// Name returns a constant identifier for logging.
	Name() string
	// Run executes the daemon's main loop. It may spawn a background
	// goroutine and return immediately, or block until startup is
	// confirmed; either way it returns an error if startup fails.
	// The daemon keeps running until Stop is called. The goroutine
	// must signal completion of shutdown so that Stop can observe it.
	Run() error
	// Stop signals the daemon to shut down. The provided context
	// carries the graceful shutdown deadline.
	Stop(ctx context.Context) error
}

// Base provides the shared lifecycle fields and methods for daemons.
// Concrete daemons embed Base and implement Run. Name and Stop are
// promoted from Base. The background goroutine spawned by Run must
// register defer close(ShutdownDone) as its first defer, so it runs
// after any other cleanup and signals completion to Stop.
// Do not close ShutdownDone or reassign Cancel from outside the
// daemon's own goroutine.
// Embed Base by value and use the daemon through a pointer: the
// promoted Name and Stop have pointer receivers, so a value daemon
// does not satisfy the Daemon interface.
type Base struct {
	// name is the constant identifier for this daemon instance.
	name   string
	Logger *slog.Logger

	// Ctx is cancelled by Stop to signal the background goroutine to exit.
	Ctx    context.Context
	Cancel context.CancelFunc

	// ShutdownDone is closed by the background goroutine to signal
	// completion. Stop waits on this channel.
	ShutdownDone chan struct{}
}

// NewBase creates a Base with an independent context (no parent),
// a shutdownDone channel, and the provided name and logger.
// A nil logger falls back to slog.Default(); Base.Stop logs through it.
func NewBase(name string, logger *slog.Logger) Base {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = slog.Default()
	}
	return Base{
		name:         name,
		Logger:       logger,
		Ctx:          ctx,
		Cancel:       cancel,
		ShutdownDone: make(chan struct{}),
	}
}

// Name returns the constant name of this daemon.
func (b *Base) Name() string {
	return b.name
}

// Stop signals the daemon's background goroutine to exit and waits
// for it to finish or for the caller's context to expire.
func (b *Base) Stop(ctx context.Context) error {
	b.Logger.Info("stopping daemon", "daemon_name", b.name)
	b.Cancel()

	select {
	case <-b.ShutdownDone:
		b.Logger.Info("daemon stopped gracefully", "daemon_name", b.name)
		return nil
	case <-ctx.Done():
		b.Logger.Error("daemon shutdown timed out", "daemon_name", b.name, "error", ctx.Err())
		return ctx.Err()
	}
}
