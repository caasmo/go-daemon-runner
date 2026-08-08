package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/caasmo/go-daemon-runner/daemon"
)

// ContinuousReplicaDaemon simulates continuous database replication
// — the pattern of litestream or MySQL's replication: the database
// stays replicated to a remote replica for the process lifetime.
//
// Its real-world counterpart: the daemon owns no loop of its own.
// It delegates to a context-aware replication engine — like
// litestream's store (github.com/caasmo/restinpieces-litestream),
// which streams the WAL to S3, or a MySQL replica applying the
// binlog — and, on shutdown, flushes the final replication state
// before signaling completion.
//
// Blocking pattern 3 — delegation to a context-aware call: the
// goroutine contains no blocking point of its own construction. It
// blocks on engine.Replicate, a library call that runs the
// replication loop in its own goroutine observing d.Ctx and
// returns once cancelled. The daemon's goroutine only does cleanup
// work, so Stop's wait on ShutdownDone is naturally bounded by the
// engine's shutdown.
//
// README rules: 1 (Run spawns the goroutine), 2 (the blocking point
// is the context-aware engine.Replicate call), 3 (first defer
// closes ShutdownDone), and 4 via the Stop inherited from Base.
type ContinuousReplicaDaemon struct {
	daemon.Base                // README rule 4: Stop inherited from Base, waits on ShutdownDone
	engine      *ReplicaEngine // the context-aware replication library
}

func NewContinuousReplicaDaemon(engine *ReplicaEngine, logger *slog.Logger) *ContinuousReplicaDaemon {
	d := &ContinuousReplicaDaemon{
		Base:   daemon.NewBase("ContinuousReplicaDaemon", logger),
		engine: engine,
	}
	// Every log line carries the daemon's identity: reuse the
	// daemon_name attribute the runner attaches to lifecycle logs.
	d.Logger = d.Logger.With("daemon_name", d.Name())
	// The engine is the daemon's inner loop — its log lines must
	// carry the same identity, so hand it the enriched logger.
	d.engine.Logger = d.Logger
	return d
}

func (d *ContinuousReplicaDaemon) Run() error {
	go func() { // README rule 1: Run spawns the background goroutine
		defer close(d.ShutdownDone) // README rule 3: first defer, signals completion to Stop
		defer d.doShutdownCleanup() // final flush, runs even when Replicate fails

		// Replicate is a library call: it runs the engine's loop in
		// its own goroutine, observing d.Ctx (cancelled by Stop()),
		// and returns once the loop has exited.
		err := d.engine.Replicate(d.Ctx) // README rule 2: blocking point — a context-aware library call
		if err != nil {
			d.Logger.Error("replication failed", "error", err)
			return
		}
	}()
	return nil
}

// doShutdownCleanup runs after the engine has shut down, cleanly or
// not: in a real daemon it would flush the final replication position
// or deregister the replica. It is deferred so it runs even when
// Replicate failed.
func (d *ContinuousReplicaDaemon) doShutdownCleanup() {
	d.Logger.Info("flushing final replication position")
}

// ReplicaEngine is the external replication library the daemon
// wraps — like litestream's store: it runs its own loop and knows
// how to stop on a cancelled context. It simulates streaming the
// database's WAL to a remote replica. It is not a daemon: it has
// no lifecycle of its own to manage — the daemon only delegates to
// it and waits.
type ReplicaEngine struct {
	Logger *slog.Logger
}

// Replicate runs the replication loop — streaming the WAL, applying
// it to the remote replica — until ctx is cancelled, then returns.
// It is a blocking library call: Replicate spawns the loop in its
// own goroutine and blocks on <-ctx.Done(), so it returns exactly
// when the engine has stopped — no earlier, no later.
func (e *ReplicaEngine) Replicate(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.Logger.Info("replicating WAL to remote replica")
			}
		}
	}()
	<-ctx.Done() // return once the engine has shut down
	return nil
}
