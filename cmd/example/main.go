// Command example wires three daemons through the runner: sequential
// start, signal-driven main loop, and concurrent graceful shutdown.
// Run it with: go run ./cmd/example
package main

import (
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/caasmo/go-daemon-runner/run"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// === PeriodicBackupDaemon: periodic database snapshot ===
	// Ticks every 10 seconds and takes a snapshot of the DB.
	//
	// SIGHUP deactivates the backup: the reload hook flips the
	// shared pause flag, which the daemon reads on every tick. A
	// second SIGHUP resumes it. Try it: kill -HUP <pid>
	backupPaused := &atomic.Bool{}
	backupDaemon := NewPeriodicBackupDaemon(10*time.Second, backupPaused, logger)
	reloadFunc := func() error {
		backupPaused.Store(!backupPaused.Load()) // flip: deactivate the backup
		logger.Info("reload: backup pause flag", "paused", backupPaused.Load())
		return nil
	}

	// === QueueDaemon: background job queue ===
	// A bounded errgroup pool (SetLimit 2) processes jobs. Submit
	// a few jobs now — the buffered channel lets main enqueue
	// them before the daemon starts inside Run.
	queueDaemon := NewQueueDaemon(logger)
	queueDaemon.SubmitJob("backup report")
	queueDaemon.SubmitJob("rotate logs")
	queueDaemon.SubmitJob("send digest")

	// === ContinuousReplicaDaemon: continuous database replication ===
	// The daemon delegates to a context-aware replication engine —
	// litestream-style, streaming the WAL to a remote replica — and
	// flushes the final replication position on clean shutdown.
	replicaDaemon := NewContinuousReplicaDaemon(&ReplicaEngine{}, logger)

	// === Wire the daemons through the runner ===
	r, err := run.NewRunner(
		run.WithLogger(logger),
		run.WithShutdownTimeout(10*time.Second),
		run.WithReloadFunc(reloadFunc),
	)
	if err != nil {
		logger.Error("failed to create runner", "error", err)
		os.Exit(1)
	}

	r.Add(backupDaemon)
	r.Add(queueDaemon)
	r.Add(replicaDaemon)

	// === Run ===
	// Run blocks until SIGINT/SIGQUIT/SIGTERM, then shuts the started
	// daemons down gracefully. Run never calls os.Exit — main maps the
	// result to an exit code.
	logger.Info("example running - send SIGINT/SIGTERM to stop, SIGHUP to reload")
	err = r.Run()
	if err != nil {
		logger.Error("runner exited with errors", "error", err)
		os.Exit(1)
	}
	os.Exit(0)
}
