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

	// === BackupDaemon: periodic database snapshot ===
	// Ticks every 2 seconds and takes a snapshot of the DB.
	//
	// SIGHUP deactivates the backup: the reload hook flips the
	// shared pause flag, which the daemon reads on every tick. A
	// second SIGHUP resumes it. Try it: kill -HUP <pid>
	backupPaused := &atomic.Bool{}
	backupDaemon := NewBackupDaemon(2*time.Second, backupPaused, logger)
	reloadFunc := func() error {
		backupPaused.Store(!backupPaused.Load()) // flip: deactivate the backup
		logger.Info("reload: backup pause flag", "paused", backupPaused.Load())
		return nil
	}

	// === QueueDaemon: background job queue ===
	// A worker consumes jobs from the daemon's buffered channel.
	// Seed a few jobs now — the channel is buffered, so main can
	// enqueue them before the worker starts inside Run.
	queueDaemon := NewQueueDaemon(logger)
	queueDaemon.jobs <- "backup report"
	queueDaemon.jobs <- "rotate logs"
	queueDaemon.jobs <- "send digest"

	// === ServiceDaemon: wraps a context-aware store ===
	// The store is the external library the daemon delegates to:
	// it syncs data to disk on its own loop and stops when the
	// daemon's context is cancelled.
	serviceDaemon := NewServiceDaemon(&Store{Logger: logger}, logger)

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
	r.Add(serviceDaemon)

	// === Run ===
	// Run blocks until SIGINT/SIGQUIT/SIGTERM, then shuts the started
	// daemons down gracefully. Run never calls os.Exit — main maps the
	// result to an exit code.
	logger.Info("example running - send SIGINT/SIGTERM to stop, SIGHUP to reload")
	if err := r.Run(); err != nil {
		logger.Error("runner exited with errors", "error", err)
		os.Exit(1)
	}
	os.Exit(0)
}
