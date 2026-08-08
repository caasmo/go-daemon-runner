// Command example wires three daemons through the runner: sequential
// start, signal-driven main loop, and concurrent graceful shutdown.
// Run it with: go run ./cmd/example
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/caasmo/go-daemon-runner/run"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// --- Build the daemons (one file per daemon, each documenting
	// its simulated workload and blocking pattern) ---
	backupDaemon := NewBackupDaemon(2*time.Second, logger)
	queueDaemon := NewQueueDaemon(logger)
	serviceDaemon := NewServiceDaemon(&Store{Logger: logger}, logger)

	// Seed a few jobs; the buffered channel lets main enqueue them
	// before the worker starts inside Run.
	queueDaemon.jobs <- "backup report"
	queueDaemon.jobs <- "rotate logs"
	queueDaemon.jobs <- "send digest"

	// The reload hook: a closure that captures the daemon and rebuilds
	// its state in place — the new state takes effect on the next tick.
	// Try it while the example runs: kill -HUP <pid>
	reloadFunc := func() error {
		paused := !backupDaemon.paused.Load()
		backupDaemon.paused.Store(paused)
		logger.Info("reload: toggled backup pause flag", "paused", paused)
		return nil
	}

	// --- Wire the daemons through the runner ---
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
