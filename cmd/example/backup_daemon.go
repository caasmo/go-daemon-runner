package main

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/caasmo/go-daemon-runner/daemon"
)

// BackupDaemon simulates a periodic database backup.
//
// Its real-world counterpart is a component that snapshots a database
// (or any stateful resource) on a fixed schedule: every tick it takes
// a snapshot of the DB. The SIGHUP reload hook toggles a pause flag
// in place, so a paused backup simply skips its ticks — visible in
// the logs as "backup skipped - paused" — until the next SIGHUP
// unpauses it.
//
// Blocking pattern 1 — a select on Ctx.Done() and a ticker: the
// goroutine's only blocking point is the select, and the Ctx.Done()
// case unblocks it when Stop cancels the context. The deferred
// close(ShutdownDone) runs after every other cleanup (the first
// defer executes last), so Stop only returns once the ticker is
// stopped and the goroutine has fully exited.
type BackupDaemon struct {
	daemon.Base // README rule 4: Stop inherited from Base, waits on ShutdownDone
	interval    time.Duration
	paused      *atomic.Bool // shared with the reload hook in main.go; read on every tick
}

func NewBackupDaemon(interval time.Duration, paused *atomic.Bool, logger *slog.Logger) *BackupDaemon {
	d := &BackupDaemon{
		Base:     daemon.NewBase("BackupDaemon", logger),
		interval: interval,
		paused:   paused,
	}
	// Every log line carries the daemon's identity: reuse the
	// daemon_name attribute the runner attaches to lifecycle logs.
	d.Logger = d.Logger.With("daemon_name", d.Name())
	return d
}

func (d *BackupDaemon) Run() error {
	go func() { // README rule 1: Run spawns the background goroutine
		defer close(d.ShutdownDone) // README rule 3: first defer, signals completion to Stop

		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()

		for {
			select {
			case <-d.Ctx.Done(): // README rule 2: blocking point; Stop cancels Ctx to unblock it
				return
			case <-ticker.C:
				// The reload hook in main.go flips the shared flag
				// on SIGHUP; a load here picks it up next tick.
				if d.paused.Load() {
					d.Logger.Info("backup skipped - paused")
					continue
				}
				d.doWork()
			}
		}
	}()
	return nil
}

// doWork simulates taking a database snapshot: in a real daemon this
// would dump the DB, upload the dump to object storage, or similar.
func (d *BackupDaemon) doWork() {
	d.Logger.Info("database backup: snapshot taken")
}
