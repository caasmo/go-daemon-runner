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
	paused      atomic.Bool // reloaded in place via SIGHUP
}

func NewBackupDaemon(interval time.Duration, logger *slog.Logger) *BackupDaemon {
	return &BackupDaemon{
		Base:     daemon.NewBase("BackupDaemon", logger),
		interval: interval,
	}
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
				// paused is toggled in place by the reloadFunc wired
				// in main.go via run.WithReloadFunc (SIGHUP); a load
				// here picks it up on the next tick.
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
