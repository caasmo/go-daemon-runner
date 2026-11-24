package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/caasmo/go-daemon-runner/daemon"
)

// PeriodicBackupDaemon simulates a periodic database backup.
//
// Its real-world counterpart is a component that snapshots a database
// (or any stateful resource) on a fixed schedule: every tick it takes
// a snapshot of the DB. It implements daemon.Reloader: SIGHUP toggles
// its pause flag inside the daemon's loop, so a paused backup simply
// skips its ticks — visible in the logs as "backup skipped - paused" —
// until the next SIGHUP unpauses it.
//
// Blocking pattern 1 — a select on Ctx.Done() and a ticker: the
// goroutine listens for Stop's shutdown signal and exits when it
// arrives. The deferred close(ShutdownDone) runs after every other
// cleanup (the first defer executes last), so Stop only returns once
// the ticker is stopped and the goroutine has fully exited.
type PeriodicBackupDaemon struct {
	daemon.Base       // README rule 4: Stop inherited from Base, waits on ShutdownDone
	daemon.ReloadBase // implements daemon.Reloader; the loop receives reload requests
	interval          time.Duration
	paused            bool // owned by the loop; toggled by reload
}

func NewPeriodicBackupDaemon(interval time.Duration, logger *slog.Logger) *PeriodicBackupDaemon {
	d := &PeriodicBackupDaemon{
		Base:       daemon.NewBase("PeriodicBackupDaemon", logger),
		ReloadBase: daemon.NewReloadBase(),
		interval:   interval,
	}
	// Every log line carries the daemon's identity: reuse the
	// daemon_name attribute the runner attaches to lifecycle logs.
	d.Logger = d.Logger.With("daemon_name", d.Name())
	return d
}

func (d *PeriodicBackupDaemon) Run() error {
	go func() { // README rule 1: Run spawns the background goroutine
		defer close(d.ShutdownDone) // README rule 3: first defer, signals completion to Stop

		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()

		for {
			select {
			case <-d.Ctx.Done(): // README rule 2: listen for Stop's shutdown signal and exit
				return
			case reloadCtx := <-d.ReloadCh:
				// The reload runs here, in the daemon's own loop, so it
				// can change d.paused without a lock. Send the result
				// back even when it fails: Reload waits for it.
				reloadErr := d.reload(reloadCtx)
				d.ReloadDone <- reloadErr
			case <-ticker.C:
				if d.paused {
					d.Logger.Info("backup skipped - paused")
					continue
				}
				d.doWork()
			}
		}
	}()
	return nil
}

// reload toggles the backup pause flag. It runs inside the daemon's
// loop, so d.paused needs no lock. The context check makes the reload
// honor the runner's deadline: toggling a bool never blocks, but a
// real reload that reads a file or calls a service must return
// ctx.Err() when the deadline expires.
func (d *PeriodicBackupDaemon) reload(ctx context.Context) error {
	reloadErr := ctx.Err()
	if reloadErr != nil {
		return reloadErr
	}

	d.paused = !d.paused
	d.Logger.Info("reload: backup pause flag", "paused", d.paused)
	return nil
}

// doWork simulates taking a database snapshot: in a real daemon this
// would dump the DB, upload the dump to object storage, or similar.
func (d *PeriodicBackupDaemon) doWork() {
	d.Logger.Info("database backup: snapshot taken")
}
