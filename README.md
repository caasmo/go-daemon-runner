# go-daemon-runner

[![Go Reference](https://pkg.go.dev/badge/github.com/caasmo/go-daemon-runner)](https://pkg.go.dev/github.com/caasmo/go-daemon-runner)

Add and control the lifecycle of goroutine-based daemons.

## Installation

```
go get github.com/caasmo/go-daemon-runner
```

## The two packages

- `daemon` (`github.com/caasmo/go-daemon-runner/daemon`) — the [`Daemon`](daemon/daemon.go) interface and the [`Base`](daemon/daemon.go) helper struct for building daemons. Stdlib only.
- `run` (`github.com/caasmo/go-daemon-runner/run`) — the [`Runner`](run/run.go) dispatcher: sequential start, signal handling, and concurrent graceful shutdown.

Usage pattern: `daemon` to build daemons, `run` to run them.

## Usage

```go
r, err := run.NewRunner()
if err != nil {
	panic(err)
}
r.Add(backupDaemon) // any daemon.Daemon
r.Add(schedulerDaemon)
if err := r.Run(); err != nil {
	panic(err) // startup failed or shutdown had errors
}
```

## Writing daemons

A daemon is an object that satisfies the `Daemon` interface:

```go
type Daemon interface {
	Name() string
	Run() error
	Stop(ctx context.Context) error
}
```

The interface alone is not enough. The runner calls [`Run`](daemon/daemon.go) on each daemon at startup, sequentially, and [`Stop`](daemon/daemon.go) at shutdown — `ctx` carries the graceful-shutdown deadline. Your object must additionally follow these rules:

- `Run` — called by the runner to start the daemon. It spawns the background goroutine that does the daemon's work and returns an error if startup fails. It may return `nil` immediately once the goroutine is running, or block until startup is confirmed.
- The goroutine must reach a blocking point that unblocks when shutdown is signaled.
- `Stop` — called by the runner during shutdown. It must signal the daemon to shut down and wait until the goroutine signals completion, or until the context deadline expires, whichever comes first.

`Base` implements one of these rules and provides the plumbing for the other two. It implements `Stop` — cancelling [`Ctx`](daemon/daemon.go) and waiting on [`ShutdownDone`](daemon/daemon.go) or the deadline — and provides the fields your `Run` needs to follow the rest:

- [`Ctx`](daemon/daemon.go) — cancelled by `Stop` to signal the goroutine to exit.
- [`Cancel`](daemon/daemon.go) — the cancel function `Stop` calls.
- [`ShutdownDone`](daemon/daemon.go) — the goroutine closes it to signal completion; `Stop` waits on it.
- [`Logger`](daemon/daemon.go) — used by `Stop` for lifecycle logging.

When using `Base`, the goroutine spawned by `Run` registers `defer close(ShutdownDone)` as its **first** defer — defers run last-in-first-out, so it executes after every other deferred cleanup and `Stop` unblocks only once all cleanup has completed. It blocks on `Ctx` being cancelled (triggered by `Stop` via `Cancel`) — a select on `Ctx.Done()`, a bare `<-Ctx.Done()`, or a context-aware library call.

`Base` alone does not satisfy the interface: it has no `Run`. The interface can also be implemented directly without `Base`.
```go
type BackupDaemon struct {
	daemon.Base
	interval time.Duration // how often to run a backup
}

func NewBackupDaemon(interval time.Duration, logger *slog.Logger) *BackupDaemon {
	return &BackupDaemon{
		Base:     daemon.NewBase("BackupDaemon", logger),
		interval: interval,
	}
}

func (d *BackupDaemon) Run() error {
	go func() {
		defer close(d.ShutdownDone) // must be present to signal shutdown to Stop()

		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()

		for {
			select {
			case <-d.Ctx.Done(): // cancelled by Stop(), exit the loop
				return
			case <-ticker.C:
				d.doWork()
			}
		}
	}()
	return nil
}
```

Real daemons following these rules: restinpieces' [log daemon](https://github.com/caasmo/restinpieces/blob/master/log/daemon.go), [scheduler](https://github.com/caasmo/restinpieces/blob/master/queue/scheduler/scheduler.go), and the [litestream daemon](https://github.com/caasmo/restinpieces-litestream/blob/master/litestream.go).

## Runner options

`NewRunner` applies defaults — `slog.Default()` logger, 15-second graceful-shutdown timeout, SIGHUP ignored — overridable with:

- `WithLogger(l *slog.Logger)`
- `WithShutdownTimeout(d time.Duration)`
- `WithReloadFunc(fn func() error)`

A non-positive shutdown timeout makes `NewRunner` return an error wrapping `ErrRunner`, checkable with `errors.Is`. The shutdown phase caps concurrent `Stop` calls at 32 (a fixed internal constant, not an option).

## Signals

| Signal | Action |
| --- | --- |
| SIGINT, SIGQUIT, SIGTERM | Graceful shutdown of all daemons |
| SIGHUP | Reload hook (next section); ignored if none configured |

Under systemd: `systemctl stop` sends SIGTERM, `systemctl reload` sends SIGHUP via `ExecReload=/bin/kill -HUP $MAINPID`.

## The reload hook

SIGHUP does not shut the runner down — it runs the `reloadFunc` set via `WithReloadFunc` and the signal loop keeps running. The systemd flow:

`systemctl reload` → `ExecReload=/bin/kill -HUP $MAINPID` → SIGHUP → `reloadFunc`

`reloadFunc` is a closure that captures whatever state the reload needs — the daemon itself, or the config provider the daemon reads from — and rebuilds that state in place. The daemon instance is never swapped: the runner's daemon list is fixed, and the rebuilt state takes effect inside the running daemon's next iteration.

The runner never interprets the reload error — it logs it and keeps running; a failed reload must not take the process down. Keep `reloadFunc` fast: it runs synchronously inside the signal loop, and termination signals arriving while it blocks can be dropped.

The real restinpieces path is `handleSIGHUP` → `reloadFunc` → `config.Reload` ([reload.go](https://github.com/caasmo/restinpieces/blob/master/config/reload.go)), wired up with [restinpieces.service](https://github.com/caasmo/restinpieces/blob/master/restinpieces.service). Reduced to the essential steps, with a scheduler daemon that reads the config provider on every tick:

```go
reloadFunc := func() error {
	bytes, _, err := store.Get(scopeApplication, 0)
	if err != nil {
		return fmt.Errorf("failed to fetch latest config: %w", err)
	}
	newCfg := &Config{}
	if err := toml.Unmarshal(bytes, newCfg); err != nil {
		return fmt.Errorf("failed to unmarshal new config: %w", err)
	}
	if err := newCfg.Validate(); err != nil {
		return fmt.Errorf("new config validation failed: %w", err)
	}
	provider.Update(newCfg) // in-place rebuild — takes effect next tick
	return nil
}

r, err := run.NewRunner(run.WithReloadFunc(reloadFunc))
if err != nil {
	panic(err)
}
r.Add(schedulerDaemon)
if err := r.Run(); err != nil {
	panic(err)
}
```
