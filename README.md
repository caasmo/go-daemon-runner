# go-daemon-runner

Add and control the lifecycle of goroutine-based daemons.

The module has no root package — import the two subpackages directly:

- `daemon` (`github.com/caasmo/go-daemon-runner/daemon`) — the `Daemon`
  interface and the `Base` helper struct for building daemons. Stdlib only.
- `run` (`github.com/caasmo/go-daemon-runner/run`) — the `Runner` dispatcher:
  sequential start, SIGINT/SIGQUIT/SIGTERM/SIGHUP handling, and concurrent graceful
  shutdown with a shared deadline.

Usage pattern: `daemon` to build daemons, `run` to run them. The hyphenated
repo name intentionally does not match the single-word package names.

`Run` never calls `os.Exit` — it returns an error the caller maps to an
exit code:

```go
r, err := run.NewRunner()
if err != nil {
	os.Exit(1) // invalid option value, e.g. non-positive shutdown timeout
}
r.Add(myDaemon)
if err := r.Run(); err != nil {
	os.Exit(1) // startup failed or shutdown had errors
}
os.Exit(0)
```

`NewRunner` applies defaults — a `slog.Default()` logger, a 15-second
graceful-shutdown timeout, and SIGHUP ignored — overridable with
`WithLogger`, `WithShutdownTimeout`, and `WithReloadFunc` (next section).
The shutdown phase additionally caps concurrent `Stop` calls at 32 (a
fixed internal constant, not an option) to prevent a closing storm with
many daemons. It rejects invalid option values that have no safe default
(a nil logger still falls back to `slog.Default()`): a non-positive
shutdown timeout returns `ErrRunner`, checkable with `errors.Is`.

## The reload hook: `reloadFunc` on SIGHUP

SIGHUP does not shut the runner down — it calls the `reloadFunc` set via
`WithReloadFunc` (absent in the usage pattern above, so SIGHUP is logged
and ignored) and the signal loop keeps running. The systemd flow:

`systemctl reload` → `ExecReload=/bin/kill -HUP $MAINPID` → SIGHUP →
`reloadFunc`

`reloadFunc` is a closure that captures whatever state the reload needs —
the daemon itself, or the config provider the daemon reads from — and
rebuilds that state **in place**. The daemon instance is never swapped:
the runner's daemon list is fixed, and the rebuilt state takes effect
inside the running daemon's next iteration.

Omitting `WithReloadFunc` ignores SIGHUP: the runner logs that no reload
function is configured and keeps running. The runner never interprets the
reload error — it logs it and keeps running; a failed reload must not
take the process down. Keep `reloadFunc` fast: it runs synchronously
inside the signal loop, and termination signals (SIGINT/SIGTERM)
arriving while it blocks can be dropped — the process then cannot be
stopped gracefully until it returns.

The real restinpieces path is `handleSIGHUP` → `reloadFunc` →
`config.Reload`
([reload.go](https://github.com/caasmo/restinpieces/blob/master/config/reload.go)),
wired up with
[restinpieces.service](https://github.com/caasmo/restinpieces/blob/master/restinpieces.service)
(`ExecReload=/bin/kill -HUP $MAINPID`). Reduced to the essential steps,
with a scheduler daemon that reads the config provider on every tick:

```go
// The closure captures the provider and the store; the scheduler daemon
// reads the provider each tick, so an in-place update is enough.
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
	os.Exit(1)
}
r.Add(schedulerDaemon)
if err := r.Run(); err != nil {
	os.Exit(1)
}
os.Exit(0)
```

## `daemon.Base` is a helper, not a `Daemon`

`Base` provides the shared lifecycle plumbing — `Name`, `Stop`, and the
`Ctx`/`Cancel`/`ShutdownDone` fields — but it does **not** implement the
`Daemon` interface: it has no `Run`. A concrete daemon embeds `Base` and
implements `Run` itself.

## The daemon struct and its `Run`

A daemon is a struct embedding `daemon.Base`, plus its own fields, with a
`Run` method. `Run` must spawn a background goroutine and report
startup failures via its returned error. It may return `nil` immediately
once the goroutine is running (scheduler, log daemon) — or it may block
until startup is confirmed, as the litestream daemon does (its `Run`
waits on a startup-confirmation channel and returns the startup result
while the goroutine keeps running). Either way, the goroutine must
`defer close(ShutdownDone)` as its last act — `Stop` waits on this
channel; if it is never closed, `Stop` blocks until the caller's
graceful-shutdown deadline expires.

`defer close(ShutdownDone)` must be the **first** `defer` registered in
the goroutine. Defers run last-in-first-out, so it executes after every
other deferred cleanup (closing monitors, stores, channels) — `Stop`
unblocks only once all cleanup has completed. There is no reason to
register it anywhere else.

The name passed to `NewBase` is the constant `Name()` returns; `Base`
is embedded by value.

Each blocking pattern below is a complete, self-contained daemon: its
own struct, constructor, `Run`, and `doWork` payload.

## The blocking principle

The goroutine must reach a blocking point that unblocks when the context
is cancelled. Three ways to satisfy it:

### 1. Select with a `ctx.Done()` case (e.g. a ticker daemon)

Use when the goroutine waits on several channels at once — its own work
and the cancellation. Real daemons: the log daemon's `processLogs`
([log/daemon.go](https://github.com/caasmo/restinpieces/blob/master/log/daemon.go))
and the scheduler's ticker loop
([queue/scheduler/scheduler.go](https://github.com/caasmo/restinpieces/blob/master/queue/scheduler/scheduler.go)).

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

func (d *BackupDaemon) doWork() {
	// run a backup
}
```

### 2. Bare receive on `ctx.Done()`

Use when the goroutine has nothing else to wait on — a coordinator that
spawns its workers (each worker observes the context itself and defers
`wg.Done()`), then waits for shutdown and joins the workers before
signaling completion. Real daemon: the litestream daemon's shutdown wait
([litestream.go](https://github.com/caasmo/restinpieces-litestream/blob/master/litestream.go)).

```go
type QueueDaemon struct {
	daemon.Base
	jobs chan Job // incoming jobs
	wg   sync.WaitGroup // joins the worker before shutdown completes
}

func NewQueueDaemon(jobs chan Job, logger *slog.Logger) *QueueDaemon {
	return &QueueDaemon{
		Base: daemon.NewBase("QueueDaemon", logger),
		jobs: jobs,
	}
}

func (d *QueueDaemon) Run() error {
	go func() {
		defer close(d.ShutdownDone) // must be present to signal shutdown to Stop()

		d.wg.Add(1)
		go d.worker() // the worker selects on d.Ctx.Done() itself
		<-d.Ctx.Done() // cancelled by Stop(), nothing else to do
		d.wg.Wait() // Stop() unblocks only once the worker has finished
	}()
	return nil
}

func (d *QueueDaemon) worker() {
	defer d.wg.Done() // lets the parent goroutine's Wait() return when the worker exits

	for {
		select {
		case <-d.Ctx.Done(): // cancelled by Stop(), exit the loop
			return
		case job := <-d.jobs:
			d.doWork(job)
		}
	}
}

func (d *QueueDaemon) doWork(job Job) {
	// process one job
}
```

### 3. Delegate to a context-aware call

Use when the work is handled by a library or service that receives the
context and observes the cancellation itself — the daemon goroutine
contains no blocking point of its own construction; it simply blocks on
a context-aware call that returns once the service has shut down. Real
daemon: the litestream daemon, whose `store.Open(l.ctx)` and directory
monitors run their loops in library goroutines, all observing `l.ctx`
([litestream.go](https://github.com/caasmo/restinpieces-litestream/blob/master/litestream.go)).
The real litestream daemon is a hybrid: after opening the store and
starting the monitors it ends with the bare `<-l.ctx.Done()` wait of
pattern 2.

```go
type ServiceDaemon struct {
	daemon.Base
	store *Store // context-aware service
}

func NewServiceDaemon(store *Store, logger *slog.Logger) *ServiceDaemon {
	return &ServiceDaemon{
		Base:  daemon.NewBase("ServiceDaemon", logger),
		store: store,
	}
}

func (d *ServiceDaemon) Run() error {
	go func() {
		defer close(d.ShutdownDone) // must be present to signal shutdown to Stop()

		// Run runs its loops in its own goroutines, all observing
		// d.Ctx (cancelled by Stop()), and returns when it is done.
		if err := d.store.Run(d.Ctx); err != nil {
			d.Logger.Error("service failed", "error", err)
			return // a failed service is not flushed
		}
		d.doWork() // flush remaining state after a clean shutdown
	}()
	return nil
}

func (d *ServiceDaemon) doWork() {
	// flush remaining state once the service has stopped
}
```
