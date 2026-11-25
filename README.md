# go-daemon-runner

[![Go Reference](https://pkg.go.dev/badge/github.com/caasmo/go-daemon-runner)](https://pkg.go.dev/github.com/caasmo/go-daemon-runner)
[![Test](https://github.com/caasmo/go-daemon-runner/actions/workflows/test.yml/badge.svg)](https://github.com/caasmo/go-daemon-runner/actions/workflows/test.yml)
[![golangci-lint](https://github.com/caasmo/go-daemon-runner/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/caasmo/go-daemon-runner/actions/workflows/golangci-lint.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/caasmo/go-daemon-runner/master/.github/badges/coverage.json)](https://github.com/caasmo/go-daemon-runner/actions/workflows/test.yml)
[![sloc](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/caasmo/go-daemon-runner/master/.github/badges/sloc.json)](https://github.com/caasmo/go-daemon-runner/actions/workflows/sloc.yml)
[![deps](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/caasmo/go-daemon-runner/master/.github/badges/deps.json)](https://github.com/caasmo/go-daemon-runner/actions/workflows/dependencies.yml)
[![GitHub Release](https://img.shields.io/github/v/release/caasmo/go-daemon-runner?style=flat)]()
[![Built Go](https://img.shields.io/badge/built_with-Go-00ADD8.svg?style=flat)]()

Compose your program from background goroutines — go-daemon-runner provides the process lifecycle around them: sequential start, signal handling, and concurrent, deadline-bounded graceful shutdown.

go-daemon-runner is for Go programs built from background goroutines. Your program is a set of components that run for its lifetime — periodic backups, schedulers, log aggregation, replication — each one a goroutine with a start/stop contract. The runner starts them sequentially in registration order, waits for SIGINT/SIGQUIT/SIGTERM, shuts them down concurrently within a configurable deadline (default 15s), and reloads the daemons that support it on SIGHUP — the process keeps running, no restart.

# Content

- [Features](#features)
- [Installation](#installation)
- [The two packages](#the-two-packages)
- [Runnable example](#runnable-example)
- [Writing daemons](#writing-daemons)
  - [Rules](#rules)
  - [Example](#example)
  - [Rule helper: `Base`](#rule-helper-base)
- [Wiring daemons](#wiring-daemons)
  - [Options](#options)
  - [Signals](#signals)
  - [Reloading daemons](#reloading-daemons)
- [Communicating daemons](#communicating-daemons)

## Features

- **Graceful shutdown on termination signals.** On SIGINT, SIGQUIT, or SIGTERM the runner stops every started daemon concurrently and waits for each to complete before `Run` returns. No daemon is left running, no goroutine leaked, no process left hanging.
- **Deadline-bounded shutdown.** Shutdown is bounded by a configurable deadline (default 15s, `WithShutdownTimeout`). A daemon that fails to stop in time is reported — `Run` returns its error — instead of being waited on indefinitely.
- **Rollback on startup failure.** Daemons start sequentially in registration order. If one fails to start, the runner stops the daemons already running and `Run` returns the startup error: the process never continues half-started.
- **Reload on SIGHUP without restart.** SIGHUP asks every daemon that implements `daemon.Reloader` to reload while all daemons keep running — `systemctl reload` works. Daemons without reload support are skipped, and the process keeps running.
- **Errors surfaced to the caller.** `Run` returns startup and shutdown errors (joined), so the caller can map them to exit codes instead of guessing from logs.

## Installation

```
go get github.com/caasmo/go-daemon-runner
```

## The two packages

- `daemon` (`github.com/caasmo/go-daemon-runner/daemon`) — the [`Daemon`](daemon/daemon.go) interface and the [`Base`](daemon/daemon.go) and [`ReloadBase`](daemon/reload.go) helper structs for building daemons. Stdlib only.
- `run` (`github.com/caasmo/go-daemon-runner/run`) — the [`Runner`](run/run.go) dispatcher: sequential start, signal handling, and concurrent graceful shutdown.

Usage pattern: `daemon` to build daemons, `run` to run them.

## Runnable example

A complete runnable example of the library — daemons showing how they listen for the shutdown signal sent by `Stop` (rule 2), including a communicating pair (a producer sending messages to a logger that owns the channel), plus a daemon that reloads on SIGHUP — lives in [`cmd/example`](cmd/example/main.go):

```
go run ./cmd/example
```

The example is self-commented: each daemon file documents its simulated workload, how it listens for `Stop`'s shutdown signal, and the README rules it follows.

## Writing daemons

A daemon is an object that satisfies the `Daemon` interface:

```go
type Daemon interface {
	Name() string
	Run() error
	Stop(ctx context.Context) error
}
```

The interface alone is not enough. The runner calls [`Run`](daemon/daemon.go) at startup, sequentially, and [`Stop`](daemon/daemon.go) at shutdown — `ctx` carries the graceful-shutdown deadline. Your object must additionally follow these rules:

### Rules

1. **`Run` must spawn the daemon's background goroutine.** It must return an error if startup fails. It may return `nil` immediately once the goroutine is running, or block until startup is confirmed.

2. **The goroutine must listen for the shutdown signal sent by its `Stop` and exit when it arrives.** It must block until shutdown is signaled, among others:

   1. A select on `Ctx.Done()`
   2. A bare `<-Ctx.Done()`
   3. A context-aware library call

   A goroutine that never blocks would keep the process alive.

3. **The goroutine must signal completion of shutdown.** Register `defer close(ShutdownDone)` as its **first** defer — defers run last-in-first-out, so it executes after every other deferred cleanup and `Stop` unblocks only once all cleanup has completed.

4. **`Stop` must wait for completion.** Signal the daemon to shut down and wait until the goroutine signals completion, or until the context deadline expires, whichever comes first.

The runner calls `Stop` once per `Run`. Calling it a second time is currently safe — it returns `nil` once shutdown has completed — but that is an implementation detail and may change, so treat repeated calls as undefined, as with [`io.Closer`](https://pkg.go.dev/io#Closer): the behavior after the first call is not part of the contract.

Reload is optional. A daemon that wants to reload on SIGHUP additionally implements `daemon.Reloader`; [Reloading daemons](#reloading-daemons) shows how.

### Example

The simplest daemon that satisfies all four rules — every rule is visible in the code:

```go
type SimpleDaemon struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed by the goroutine to signal completion
}

func NewSimpleDaemon() *SimpleDaemon {
	ctx, cancel := context.WithCancel(context.Background())
	return &SimpleDaemon{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (d *SimpleDaemon) Name() string { return "SimpleDaemon" }

func (d *SimpleDaemon) Run() error { // rule 1: Run spawns the background goroutine
	go func() {
		defer close(d.done) // rule 3: signal completion, after all cleanup
		<-d.ctx.Done()      // rule 2: block until shutdown is signaled
	}()
	return nil
}

func (d *SimpleDaemon) Stop(ctx context.Context) error {
	d.cancel()
	select {
	case <-d.done: // rule 4: wait for the goroutine's completion signal
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

### Rule helper: `Base`

The example above hand-writes `Stop`, the context, and the completion channel — the boilerplate every daemon needs. [`Base`](daemon/daemon.go) reduces that boilerplate. `Base` alone does not satisfy the interface: it has no `Run` — it is a helper, not a daemon, and you can implement the interface directly without it. It implements `Stop` (rule 4) — cancelling `Ctx` and waiting on `ShutdownDone` or the deadline — and provides the fields `Run` needs for the other rules:

- [`Ctx`](daemon/daemon.go) — cancelled by `Stop` to signal the goroutine to exit.
- [`Cancel`](daemon/daemon.go) — the cancel function `Stop` calls.
- [`ShutdownDone`](daemon/daemon.go) — the goroutine closes it to signal completion; `Stop` waits on it.
- [`Logger`](daemon/daemon.go) — used by `Stop` for lifecycle logging.

The same daemon with `Base`:

```go
type SimpleDaemon struct {
	daemon.Base // rule 4: Stop given as implemented; provides Ctx (rule 2) and ShutdownDone (rule 3)
}

func NewSimpleDaemon(logger *slog.Logger) *SimpleDaemon {
	return &SimpleDaemon{
		Base: daemon.NewBase("SimpleDaemon", logger),
	}
}

func (d *SimpleDaemon) Run() error { // rule 1: Run spawns the background goroutine
	go func() {
		defer close(d.ShutdownDone) // rule 3: signal completion, after all cleanup
		<-d.Ctx.Done()              // rule 2: block until shutdown is signaled
	}()
	return nil
}
```

With `Base`, `Run` listens on `Ctx` for the shutdown signal and closes `ShutdownDone` when the goroutine exits.

Real daemons following these rules: restinpieces' [log daemon](https://github.com/caasmo/restinpieces/blob/master/log/daemon.go), [scheduler](https://github.com/caasmo/restinpieces/blob/master/queue/scheduler/scheduler.go), and the [litestream daemon](https://github.com/caasmo/restinpieces-litestream/blob/master/litestream.go).

## Wiring daemons

The [`Runner`](run/run.go) wires daemons into a process lifecycle: it starts them sequentially, then waits for a termination signal, then shuts the started daemons down concurrently within a graceful deadline.

```go
r, err := run.NewRunner(
	run.WithLogger(logger),
	run.WithShutdownTimeout(30 * time.Second),
)
if err != nil {
	panic(err)
}
r.Add(backupDaemon) // any daemon.Daemon
r.Add(schedulerDaemon)
if err := r.Run(); err != nil {
	panic(err) // startup failed or shutdown had errors
}
```

### Options

- [`WithLogger(l *slog.Logger)`](run/run.go) — the logger for lifecycle logging. Default: `slog.Default()`; a nil logger falls back to it.
- [`WithShutdownTimeout(d time.Duration)`](run/run.go) — the deadline bounding the graceful shutdown. Default: 15 seconds; a non-positive duration makes `NewRunner` return an error wrapping [`ErrRunner`](run/run.go), checkable with `errors.Is`.
- [`WithReloadTimeout(d time.Duration)`](run/run.go) — the deadline bounding the reload pass started by SIGHUP. Default: 15 seconds; one deadline covers the whole pass, so every reload shares it. A non-positive duration makes `NewRunner` return an error wrapping [`ErrRunner`](run/run.go), checkable with `errors.Is`.

### Signals

The runner supports the following signals:

| Signal | Action |
| --- | --- |
| SIGINT, SIGQUIT, SIGTERM | Graceful shutdown of all daemons |
| SIGHUP | Reload every daemon that implements `daemon.Reloader`; daemons without it are skipped |

Under systemd: `systemctl stop` sends SIGTERM, `systemctl reload` sends SIGHUP via `ExecReload=/bin/kill -HUP $MAINPID`.

### Reloading daemons

SIGHUP does not shut the runner down. It asks every daemon that implements [`daemon.Reloader`](daemon/reload.go) to reload, in registration order; daemons without the interface are skipped. Under systemd: `systemctl reload` → `ExecReload=/bin/kill -HUP $MAINPID` → SIGHUP → `Reload` on each reloadable daemon.

A daemon opts in by embedding [`daemon.ReloadBase`](daemon/reload.go) next to `Base` and adding a reload case to its loop. `ReloadBase` provides two unbuffered channels: `ReloadCh` carries the caller's context into the loop (the send succeeds only when the loop is reading), and `ReloadDone` carries the result back (the loop hands it only to a waiting caller). Create them with `daemon.NewReloadBase()` in the constructor. The loop does the work, so it can change the daemon's state without locks — the loop is the only goroutine that touches it.

```go
type BackupDaemon struct {
	daemon.Base
	daemon.ReloadBase // implements daemon.Reloader
	interval time.Duration
	paused   bool // owned by the loop; toggled by reload
}

func (d *BackupDaemon) Run() error {
	go func() {
		defer close(d.ShutdownDone)

		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()

		for {
			select {
			case <-d.Ctx.Done():
				return
			case reloadCtx := <-d.ReloadCh:
				reloadErr := d.reload(reloadCtx)
				d.ReloadDone <- reloadErr
			case <-ticker.C:
				if d.paused {
					continue
				}
				// ... do the daemon's normal work ...
			}
		}
	}()
	return nil
}

func (d *BackupDaemon) reload(ctx context.Context) error {
	reloadErr := ctx.Err()
	if reloadErr != nil {
		return reloadErr
	}

	d.paused = !d.paused
	return nil
}
```

The reload runs inside the daemon's loop, so the loop cannot handle the next tick or the shutdown signal until the reload returns. The runner gives the whole SIGHUP pass a deadline through `ctx` — 15 seconds by default, configurable with [`WithReloadTimeout`](#options): every reload in that pass shares it. A reload that does blocking work must watch `ctx` and return when it expires. A reload that ignores the context blocks the runner's signal loop. A daemon that is not reading `ReloadCh` makes `Reload` return the context error after the deadline instead of blocking forever.

`Reload` is meant to be called once per reload signal. The signal loop handles one SIGHUP at a time, so the runner's calls are sequential. The unbuffered `ReloadCh` also serializes concurrent callers: the daemon's loop accepts one request at a time and sends that request's result before accepting the next, so each caller receives the result of its own request. A caller whose context expires while waiting its turn returns the context error without delivering the request. A failed reload is logged with the daemon's name, and the runner reloads the remaining daemons and keeps running.

## Communicating daemons

Daemons communicate the Go way — shared channels, not shared state. Forms:

- **Channel sharing at init time** — as in the example: main wires the write-end of one daemon's channel into another's constructor (`NewLogProducerDaemon(loggerDaemon.Chan(), ...)` in [main.go](cmd/example/main.go)). The consumer owns the channel, the drain, and the shutdown flush; producers hold only the write-end. See [logger_daemon.go](cmd/example/logger_daemon.go) and [log_producer_daemon.go](cmd/example/log_producer_daemon.go).
- **Channel sharing at runtime** — a daemon exposes a `Submit`-style method (`QueueDaemon.SubmitJob` in [queue_daemon.go](cmd/example/queue_daemon.go)); callers never touch the channel.
- **Context exposure** — a daemon's `Chan()` returns its context too, so producers stop sending before the owner drains and closes ([restinpieces' log daemon](https://github.com/caasmo/restinpieces/blob/master/log/daemon.go)).
