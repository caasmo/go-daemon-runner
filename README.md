# go-daemon-runner

[![Go Reference](https://pkg.go.dev/badge/github.com/caasmo/go-daemon-runner)](https://pkg.go.dev/github.com/caasmo/go-daemon-runner)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/caasmo/go-daemon-runner/master/.github/badges/coverage.json)](https://github.com/caasmo/go-daemon-runner/actions/workflows/test.yml)
[![golangci-lint](https://github.com/caasmo/go-daemon-runner/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/caasmo/go-daemon-runner/actions/workflows/golangci-lint.yml)
[![sloc](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/caasmo/go-daemon-runner/master/.github/badges/sloc.json)](https://github.com/caasmo/go-daemon-runner/actions/workflows/sloc.yml)
[![deps](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/caasmo/go-daemon-runner/master/.github/badges/deps.json)](https://github.com/caasmo/go-daemon-runner/actions/workflows/dependencies.yml)

Compose your program from background goroutines — go-daemon-runner provides the process lifecycle around them: sequential start, signal handling, and concurrent, deadline-bounded graceful shutdown.

go-daemon-runner is for Go programs built from background goroutines. Your program is a set of components that run for its lifetime — periodic backups, schedulers, log aggregation, replication — each one a goroutine with a start/stop contract. The runner starts them sequentially in registration order, waits for SIGINT/SIGQUIT/SIGTERM, shuts them down concurrently within a configurable deadline (default 15s), and runs your reload hook on SIGHUP — the process keeps running, no restart.

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
  - [The reload hook](#the-reload-hook)
- [Communicating daemons](#communicating-daemons)

## Features

- **Graceful shutdown on termination signals.** On SIGINT, SIGQUIT, or SIGTERM the runner stops every started daemon concurrently and waits for each to complete before `Run` returns. No daemon is left running, no goroutine leaked, no process left hanging.
- **Deadline-bounded shutdown.** Shutdown is bounded by a configurable deadline (default 15s, `WithShutdownTimeout`). A daemon that fails to stop in time is reported — `Run` returns its error — instead of being waited on indefinitely.
- **Rollback on startup failure.** Daemons start sequentially in registration order. If one fails to start, the runner stops the daemons already running and `Run` returns the startup error: the process never continues half-started.
- **Reload on SIGHUP without restart.** SIGHUP invokes the hook set via `WithReloadFunc` while all daemons keep running — `systemctl reload` works. With no hook configured, SIGHUP is logged and ignored.
- **Errors surfaced to the caller.** `Run` returns startup and shutdown errors (joined), so the caller can map them to exit codes instead of guessing from logs.

## Installation

```
go get github.com/caasmo/go-daemon-runner
```

## The two packages

- `daemon` (`github.com/caasmo/go-daemon-runner/daemon`) — the [`Daemon`](daemon/daemon.go) interface and the [`Base`](daemon/daemon.go) helper struct for building daemons. Stdlib only.
- `run` (`github.com/caasmo/go-daemon-runner/run`) — the [`Runner`](run/run.go) dispatcher: sequential start, signal handling, and concurrent graceful shutdown.

Usage pattern: `daemon` to build daemons, `run` to run them.

## Runnable example

A complete runnable example of the library — daemons covering the blocking patterns of [rule 2](#rules), including a communicating pair (a producer sending messages to a logger that owns the channel), plus the reload hook — lives in [`cmd/example`](cmd/example/main.go):

```
go run ./cmd/example
```

The example is self-commented: each daemon file documents its simulated workload, blocking pattern, and the README rules it follows.

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

2. **The goroutine must reach a blocking point.** It must block until shutdown is signaled, among others:

   1. A select on `Ctx.Done()`
   2. A bare `<-Ctx.Done()`
   3. A context-aware library call

   A goroutine that never blocks would keep the process alive.

3. **The goroutine must signal completion of shutdown.** Register `defer close(ShutdownDone)` as its **first** defer — defers run last-in-first-out, so it executes after every other deferred cleanup and `Stop` unblocks only once all cleanup has completed.

4. **`Stop` must wait for completion.** Signal the daemon to shut down and wait until the goroutine signals completion, or until the context deadline expires, whichever comes first.

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

With `Base` you only write `Run`: the context and completion-channel fields, the constructor plumbing, `Name`, and `Stop` (rule 4) are inherited — `Name` is trivial anyway. What remains the daemon's job: `Run` spawning the goroutine (rule 1), reaching the blocking point (rule 2), and the goroutine signaling completion (rule 3).

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
- [`WithReloadFunc(fn func() error)`](run/run.go) — the function run on SIGHUP. Default: none — SIGHUP is logged and ignored.

### Signals

The runner supports the following signals — SIGHUP is the only one with a function, a single reload hook:

| Signal | Action |
| --- | --- |
| SIGINT, SIGQUIT, SIGTERM | Graceful shutdown of all daemons |
| SIGHUP | Reload hook (below); ignored if none configured |

Under systemd: `systemctl stop` sends SIGTERM, `systemctl reload` sends SIGHUP via `ExecReload=/bin/kill -HUP $MAINPID`.

### The reload hook

SIGHUP does not shut the runner down — it runs the `reloadFunc` set via `WithReloadFunc` and the signal loop keeps running:

`systemctl reload` → `ExecReload=/bin/kill -HUP $MAINPID` → SIGHUP → `reloadFunc`

`reloadFunc` is a closure that captures whatever state the reload needs and rebuilds it in place; the daemon instance is never swapped, so the rebuilt state takes effect on its next iteration. A failed reload is logged and the runner keeps running. Keep `reloadFunc` fast: it runs synchronously inside the signal loop, and termination signals arriving while it blocks can be dropped.

Wired into the runner with the example above:

```go
// pseudo-code: rebuild the daemon's state in place
reloadFunc := func() error {
	cfg, err := fetchConfig()
	if err != nil {
		return err
	}
	schedulerDaemon.Update(cfg) // takes effect on the next tick
	return nil
}

r, err := run.NewRunner(run.WithReloadFunc(reloadFunc))
if err != nil {
	panic(err)
}
r.Add(backupDaemon)
r.Add(schedulerDaemon)
if err := r.Run(); err != nil {
	panic(err)
}
```

## Communicating daemons

Daemons communicate the Go way — shared channels, not shared state. Forms:

- **Channel sharing at init time** — as in the example: main wires the write-end of one daemon's channel into another's constructor (`NewLogProducerDaemon(loggerDaemon.Chan(), ...)` in [main.go](cmd/example/main.go)). The consumer owns the channel, the drain, and the shutdown flush; producers hold only the write-end. See [logger_daemon.go](cmd/example/logger_daemon.go) and [log_producer_daemon.go](cmd/example/log_producer_daemon.go).
- **Channel sharing at runtime** — a daemon exposes a `Submit`-style method (`QueueDaemon.SubmitJob` in [queue_daemon.go](cmd/example/queue_daemon.go)); callers never touch the channel.
- **Context exposure** — a daemon's `Chan()` returns its context too, so producers stop sending before the owner drains and closes ([restinpieces' log daemon](https://github.com/caasmo/restinpieces/blob/master/log/daemon.go)).
- **Shared state** — a common pointer mutated in place, e.g. the reload hook flipping the backup pause flag in [main.go](cmd/example/main.go).
