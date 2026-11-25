# run: sequential run phase blocks the signal loop — consider non-sequential start

- [impl-daemon-runner.md](impl-daemon-runner.md) Phase 3 — `Runner.Run` runs daemons sequentially in registration order: a daemon whose `Run` hangs blocks the signal loop forever (no timeout; `shutdownTimeout` bounds only the shutdown phase). Consideration: start daemons non-sequentially (concurrently) so a hanging `Run` no longer blocks the others or the signal loop. The blocking-`Run` semantics (litestream style) are part of the documented contract, so this is a design decision, not a bug.

# run: no async error reporting for listener daemons

- [impl-daemon-runner.md](impl-daemon-runner.md) Phase 3 — `Runner.Run`: the `serverError` channel (cap 2) from server.go is dropped. When daemons that run listeners (e.g. HTTP server) are added, they may fail asynchronously after `Run` returns nil. Fix: add an `Error() <-chan error` method to the `Daemon` interface and reintroduce a select in the signal loop that also waits on it to trigger graceful shutdown on runtime errors.

# daemon.ReloadBase: make the request carry its own reply channel and let both waits honor ctx

The current plan (§1.1) uses an unbuffered `ReloadCh` and a shared unbuffered `ReloadDone`. The send is cancellable by `ctx`, but the wait on `ReloadDone` is not: a reload that ignores `ctx` blocks `Reload` and the runner's signal loop until the reload returns. A request struct gives every call its own reply channel and lets both waits select on `ctx.Done()`:

```go
type ReloadRequest struct {
	Ctx  context.Context
	Done chan error // buffered, cap 1: the loop never blocks on the reply
}
type ReloadBase struct{ ReloadCh chan ReloadRequest } // unbuffered

func (b *ReloadBase) Reload(ctx context.Context) error {
	req := ReloadRequest{Ctx: ctx, Done: make(chan error, 1)}
	select {
	case b.ReloadCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.Done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
// daemon loop: case req := <-d.ReloadCh: req.Done <- d.reload(req.Ctx)
```

This is not a concurrency change. Daemons are controlled by the runner, and `handleSIGHUP` calls `Reload` synchronously, one daemon at a time; the signal loop handles one SIGHUP at a time. The per-request `Done` exists so a caller that gives up at the deadline does not leave the loop blocked on a shared reply, not to support concurrent callers.

Reasoning:
- The runner's signal loop returns at the reload deadline and can handle SIGTERM. The daemon's loop never blocks on the reply because `Done` has cap 1, even if the caller already returned.
- The timeout now bounds both waits, not just the send.
- The daemon's loop can still be stuck inside a reload that ignores `ctx`, so `Stop` can still time out waiting for `ShutdownDone`. The "reload must honor ctx" contract remains.
- Cost: one allocation per reload, a new exported type, and updated docs/tests/examples. If the runner is the only caller and reloads honor `ctx`, the simpler shared-channel design is sufficient.

References:
- `impl-reloader.md` §1.1 (`Reloader`, `ReloadBase`, `Reload`), §2.1 (tests), §3.1 (`defaultReloadTimeout`), §5.1 (example loop), §6.3 (README)
- `daemon/reload.go` (planned)
- `daemon/reload_test.go` (planned)
- `run/run.go` (`handleSIGHUP`)
- `cmd/example/periodic_backup_daemon.go`
- `README.md` (reload section)

# ci: badge bot commits to master orphan tags on rebase

- `test.yml`, `sloc.yml`, `dependencies.yml` push badge JSON to `master`; a rebase rewrites those commits and orphans any tag on them (v0.3.0 orphaned this way)
- fix: push badges to a dedicated `badges` branch, point `README.md` badge URLs at it, never rebase tagged commits
- refs: `.github/workflows/test.yml`, `.github/workflows/sloc.yml`, `.github/workflows/dependencies.yml`, `README.md`
