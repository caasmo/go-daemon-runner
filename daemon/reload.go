package daemon

import "context"

// Reloader is implemented by daemons that can reload their state
// without a restart. On SIGHUP the runner checks each registered
// daemon for this interface and calls Reload on the ones that have
// it; daemons without it are skipped.
//
// Reload sends the caller's context to the daemon's loop and waits
// for the loop to accept the request and finish the reload. The
// context carries the reload deadline, so a reload that does blocking
// work must watch it and return when it expires. If the deadline
// expires before the loop accepts the request, Reload returns the
// context error without delivering the request.
//
// The runner calls Reload once per reloadable daemon per SIGHUP, in
// registration order, and serializes those calls. The unbuffered
// ReloadCh also serializes concurrent callers: the daemon's loop
// accepts one request at a time and sends that request's result
// before accepting the next, so each caller receives the result of
// its own request. A caller whose context expires while waiting its
// turn returns the context error without delivering the request. The
// runner does not rely on this; it calls Reload sequentially.
//
// The runner always passes a context with a deadline. A caller that
// uses Reload directly should do the same: with no deadline and no
// loop reading ReloadCh, the send waits forever.
type Reloader interface {
	Reload(ctx context.Context) error
}

// ReloadBase holds the channels that carry a reload request into a
// daemon's loop and the result back to the caller. Embed it next to
// Base to give a daemon the Reloader method:
//
//	type BackupDaemon struct {
//		Base
//		ReloadBase
//	}
//
// The daemon's loop reads the request and writes the result:
//
//	case reloadCtx := <-d.ReloadCh:
//		reloadErr := d.reload(reloadCtx)
//		d.ReloadDone <- reloadErr
//
// The daemon implements the unexported reload method; its body does
// the actual work. A daemon that does not embed ReloadBase has no
// reload capability and the runner skips it on SIGHUP.
type ReloadBase struct {
	// ReloadCh carries the caller's context to the daemon's loop. It is
	// unbuffered: the send succeeds only when the loop is reading, so
	// the send can be cancelled by the caller's context.
	ReloadCh chan context.Context

	// ReloadDone carries the loop's result back to Reload. It is
	// unbuffered: the loop hands the result only to a caller that is
	// still waiting.
	ReloadDone chan error
}

// NewReloadBase creates the reload channels. Embed the returned value
// by value and use the daemon through a pointer: Reload has a pointer
// receiver, so a value daemon does not satisfy Reloader.
func NewReloadBase() ReloadBase {
	return ReloadBase{
		ReloadCh:   make(chan context.Context),
		ReloadDone: make(chan error),
	}
}

// Reload sends the caller's context to the daemon's loop and waits
// for the loop to accept the request and finish the reload. The
// context is forwarded to the reload, which must return when it
// expires. If the context is already canceled, Reload returns its
// error without sending the request.
//
// ReloadCh is unbuffered, so the send waits until the loop reads the
// request; if the context expires first, Reload returns ctx.Err() and
// the request is never delivered. Once the loop takes the request it
// must send the result on ReloadDone, so the reload must honor ctx: a
// reload that blocks past the deadline keeps Reload, the loop, and
// the runner's signal loop waiting.
func (b *ReloadBase) Reload(ctx context.Context) error {
	reloadErr := ctx.Err()
	if reloadErr != nil {
		return reloadErr
	}

	select {
	case b.ReloadCh <- ctx:
	case <-ctx.Done():
		return ctx.Err()
	}

	// The daemon's loop must send a result for every request it
	// receives, even when the reload failed or the context expired,
	// so this receive is paired with that send. That is why the wait
	// does not select on ctx.Done(): a loop that receives the request
	// and never sends violates the contract and blocks Reload.
	return <-b.ReloadDone
}
