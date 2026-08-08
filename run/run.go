// Package run provides a Runner that manages the lifecycle of daemons:
// sequential start, signal handling, and concurrent graceful shutdown.
package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caasmo/go-daemon-runner/daemon"
	"golang.org/x/sync/errgroup"
)

// Runner manages the lifecycle of a collection of daemons:
// sequential start, signal-driven main loop, and concurrent
// graceful shutdown via errgroup.
type Runner struct {
	daemons         []daemon.Daemon
	logger          *slog.Logger
	shutdownTimeout time.Duration
	reloadFunc      func() error
}

// defaultShutdownTimeout bounds the graceful shutdown phase when
// WithShutdownTimeout is not provided; mirrors restinpieces'
// ShutdownGracefulTimeout default of 15s.
const defaultShutdownTimeout = 15 * time.Second

// defaultErrGroupLimit bounds the number of daemons stopped
// concurrently; with many daemons it prevents a closing storm of
// ShutdownDone channels.
const defaultErrGroupLimit = 32

// ErrRunner is the framework's general sentinel error: NewRunner
// returns an error wrapping it when an option value is invalid,
// e.g. a non-positive shutdown timeout to WithShutdownTimeout.
var ErrRunner = errors.New("runner error")

// Option configures a Runner created by NewRunner.
type Option func(*Runner)

// WithLogger sets the logger used for lifecycle logging. The default
// is slog.Default(); nil falls back to the default.
func WithLogger(l *slog.Logger) Option {
	return func(r *Runner) {
		r.logger = l
	}
}

// WithShutdownTimeout sets the deadline bounding the graceful shutdown
// phase. The default is 15 seconds; a non-positive duration makes
// NewRunner return an error wrapping ErrRunner.
func WithShutdownTimeout(d time.Duration) Option {
	return func(r *Runner) {
		r.shutdownTimeout = d
	}
}

// WithReloadFunc sets the function called on SIGHUP; without it,
// SIGHUP is logged and ignored.
func WithReloadFunc(fn func() error) Option {
	return func(r *Runner) {
		r.reloadFunc = fn
	}
}

// NewRunner creates a Runner with default settings: slog.Default()
// logger, 15-second shutdown timeout, no reload function. Customize
// with WithLogger, WithShutdownTimeout, and WithReloadFunc. It
// returns an error wrapping ErrRunner when a configured option
// value is invalid, e.g. a non-positive shutdown timeout.
func NewRunner(opts ...Option) (*Runner, error) {
	r := &Runner{
		logger:          slog.Default(),
		shutdownTimeout: defaultShutdownTimeout,
		daemons:         make([]daemon.Daemon, 0),
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.logger == nil {
		r.logger = slog.Default()
	}
	if r.shutdownTimeout <= 0 {
		return nil, fmt.Errorf("%w: shutdown timeout must be positive, got %s", ErrRunner, r.shutdownTimeout)
	}
	return r, nil
}

// Add registers a daemon whose lifecycle will be managed by the runner.
// Add must be called before Run: daemons added after Run has entered
// the signal loop are never started.
func (r *Runner) Add(d daemon.Daemon) {
	if d == nil {
		r.logger.Warn("attempted to add a nil daemon")
		return
	}
	r.logger.Info("adding daemon", "daemon_name", d.Name())
	r.daemons = append(r.daemons, d)
}

// handleSIGHUP handles the SIGHUP signal, which carries reload
// semantics: it runs the configured reloadFunc and logs the outcome;
// without one, SIGHUP is logged and ignored.
func (r *Runner) handleSIGHUP() {
	if r.reloadFunc == nil {
		r.logger.Info("received SIGHUP signal but no reload function configured")
		return
	}
	r.logger.Info("received SIGHUP signal - attempting reload")
	err := r.reloadFunc()
	if err != nil {
		r.logger.Error("reload failed", "error", err)
	} else {
		r.logger.Info("reload successful")
	}
}

// Run starts all daemons sequentially, enters a signal-handling loop,
// and on SIGINT/SIGQUIT/SIGTERM (or a startup failure) performs concurrent
// graceful shutdown of the started daemons. It returns nil on success, or
// the startup or shutdown error, which the caller maps to an exit code.
// Run is intended to be called once per Runner: a second call would
// re-run d.Run() on already-started daemons. The runner trusts the
// caller on this — a second call is not guarded.
func (r *Runner) Run() error {
	// signal.Notify is process-global state: the runtime binds the
	// signal to this channel for the whole process, suppressing the
	// default dispositions process-wide. Register before daemons
	// start: a termination signal arriving during a slow daemon
	// startup would otherwise hit its default disposition and kill the
	// process without graceful shutdown. Registered early, the signal
	// is buffered and honored once the signal loop starts.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGHUP)

	// --- Start Daemons Sequentially ---
	r.logger.Info("starting daemons sequentially...")
	var startErr error
	startedDaemons := make([]daemon.Daemon, 0, len(r.daemons))
	for _, d := range r.daemons {
		r.logger.Info("starting daemon", "daemon_name", d.Name())
		startErr = d.Run()
		if startErr != nil {
			r.logger.Error("failed to start daemon, initiating shutdown",
				"daemon_name", d.Name(), "error", startErr)
			break
		}
		startedDaemons = append(startedDaemons, d)
		r.logger.Info("daemon started successfully", "daemon_name", d.Name())
	}

	// TODO: Listener daemons may fail asynchronously after Run
	// returns nil; async errors are not reported. See TODO.md.

	// Signal loop — only entered if all daemons started successfully.
	if startErr == nil {
		running := true
		for running {
			sig := <-sigChan
			switch sig {
			case syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
				r.logger.Info("received termination signal - gracefully shutting down",
					"signal", sig)
				running = false
			case syscall.SIGHUP:
				r.handleSIGHUP()
			}
		}
	}

	// Stop relaying signals to the channel — the runtime restores each
	// signal's default disposition — and release the channel. Runs on
	// every path, also when a startup failure skipped the signal loop.
	signal.Stop(sigChan)
	close(sigChan)

	// --- Graceful Shutdown ---
	gracefulCtx, cancelShutdown := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancelShutdown()

	// The errgroup is used only for concurrent execution + error
	// collection, not for cancellation propagation — a plain Group
	// is used, so one daemon's failure does not cancel the others.
	// SetLimit bounds concurrent Stop calls so a burst of closing
	// ShutdownDone channels (closing storm) cannot happen with many
	// daemons.
	var shutdownGroup errgroup.Group
	shutdownGroup.SetLimit(defaultErrGroupLimit)

	r.logger.Info("stopping daemons...")
	for _, d := range startedDaemons {
		shutdownGroup.Go(func() error {
			err := d.Stop(gracefulCtx)
			if err != nil {
				return fmt.Errorf("daemon %q failed to stop gracefully: %w", d.Name(), err)
			}
			return nil
		})
	}

	shutdownErr := shutdownGroup.Wait()
	if shutdownErr != nil {
		r.logger.Error("error during graceful shutdown", "err", shutdownErr)
	}

	// Report the result to the caller, which maps a non-nil error to
	// an exit code: startup failed or the shutdown process failed.
	if startErr != nil || shutdownErr != nil {
		r.logger.Info("shutdown finished with errors.")
		return errors.Join(startErr, shutdownErr)
	}
	r.logger.Info("all systems stopped gracefully.")
	return nil
}
