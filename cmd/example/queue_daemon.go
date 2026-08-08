package main

import (
	"log/slog"

	"github.com/caasmo/go-daemon-runner/daemon"
	"golang.org/x/sync/errgroup"
)

// QueueDaemon simulates a background job queue.
//
// Its real-world counterpart is a worker pool consuming jobs that
// other components enqueue (emails to send, reports to generate,
// logs to rotate). Jobs arrive through Submit; a bounded errgroup
// pool runs them, at most two at a time.
//
// Blocking pattern 2 — a bare receive on Ctx.Done(): the daemon
// goroutine reads jobs and submits each to the pool via g.Go, then
// blocks on <-Ctx.Done(). On shutdown it joins the in-flight jobs
// with g.Wait() before signaling completion — a graceful drain of
// the queue.
//
// README rules: 1 (Run spawns the goroutine), 2 (the blocking point
// is the bare <-Ctx.Done()), 3 (first defer closes ShutdownDone),
// and 4 via the Stop inherited from Base.
type QueueDaemon struct {
	daemon.Base             // README rule 4: Stop inherited from Base, waits on ShutdownDone
	jobs        chan string // incoming jobs
}

func NewQueueDaemon(logger *slog.Logger) *QueueDaemon {
	d := &QueueDaemon{
		Base: daemon.NewBase("QueueDaemon", logger),
		jobs: make(chan string, 8),
	}
	// Every log line carries the daemon's identity: reuse the
	// daemon_name attribute the runner attaches to lifecycle logs.
	d.Logger = d.Logger.With("daemon_name", d.Name())
	return d
}

// SubmitJob enqueues a job. The daemon owns the channel; callers
// only see this write-end. The buffered channel lets main submit
// jobs before Run starts the daemon goroutine.
func (d *QueueDaemon) SubmitJob(job string) {
	d.jobs <- job
}

func (d *QueueDaemon) Run() error {
	go func() { // README rule 1: Run spawns the background goroutine
		defer close(d.ShutdownDone) // README rule 3: first defer, signals completion to Stop

		// Bounded worker pool — the go-walkthrough worker_errgroup
		// pattern: an errgroup with SetLimit runs at most two jobs
		// concurrently; each received job is submitted via g.Go.
		g := new(errgroup.Group)
		g.SetLimit(2)

		for {
			select {
			case <-d.Ctx.Done(): // README rule 2: blocking point; Stop cancels Ctx to unblock it
				// Graceful drain: join the in-flight jobs before
				// signaling completion to Stop; a failed job is
				// logged, not fatal.
				drainErr := g.Wait()
				if drainErr != nil {
					d.Logger.Error("job failed", "error", drainErr)
				}
				return
			case job := <-d.jobs:
				g.Go(func() error {
					d.doWork(job)
					return nil
				})
			}
		}
	}()
	return nil
}

// doWork simulates processing a job: in a real daemon this would
// send the email, generate the report, or rotate the logs.
func (d *QueueDaemon) doWork(job string) {
	d.Logger.Info("processing job", "job", job)
}
