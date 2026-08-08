package main

import (
	"log/slog"
	"sync"

	"github.com/caasmo/go-daemon-runner/daemon"
)

// QueueDaemon simulates a background job queue.
//
// Its real-world counterpart is a worker pool consuming jobs that
// other components enqueue (emails to send, reports to generate,
// logs to rotate). A single worker reads jobs from a buffered
// channel and processes them one at a time; in a real daemon the
// processing would be the actual work instead of a log line.
//
// Blocking pattern 2 — a bare receive on Ctx.Done(): the daemon
// goroutine spawns the worker (which observes the context itself)
// and then blocks on <-Ctx.Done(). Before signaling completion it
// joins the worker with a WaitGroup, so Stop unblocks only once the
// worker has finished its in-flight job — a graceful drain of the
// queue.
type QueueDaemon struct {
	daemon.Base
	jobs chan string    // incoming jobs
	wg   sync.WaitGroup // joins the worker before shutdown completes
}

func NewQueueDaemon(logger *slog.Logger) *QueueDaemon {
	return &QueueDaemon{
		Base: daemon.NewBase("QueueDaemon", logger),
		jobs: make(chan string, 8),
	}
}

func (d *QueueDaemon) Run() error {
	go func() {
		defer close(d.ShutdownDone) // first defer: signal completion after all cleanup

		d.wg.Add(1)
		go d.worker()  // the worker selects on d.Ctx.Done() itself
		<-d.Ctx.Done() // cancelled by Stop(), nothing else to do
		d.wg.Wait()    // Stop() unblocks only once the worker has finished
	}()
	return nil
}

// worker consumes jobs until the context is cancelled; the select
// makes the goroutine exit even if no more jobs arrive.
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

// doWork simulates processing a job: in a real daemon this would
// send the email, generate the report, or rotate the logs.
func (d *QueueDaemon) doWork(job string) {
	d.Logger.Info("processing job", "job", job)
}
