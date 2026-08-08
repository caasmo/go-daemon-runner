package main

import (
	"log/slog"

	"github.com/caasmo/go-daemon-runner/daemon"
)

// LoggerDaemon is the consumer in an inter-daemon communication:
// it owns a messages channel and exposes only its write-end via
// Chan(), following the restinpieces log daemon pattern. Producers
// send strings to it; on shutdown it drains the channel before
// signaling completion, so no message enqueued before Stop is lost.
//
// Blocking pattern 1 — a select on Ctx.Done() and the messages
// channel: the message case logs the message, the Ctx.Done() case
// drains and exits. The deferred close(ShutdownDone) runs after the
// drain, so Stop only returns once the channel is empty.
//
// README rules: 1 (Run spawns the goroutine), 2 (the blocking point
// is the select), 3 (first defer closes ShutdownDone), and 4 via the
// Stop inherited from Base.
type LoggerDaemon struct {
	daemon.Base // README rule 4: Stop inherited from Base, waits on ShutdownDone
	messages    chan string
}

func NewLoggerDaemon(logger *slog.Logger) *LoggerDaemon {
	d := &LoggerDaemon{
		Base:     daemon.NewBase("LoggerDaemon", logger),
		messages: make(chan string, 64),
	}
	// Every log line carries the daemon's identity: reuse the
	// daemon_name attribute the runner attaches to lifecycle logs.
	d.Logger = d.Logger.With("daemon_name", d.Name())
	return d
}

// Chan returns the write-end of the messages channel. Producers send
// messages here; the daemon keeps the read-end and the drain to
// itself.
func (d *LoggerDaemon) Chan() chan<- string {
	return d.messages
}

func (d *LoggerDaemon) Run() error {
	go func() { // README rule 1: Run spawns the background goroutine
		defer close(d.ShutdownDone) // README rule 3: first defer, signals completion to Stop

		for {
			select { // README rule 2: blocking point
			case msg := <-d.messages:
				d.Logger.Info("received message", "message", msg)
			case <-d.Ctx.Done():
				// Shutdown: drain whatever producers enqueued before
				// stopping, then signal completion.
				for {
					select {
					case msg := <-d.messages:
						d.Logger.Info("received message", "message", msg)
					default:
						return
					}
				}
			}
		}
	}()
	return nil
}
