package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/caasmo/go-daemon-runner/daemon"
)

// LogProducerDaemon is the producer side of the inter-daemon
// communication: it emits log messages and sends them to the
// LoggerDaemon through the write-end Chan() gave it — the wiring
// happens in main. The producer owns nothing: the consumer owns the
// channel, the producer just sends into it.
//
// Blocking pattern 1 — a select on Ctx.Done() and a ticker: the
// ticker case emits one message, the Ctx.Done() case exits on
// shutdown. The deferred close(ShutdownDone) runs after the ticker
// stops.
//
// README rules: 1 (Run spawns the goroutine), 2 (the blocking point
// is the select), 3 (first defer closes ShutdownDone), and 4 via the
// Stop inherited from Base.
type LogProducerDaemon struct {
	daemon.Base // README rule 4: Stop inherited from Base, waits on ShutdownDone
	messages    chan<- string // write-end from LoggerDaemon.Chan()
	every       time.Duration
}

func NewLogProducerDaemon(messages chan<- string, every time.Duration, logger *slog.Logger) *LogProducerDaemon {
	d := &LogProducerDaemon{
		Base:     daemon.NewBase("LogProducerDaemon", logger),
		messages: messages,
		every:    every,
	}
	// Every log line carries the daemon's identity: reuse the
	// daemon_name attribute the runner attaches to lifecycle logs.
	d.Logger = d.Logger.With("daemon_name", d.Name())
	return d
}

func (d *LogProducerDaemon) Run() error {
	go func() { // README rule 1: Run spawns the background goroutine
		defer close(d.ShutdownDone) // README rule 3: first defer, signals completion to Stop

		ticker := time.NewTicker(d.every)
		defer ticker.Stop()

		n := 0
		for {
			select { // README rule 2: blocking point
			case <-ticker.C:
				n++
				msg := fmt.Sprintf("message %d", n)
				d.Logger.Info("sent message", "message", msg)
				d.messages <- msg
			case <-d.Ctx.Done():
				return
			}
		}
	}()
	return nil
}
