package client

import (
	"log/slog"
)

func RunClient() {
	fsm := newFSM()
	go fsm.run()
	fsm.event <- event{kind: evtStart}
	<-fsm.done
	slog.Info("Client exited gracefully")
}
