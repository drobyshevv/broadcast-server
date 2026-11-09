package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const addr = "localhost:8082"

func RunServer() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	slog.Info("starting broadcast server", "addr", addr)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("failed to start listener", "addr", addr, "error", err)
		os.Exit(1)
	}
	defer l.Close()

	slog.Info("server is ready to accept connections")
	hub := newHub()
	go hub.run()

	wg := &sync.WaitGroup{}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				slog.Error("failed to accept connection", "error", err)
				continue
			}
			slog.Info("new client connected", "addr", conn.RemoteAddr())
			wg.Add(1)
			go handleConnection(wg, conn, hub)
		}
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-sig.Done()
	l.Close()

	close(hub.shutdown)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		<-hub.done
		slog.Info("server has shut down correctly")
	case <-time.After(time.Second * 10):
		slog.Error("graceful shutdown timeout")
		os.Exit(0)
	}
}
