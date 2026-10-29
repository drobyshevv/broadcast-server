package main

import (
	"log/slog"
	"net"
	"os"

	"github.com/drobyshevv/broadcast-server/internal/broadcast"
	"github.com/drobyshevv/broadcast-server/internal/handler"
)

const addr = "localhost:8082"

func main() {
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
	hub := broadcast.NewHub()
	go hub.Run()

	for {
		conn, err := l.Accept()
		if err != nil {
			slog.Error("failed to accept connection", "error", err)
			continue
		}
		slog.Info("new client connected", "addr", conn.RemoteAddr())
		go handler.HandleConnection(conn, hub)
	}
}
