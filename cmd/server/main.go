package main

import (
	"log/slog"
	"net"
	"os"

	"github.com/drobyshevv/broadcast-server/internal/broadcast"
	"github.com/drobyshevv/broadcast-server/internal/handler"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	slog.Info("starting broadcast server", "addr", "localhost:8082")

	h := broadcast.NewHub()

	l, err := net.Listen("tcp", "localhost:8082")
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	slog.Info("server is ready to accept connections")

	for {
		conn, err := l.Accept()
		if err != nil {
			slog.Error("failed to listen", "error", err)
			continue
		}
		slog.Info("new client connected", "addr", conn.RemoteAddr())
		h.Add(conn)
		go handler.HandleConnection(conn, h)
	}
}
