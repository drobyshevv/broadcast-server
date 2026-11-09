package main

import (
	"log/slog"
	"os"

	"github.com/drobyshevv/broadcast-server/internal/client"
	"github.com/drobyshevv/broadcast-server/internal/server"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		slog.Warn("arguments not specified")
		os.Exit(1)
	}

	arg := os.Args[1]

	switch arg {
	case "start":
		server.RunServer()
	case "connect":
		client.RunClient()
	default:
		slog.Warn("Unknown command", "arg", arg)
		os.Exit(1)
	}
}
