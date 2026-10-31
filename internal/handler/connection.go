package handler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/drobyshevv/broadcast-server/internal/broadcast"
	"github.com/drobyshevv/broadcast-server/internal/client"
)

const maxMessageSize = 1 << 8

func HandleConnection(conn net.Conn, h *broadcast.Hub) {
	client := client.NewClient(conn)
	h.Register <- client
	defer func() { h.Unregister <- client }()
	for {
		message, err := readMessage(client.Conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				slog.Info("client disconnected grasefully", "addr", client.Conn.RemoteAddr())
				return
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				slog.Error("read timeout", "addr", client.Conn.RemoteAddr())
				return
			}
			slog.Error("read error", "addr", client.Conn.RemoteAddr(), "error", err)
			return
		}
		h.Broadcast <- &broadcast.BroadcastMessage{
			Sender:  client,
			Message: message,
		}
	}
}

func readMessage(conn net.Conn) ([]byte, error) {
	const op = "handler.readMessage"
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return nil, fmt.Errorf("%s read timeout: %w", op, err)
		}
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	size := binary.BigEndian.Uint32(buf)
	if size > maxMessageSize {
		return nil, fmt.Errorf("%s: message to large", op)
	}
	payload := make([]byte, int(size))

	_, err = io.ReadFull(conn, payload)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return payload, nil
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return nil, fmt.Errorf("%s read timeout: %w", op, err)
		}
		return nil, fmt.Errorf("%s failed to read payload: %w", op, err)
	}
	return payload, nil
}
