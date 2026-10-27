package handler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/broadcast"
)

const maxMessageSize = 1 << 10

func HandleConnection(conn net.Conn, h *broadcast.Hub) {
	defer h.Delete(conn)
	for {
		message, err := readMessage(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				slog.Info("client disconnected grasefully", "addr", conn.RemoteAddr())
				return
			}
			slog.Info("read error", "addr", conn.RemoteAddr())
			return
		}
		h.SendMessage(conn, string(message))
	}

}

func readMessage(conn net.Conn) ([]byte, error) {
	const op = "handler.readMessage"
	conn.SetDeadline(time.Now().Add(3 * time.Second))
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
		return nil, fmt.Errorf("%s: %w", op, err)
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
