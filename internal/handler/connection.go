package handler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"

	"github.com/drobyshevv/broadcast-server/internal/broadcast"
	"github.com/drobyshevv/broadcast-server/internal/client"
	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

const maxMessageSize = 1 << 8

func HandleConnection(wg *sync.WaitGroup, conn net.Conn, h *broadcast.Hub) {
	defer wg.Done()
	client := client.NewClient(conn)
	h.Register <- client
	defer func() { h.Unregister <- client }()
	for {
		messageType, message, err := readMessage(client.Conn)
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
		if messageType == protocol.MessageTypeACK {
			break
		}
		h.Broadcast <- &broadcast.BroadcastEvent{
			Sender:  client,
			Message: protocol.NewMessage(messageType, message),
		}
	}
}

func readMessage(conn net.Conn) (protocol.MessageType, []byte, error) {
	const op = "handler.readMessage"
	buf := make([]byte, 5)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return 0, nil, fmt.Errorf("%s read timeout: %w", op, err)
		}
		return 0, nil, fmt.Errorf("%s: %w", op, err)
	}

	messageType := protocol.MessageType(int8(buf[0]))
	size := binary.BigEndian.Uint32(buf[1:])
	if size > maxMessageSize {
		return 0, nil, fmt.Errorf("%s: message to large", op)
	}
	payload := make([]byte, int(size))

	_, err = io.ReadFull(conn, payload)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return messageType, payload, nil
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return 0, nil, fmt.Errorf("%s read timeout: %w", op, err)
		}
		return 0, nil, fmt.Errorf("%s failed to read payload: %w", op, err)
	}
	return messageType, payload, nil
}
