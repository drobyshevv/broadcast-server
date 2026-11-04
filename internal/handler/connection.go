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
	"time"

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
			switch {
			case errors.Is(err, io.EOF):
				slog.Info("client disconnected", "addr", conn.RemoteAddr())

			case errors.Is(err, os.ErrDeadlineExceeded):
				slog.Warn("client read timeout", "addr", conn.RemoteAddr())

			case errors.Is(err, io.ErrUnexpectedEOF):
				slog.Warn("incomplete message", "addr", conn.RemoteAddr(), "error", err)

			default:
				slog.Error("failed to read message", "addr", conn.RemoteAddr(), "error", err)
			}
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
	var header [5]byte
	_, err := io.ReadFull(conn, header[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("failed to read header: %w", err)
	}

	messageType := protocol.MessageType(int8(header[0]))
	size := binary.BigEndian.Uint32(header[1:])
	if size > maxMessageSize {
		return 0, nil, fmt.Errorf("message too large")
	}
	payload := make([]byte, int(size))

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return 0, nil, fmt.Errorf("failed to set read deadline: %w", err)
	}
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, fmt.Errorf("incomplete payload: %w", io.ErrUnexpectedEOF)
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return 0, nil, fmt.Errorf("read payload timeout: %w", err)
		}

		return 0, nil, fmt.Errorf("failed to read payload: %w", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return 0, nil, fmt.Errorf("failed to reset read deadline: %w", err)
	}
	return messageType, payload, nil
}
