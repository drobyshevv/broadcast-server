package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

const maxMessageSize = 1 << 8

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	conn, err := net.Dial("tcp", "localhost:8082")
	if err != nil {
		slog.Error("failed to connection to server", "error", err)
		panic(err)
	}
	disconnect := make(chan struct{})
	message := make(chan []byte)

	go scanStdin(message)

	errRead := make(chan error, 1)
	errWrite := make(chan error, 1)

	go func() {
		errRead <- readMessage(disconnect, conn)
	}()

	shutdown := make(chan struct{})

	go func() {
		errWrite <- writeMessage(disconnect, shutdown, conn, message)
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errRead:
		logError(err)
		conn.Close()

	case err := <-errWrite:
		logError(err)
		conn.Close()

	case <-disconnect:
		slog.Info("client is disconnecting")

		err := <-errRead
		if err != nil {
			logError(err)
			conn.Close()
			return
		}
		err = <-errWrite
		if err != nil {
			logError(err)
		}
		if err := conn.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			logError(err)
		}
		slog.Info("client has disconnected correctly")

	case <-sig.Done():
		slog.Info("client is shutting down")

		close(shutdown)

		err := <-errWrite
		if err != nil {
			logError(err)
			conn.Close()
			return
		}

		if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
			logError(err)
			conn.Close()
			return
		}

		err = <-errRead
		if err != nil && !errors.Is(err, io.EOF) {
			logError(err)
			conn.Close()
			return
		}

		if err := conn.Close(); err != nil {
			logError(err)
			return
		}

		slog.Info("client has shut down correctly")
	}
}

func scanStdin(message chan<- []byte) {
	scannerWriter := bufio.NewScanner(os.Stdin)
	for scannerWriter.Scan() {
		message <- append([]byte(nil), scannerWriter.Bytes()...)
	}
}

func writeMessage(disconnect chan struct{}, shutdown chan struct{}, conn net.Conn, message <-chan []byte) error {
	for {
		select {
		case ms := <-message:
			if len(ms) > maxMessageSize {
				slog.Warn("message too large", "size", len(ms))
				continue
			}
			header := make([]byte, 4)
			binary.BigEndian.PutUint32(header, uint32(len(ms)))
			typeByte := []byte{byte(protocol.MessageTypeChat)}
			bf := net.Buffers{typeByte, header, ms}
			_, err := bf.WriteTo(conn)
			if err != nil {
				return fmt.Errorf("send message: %w", err)
			}
		case <-disconnect:
			header := make([]byte, 4)
			binary.BigEndian.PutUint32(header, 0)
			typeByte := []byte{byte(protocol.MessageTypeACK)}
			bf := net.Buffers{typeByte, header, []byte{}}
			_, err := bf.WriteTo(conn)
			if err != nil {
				return fmt.Errorf("send message: %w", err)
			}
			return nil
		case <-shutdown:
			return nil
		}
	}
}

func readMessage(disconnect chan struct{}, conn net.Conn) error {
	for {
		var header [5]byte
		_, err := io.ReadFull(conn, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return fmt.Errorf("failed to read header: %w", err)
		}

		messageType := protocol.MessageType(int8(header[0]))
		size := binary.BigEndian.Uint32(header[1:])
		if size > maxMessageSize {
			return fmt.Errorf("message too large")
		}
		payload := make([]byte, int(size))

		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			return fmt.Errorf("failed to set read deadline: %w", err)
		}
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("incomplete payload: %w", io.ErrUnexpectedEOF)
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return fmt.Errorf("read payload timeout: %w", err)
			}

			return fmt.Errorf("failed to read payload: %w", err)
		}
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			return fmt.Errorf("failed to reset read deadline: %w", err)
		}
		printMessage(messageType, payload)
		if messageType == protocol.MessageTypeShutdown {
			close(disconnect)
			return nil
		}
	}
}

func printMessage(messageType protocol.MessageType, payload []byte) {
	switch messageType {
	case protocol.MessageTypeChat:
		fmt.Printf("\n[CHAT] %s\n> ", payload)

	case protocol.MessageTypeShutdown:
		fmt.Println("\n[SERVER] Server is shutting down...")

	default:
		fmt.Printf("\n[UNKNOWN] type=%d payload=%q\n> ", messageType, payload)
	}
}

func logError(err error) {
	switch {
	case err == nil:
		return

	case errors.Is(err, io.EOF):
		slog.Info("server disconnected")

	case errors.Is(err, os.ErrDeadlineExceeded):
		slog.Warn("connection timeout", "error", err)

	case errors.Is(err, net.ErrClosed):
		slog.Debug("connection closed")

	default:
		slog.Error("client failed", "error", err)
	}
}
