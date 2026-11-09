package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

const maxMessageSize = 1 << 8

type eventType int

const (
	evtStart eventType = iota
	evtUserMessage
	evtServerMessage
	evtRemoteShutdown
	evtLocalShutdown
	evtError
)

type event struct {
	kind    eventType
	payload []byte
	err     error
}

type stateFSM string

const (
	initializing  stateFSM = "Initializing"
	connected     stateFSM = "Connected"
	shuttingDown  stateFSM = "Shutting down"
	disconnecting stateFSM = "Disconnecting"
	disconnected  stateFSM = "Disconnected"
)

type fsm struct {
	state stateFSM
	conn  net.Conn
	event chan event
	done  chan struct{}
}

func newFSM() *fsm {
	return &fsm{
		state: initializing,
		event: make(chan event, 10),
		done:  make(chan struct{}),
	}
}

func (f *fsm) run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for f.state != disconnected {
		select {
		case ev := <-f.event:
			f.handler(ev)
		case <-sigChan:
			f.handler(event{evtLocalShutdown, []byte{}, nil})
		}
	}

	slog.Info("FSM stopped, final state: Disconnected")
	close(f.done)
}

func (f *fsm) handler(ev event) {
	switch f.state {
	case initializing:
		if ev.kind == evtStart {
			f.connection()
			f.state = connected
		}

	case connected:
		switch ev.kind {
		case evtUserMessage:
			f.sendMessage(ev.payload)

		case evtServerMessage:
			printMessage(protocol.MessageTypeChat, ev.payload)

		case evtRemoteShutdown:
			printMessage(protocol.MessageTypeShutdown, ev.payload)
			f.sendACK()
			f.state = disconnecting

		case evtLocalShutdown:
			slog.Info("Local shutdown initiated")
			f.closeWrite()
			f.stopInput()
			f.state = shuttingDown

		case evtError:
			slog.Error("Unexpected connection error", "error", ev.err)
			f.closeConnection()
			f.state = disconnected
		}

	case disconnecting:
		switch ev.kind {
		case evtLocalShutdown:
			slog.Warn("Force closing connection")
			f.closeConnection()
			f.state = disconnected

		case evtError:
			if errors.Is(ev.err, io.EOF) {
				slog.Info("Server acknowledged shutdown (EOF received)")
			} else {
				slog.Warn("Connection closed with error during shutdown", "error", ev.err)
			}
			f.closeConnection()
			f.state = disconnected
		}

	case shuttingDown:
		switch ev.kind {
		case evtError:
			slog.Info("Server responded to CloseWrite (EOF received)")
			f.closeConnection()
			f.state = disconnected
		}

	case disconnected:
		slog.Debug("Ignoring event in Disconnected state", "event", ev.kind)
	}
}

func (f *fsm) connection() {
	conn, err := net.Dial("tcp", "localhost:8082")
	if err != nil {
		f.event <- event{evtError, []byte{}, err}
		return
	}

	f.conn = conn

	go f.writerLoop()
	go f.readerLoop()
}

func (f *fsm) closeConnection() {
	if f.conn != nil {
		f.conn.Close()
	}
}

func (f *fsm) closeWrite() {
	if f.conn == nil {
		return
	}
	if tcpConn, ok := f.conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}
}

func (f *fsm) stopInput() {
	os.Stdin.Close()
}

func (f *fsm) sendMessage(message []byte) {
	if len(message) > maxMessageSize {
		slog.Warn("message too large, dropping", "size", len(message))
		return
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(message)))
	typeByte := []byte{byte(protocol.MessageTypeChat)}
	bf := net.Buffers{typeByte, header, message}
	_, err := bf.WriteTo(f.conn)
	if err != nil {
		f.event <- event{evtError, []byte{}, err}
		return
	}
}

func (f *fsm) sendACK() {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 0)
	typeByte := []byte{byte(protocol.MessageTypeACK)}
	bf := net.Buffers{typeByte, header, []byte{}}
	_, err := bf.WriteTo(f.conn)
	if err != nil {
		f.event <- event{evtError, []byte{}, err}
	}
}

func printMessage(messageType protocol.MessageType, payload []byte) {
	switch messageType {
	case protocol.MessageTypeChat:
		fmt.Printf("\n[CHAT] %s\n", payload)
	case protocol.MessageTypeShutdown:
		fmt.Println("\n[SERVER] Server is shutting down...")
	default:
		fmt.Printf("\n[UNKNOWN] type=%d payload=%q\n", messageType, payload)
	}
}
