package main

import (
	"bufio"
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

type EventType int

const (
	EvtStart EventType = iota
	EvtUserMessage
	EvtServerMessage
	EvtRemoteShutdown
	EvtLocalShutdown
	EvtError
)

type Event struct {
	Type    EventType
	Payload []byte
	Err     error
}

type StateFSM string

const (
	Initializing  StateFSM = "Initializing"
	Connected     StateFSM = "Connected"
	ShuttingDown  StateFSM = "Shutting down"
	Disconnecting StateFSM = "Disconnecting"
	Disconnected  StateFSM = "Disconnected"
)

type FSM struct {
	state StateFSM
	conn  net.Conn
	event chan Event
	done  chan struct{}
}

func NewFSM() *FSM {
	return &FSM{
		state: Initializing,
		event: make(chan Event, 10),
		done:  make(chan struct{}),
	}
}

func (fsm *FSM) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for fsm.state != Disconnected {
		select {
		case ev := <-fsm.event:
			fsm.handler(ev)
		case <-sigChan:
			fsm.handler(Event{EvtLocalShutdown, []byte{}, nil})
		}
	}

	slog.Info("FSM stopped, final state: Disconnected")
	close(fsm.done)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)
	fsm := NewFSM()
	go fsm.Run()
	fsm.event <- Event{Type: EvtStart}
	<-fsm.done
	slog.Info("Client exited gracefully")
}

func (fsm *FSM) handler(ev Event) {
	switch fsm.state {
	case Initializing:
		if ev.Type == EvtStart {
			fsm.connection()
			fsm.state = Connected
		}

	case Connected:
		switch ev.Type {
		case EvtUserMessage:
			fsm.sendMessage(ev.Payload)

		case EvtServerMessage:
			printMessage(protocol.MessageTypeChat, ev.Payload)

		case EvtRemoteShutdown:
			printMessage(protocol.MessageTypeShutdown, ev.Payload)
			fsm.sendACK()
			fsm.state = Disconnecting

		case EvtLocalShutdown:
			slog.Info("Local shutdown initiated")
			fsm.closeWrite()
			fsm.stopInput()
			fsm.state = ShuttingDown

		case EvtError:
			slog.Error("Unexpected connection error", "error", ev.Err)
			fsm.closeConnection()
			fsm.state = Disconnected
		}

	case Disconnecting:
		switch ev.Type {
		case EvtLocalShutdown:
			slog.Warn("Force closing connection")
			fsm.closeConnection()
			fsm.state = Disconnected

		case EvtError:
			if errors.Is(ev.Err, io.EOF) {
				slog.Info("Server acknowledged shutdown (EOF received)")
			} else {
				slog.Warn("Connection closed with error during shutdown", "error", ev.Err)
			}
			fsm.closeConnection()
			fsm.state = Disconnected
		}

	case ShuttingDown:
		switch ev.Type {
		case EvtError:
			slog.Info("Server responded to CloseWrite (EOF received)")
			fsm.closeConnection()
			fsm.state = Disconnected
		}

	case Disconnected:
		slog.Debug("Ignoring event in Disconnected state", "event", ev.Type)
	}
}

func (fsm *FSM) connection() {
	conn, err := net.Dial("tcp", "localhost:8082")
	if err != nil {
		fsm.event <- Event{EvtError, []byte{}, err}
		return
	}

	fsm.conn = conn

	go fsm.writerLoop()
	go fsm.readerLoop()
}

func (fsm *FSM) closeConnection() {
	if fsm.conn != nil {
		fsm.conn.Close()
	}
}

func (fsm *FSM) closeWrite() {
	if fsm.conn == nil {
		return
	}
	if tcpConn, ok := fsm.conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}
}

func (fsm *FSM) stopInput() {
	os.Stdin.Close()
}

func (fsm *FSM) sendMessage(message []byte) {
	if len(message) > maxMessageSize {
		slog.Warn("message too large, dropping", "size", len(message))
		return
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(message)))
	typeByte := []byte{byte(protocol.MessageTypeChat)}
	bf := net.Buffers{typeByte, header, message}
	_, err := bf.WriteTo(fsm.conn)
	if err != nil {
		fsm.event <- Event{EvtError, []byte{}, err}
		return
	}
}

func (fsm *FSM) writerLoop() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		message := scanner.Bytes()
		fsm.event <- Event{EvtUserMessage, message, nil}
	}
}

func (fsm *FSM) readerLoop() {
	for {
		var header [5]byte

		_, err := io.ReadFull(fsm.conn, header[:])
		if err != nil {
			fsm.event <- Event{EvtError, []byte{}, err}
			return
		}

		messageType := protocol.MessageType(int8(header[0]))
		size := binary.BigEndian.Uint32(header[1:])
		if size > maxMessageSize {
			fsm.event <- Event{EvtError, []byte{}, fmt.Errorf("message too large")}
			return
		}
		payload := make([]byte, int(size))

		if err := fsm.conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			fsm.event <- Event{EvtError, []byte{}, err}
			return
		}
		_, err = io.ReadFull(fsm.conn, payload)
		if err != nil {
			fsm.event <- Event{EvtError, []byte{}, err}
			return
		}
		if err := fsm.conn.SetReadDeadline(time.Time{}); err != nil {
			fsm.event <- Event{EvtError, []byte{}, err}
			return
		}
		if messageType == protocol.MessageTypeShutdown {
			fsm.event <- Event{EvtRemoteShutdown, []byte{}, nil}
			continue
		}

		fsm.event <- Event{
			Type:    EvtServerMessage,
			Payload: payload,
		}
	}
}

func (fsm *FSM) sendACK() {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 0)
	typeByte := []byte{byte(protocol.MessageTypeACK)}
	bf := net.Buffers{typeByte, header, []byte{}}
	_, err := bf.WriteTo(fsm.conn)
	if err != nil {
		fsm.event <- Event{EvtError, []byte{}, err}
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
