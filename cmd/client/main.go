package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

func main() {

}

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
	Disconnected  StateFSM = "Disconnected"
	Connected     StateFSM = "Connected"
	ShuttingDown  StateFSM = "Shutting down"
	Disconnecting StateFSM = "Disconnecting"
)

type FSM struct {
	state StateFSM
	// logger
	conn  net.Conn
	event chan Event
}

func NewFSM() *FSM {
	return &FSM{
		state: Disconnected,
		event: make(chan Event, 10),
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

	// slog.Info...
}

func (fsm *FSM) handler(ev Event) {
	for {
		switch fsm.state {
		case Disconnected:
			switch ev.Type {
			case EvtStart:
				fsm.connection()
				fsm.state = Connected
			}

		case Connected:

		case Disconnecting:

		case ShuttingDown:

		}
	}
}

func (fsm *FSM) connection() {
	conn, err := net.Dial("tcp", "localhost:8082")
	if err != nil {
		fsm.event <- Event{EvtError, []byte{}, err}
		return
	}

	fsm.conn = conn

	go fsm.writeLoop()
	go fsm.readLoop()
}

func (fsm *FSM) writeLoop() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		message := scanner.Bytes()
		if len(message) > maxMessageSize {
			// TODO custom error
			fsm.event <- Event{EvtError, []byte{}, fmt.Errorf("message too large")}
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
}

func (fsm *FSM) readLoop() {
	for {
		var header [5]byte
		if err := fsm.conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			fsm.event <- Event{EvtError, []byte{}, err}
			return
		}
		_, err := io.ReadFull(fsm.conn, header[:])
		if err != nil {
			fsm.event <- Event{EvtError, []byte{}, err}
			return
		}
		if err := fsm.conn.SetReadDeadline(time.Time{}); err != nil {
			fsm.event <- Event{EvtError, []byte{}, err}
			return
		}

		messageType := protocol.MessageType(int8(header[0]))
		size := binary.BigEndian.Uint32(header[1:])
		if size > maxMessageSize {
			// TODO custom error
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
		printMessage(messageType, payload)
		if messageType == protocol.MessageTypeShutdown {
			fsm.event <- Event{EvtRemoteShutdown, []byte{}, nil}
			// TODO conn.write close
			return
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
		return
	}
}
