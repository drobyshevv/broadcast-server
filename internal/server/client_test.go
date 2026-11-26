package server

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

const clientTestTimeout = 2 * time.Second

func readClientMessage(t *testing.T, conn net.Conn) *protocol.Message {
	t.Helper()

	_ = conn.SetReadDeadline(time.Now().Add(clientTestTimeout))

	var header [5]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	size := binary.BigEndian.Uint32(header[1:])

	payload := make([]byte, size)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("failed to read payload: %v", err)
	}

	return protocol.NewMessage(
		protocol.MessageType(header[0]),
		payload,
	)
}

func TestClientWriteMessage(t *testing.T) {
	server, peer := net.Pipe()
	defer peer.Close()

	cl := NewClient(server)

	message := protocol.NewMessage(
		protocol.MessageTypeChat,
		[]byte("hello"),
	)

	cl.outbound <- message

	got := readClientMessage(t, peer)

	if got.Type != message.Type {
		t.Fatalf("type = %v, want %v", got.Type, message.Type)
	}

	if string(got.Payload) != string(message.Payload) {
		t.Fatalf("payload = %q, want %q", got.Payload, message.Payload)
	}

	close(cl.outbound)
}

func TestClientWriteMultipleMessages(t *testing.T) {
	server, peer := net.Pipe()
	defer peer.Close()

	cl := NewClient(server)

	messages := []*protocol.Message{
		protocol.NewMessage(protocol.MessageTypeChat, []byte("first")),
		protocol.NewMessage(protocol.MessageTypeChat, []byte("second")),
		protocol.NewMessage(protocol.MessageTypeChat, []byte("third")),
	}

	for _, message := range messages {
		cl.outbound <- message
	}

	for i, want := range messages {
		got := readClientMessage(t, peer)

		if got.Type != want.Type {
			t.Fatalf("[%d] type = %v, want %v", i, got.Type, want.Type)
		}

		if string(got.Payload) != string(want.Payload) {
			t.Fatalf("[%d] payload = %q, want %q",
				i, got.Payload, want.Payload)
		}
	}

	close(cl.outbound)
}

func TestClientWriteClosesConnection(t *testing.T) {
	server, peer := net.Pipe()

	cl := NewClient(server)
	close(cl.outbound)

	done := make(chan struct{})

	go func() {
		defer close(done)

		var buf [1]byte
		_, err := peer.Read(buf[:])

		if !errors.Is(err, io.EOF) {
			t.Errorf("read error = %v, want EOF", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(clientTestTimeout):
		t.Fatal("connection was not closed")
	}

	_ = peer.Close()
}

type failingConn struct {
	net.Conn
	once sync.Once
	done chan struct{}
}

func (c *failingConn) SetWriteDeadline(time.Time) error {
	return errors.New("write deadline error")
}

func (c *failingConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		close(c.done)
	})
	return err
}

func TestClientWriteStopsOnWriteError(t *testing.T) {
	server, peer := net.Pipe()
	defer peer.Close()

	conn := &failingConn{
		Conn: server,
		done: make(chan struct{}),
	}

	cl := NewClient(conn)

	cl.outbound <- protocol.NewMessage(
		protocol.MessageTypeChat,
		[]byte("hello"),
	)

	select {
	case <-conn.done:
	case <-time.After(clientTestTimeout):
		t.Fatal("connection was not closed after write error")
	}

	select {
	case cl.outbound <- protocol.NewMessage(
		protocol.MessageTypeChat,
		[]byte("after-error"),
	):
	case <-time.After(clientTestTimeout):
		t.Fatal("unexpected blocking while sending message")
	}
}
