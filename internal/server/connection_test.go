package server

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestReadMessage(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		wantType    protocol.MessageType
		wantPayload []byte
		wantErr     error
		wantError   bool
	}{
		{
			name:        "valid message",
			input:       makeFrame(protocol.MessageTypeChat, []byte("hello")),
			wantType:    protocol.MessageTypeChat,
			wantPayload: []byte("hello"),
		},
		{
			name:        "empty payload",
			input:       makeFrame(protocol.MessageTypeChat, nil),
			wantType:    protocol.MessageTypeChat,
			wantPayload: []byte{},
		},
		{
			name:        "maximum payload size",
			input:       makeFrame(protocol.MessageTypeChat, make([]byte, maxMessageSize)),
			wantType:    protocol.MessageTypeChat,
			wantPayload: make([]byte, maxMessageSize),
		},
		{
			name:    "EOF before header",
			wantErr: io.EOF,
		},
		{
			name:    "incomplete header",
			input:   []byte{1, 0, 0},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:      "message too large",
			input:     makeFrameWithSize(protocol.MessageTypeChat, maxMessageSize+1),
			wantError: true,
		},
		{
			name:    "incomplete payload",
			input:   append(makeFrameWithSize(protocol.MessageTypeChat, 5), []byte("he")...),
			wantErr: io.ErrUnexpectedEOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverConn, clientConn := net.Pipe()
			defer serverConn.Close()

			done := make(chan struct{})

			go func() {
				defer close(done)
				defer clientConn.Close()

				if len(tt.input) > 0 {
					_, _ = clientConn.Write(tt.input)
				}
			}()

			gotType, gotPayload, err := readMessage(serverConn)
			<-done

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantType, gotType)
			require.Equal(t, tt.wantPayload, gotPayload)
		})
	}
}

func TestReadMessagePayloadTimeout(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	conn := &deadlineConn{
		Conn:    serverConn,
		timeout: 50 * time.Millisecond,
	}

	go func() {
		header := makeFrameWithSize(protocol.MessageTypeChat, 5)
		_, _ = clientConn.Write(header)
	}()

	_, _, err := readMessage(conn)

	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestReadMessageIncompletePayload(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go func() {
		frame := append(
			makeFrameWithSize(protocol.MessageTypeChat, 5),
			[]byte("he")...,
		)
		_, _ = clientConn.Write(frame)
		clientConn.Close()
	}()

	_, _, err := readMessage(serverConn)

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

type deadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c *deadlineConn) SetReadDeadline(t time.Time) error {
	if !t.IsZero() {
		t = time.Now().Add(c.timeout)
	}

	return c.Conn.SetReadDeadline(t)
}

func makeFrame(messageType protocol.MessageType, payload []byte) []byte {
	frame := makeFrameWithSize(messageType, uint32(len(payload)))
	return append(frame, payload...)
}

func makeFrameWithSize(messageType protocol.MessageType, size uint32) []byte {
	frame := make([]byte, 5)
	frame[0] = byte(messageType)
	binary.BigEndian.PutUint32(frame[1:], size)
	return frame
}
