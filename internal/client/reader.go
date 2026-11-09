package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

func (f *fsm) readerLoop() {
	for {
		var header [5]byte

		_, err := io.ReadFull(f.conn, header[:])
		if err != nil {
			f.event <- event{evtError, []byte{}, err}
			return
		}

		messageType := protocol.MessageType(int8(header[0]))
		size := binary.BigEndian.Uint32(header[1:])
		if size > maxMessageSize {
			f.event <- event{evtError, []byte{}, fmt.Errorf("message too large")}
			return
		}
		payload := make([]byte, int(size))

		if err := f.conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			f.event <- event{evtError, []byte{}, err}
			return
		}
		_, err = io.ReadFull(f.conn, payload)
		if err != nil {
			f.event <- event{evtError, []byte{}, err}
			return
		}
		if err := f.conn.SetReadDeadline(time.Time{}); err != nil {
			f.event <- event{evtError, []byte{}, err}
			return
		}
		if messageType == protocol.MessageTypeShutdown {
			f.event <- event{evtRemoteShutdown, []byte{}, nil}
			continue
		}

		f.event <- event{
			kind:    evtServerMessage,
			payload: payload,
		}
	}
}
