package server

import (
	"encoding/binary"
	"net"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

type client struct {
	conn     net.Conn
	outbound chan *protocol.Message
}

func NewClient(conn net.Conn) *client {
	cl := &client{
		conn:     conn,
		outbound: make(chan *protocol.Message, 256),
	}
	go cl.write()
	return cl
}

func (c *client) write() {
	defer c.conn.Close()
	for message := range c.outbound {
		typeByte := []byte{byte(message.Type)}
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(message.Payload)))
		bf := net.Buffers{typeByte, header, message.Payload}
		if err := c.conn.SetWriteDeadline(time.Now().Add(time.Second * 3)); err != nil {
			return
		}
		if _, err := bf.WriteTo(c.conn); err != nil {
			return
		}
		if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
			return
		}
	}
}
