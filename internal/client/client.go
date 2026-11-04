package client

import (
	"encoding/binary"
	"net"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

type Client struct {
	Conn     net.Conn
	Outbound chan *protocol.Message
}

func NewClient(conn net.Conn) *Client {
	cl := &Client{
		Conn:     conn,
		Outbound: make(chan *protocol.Message, 256),
	}
	go cl.write()
	return cl
}

func (c *Client) write() {
	defer c.Conn.Close()
	for message := range c.Outbound {
		typeByte := []byte{byte(message.Type)}
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(message.Payload)))
		bf := net.Buffers{typeByte, header, message.Payload}
		if err := c.Conn.SetWriteDeadline(time.Now().Add(time.Second * 3)); err != nil {
			return
		}
		if _, err := bf.WriteTo(c.Conn); err != nil {
			return
		}
		if err := c.Conn.SetWriteDeadline(time.Time{}); err != nil {
			return
		}
	}
}
