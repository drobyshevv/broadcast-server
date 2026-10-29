package client

import (
	"encoding/binary"
	"net"
)

type Client struct {
	Conn net.Conn
	Buf  chan []byte
}

func NewClient(conn net.Conn) *Client {
	cl := &Client{
		Conn: conn,
		Buf:  make(chan []byte, 256),
	}
	go cl.write()
	return cl
}

func (c *Client) write() {
	for message := range c.Buf {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(message)))
		bf := net.Buffers{header, []byte(message)}
		bf.WriteTo(c.Conn)
	}
}
