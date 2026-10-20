package main

import (
	"fmt"
	"net"

	"github.com/drobyshevv/broadcast-server/internal/broadcast"
	"github.com/drobyshevv/broadcast-server/internal/handler"
)

func main() {
	l, err := net.Listen("tcp", "localhost:8082")
	if err != nil {
		fmt.Println("failed to conn", err)
	}

	h := broadcast.NewHub()

	for {
		conn, err := l.Accept()
		if err != nil {
			println(err)
			continue
		}
		h.Add(conn)
		go handler.HandleConnection(conn, h)
	}
}
