package handler

import (
	"bufio"
	"fmt"
	"net"

	"github.com/drobyshevv/broadcast-server/internal/broadcast"
)

func HandleConnection(conn net.Conn, h *broadcast.Hub) {
	defer conn.Close()
	defer h.Delete(conn)
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	for scanner.Scan() {
		h.SendMessage(conn, scanner.Text())
	}
	if scanner.Err() != nil {
		fmt.Println(scanner.Err().Error())
		return
	}
}
