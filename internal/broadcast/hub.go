package broadcast

import (
	"net"

	"github.com/drobyshevv/broadcast-server/internal/client"
)

type Hub struct {
	Register   chan *client.Client
	Unregister chan *client.Client
	Broadcast  chan BroadcastMessage
	Clients    map[*client.Client]struct{}
}

type BroadcastMessage struct {
	Sender  net.Conn
	Message []byte
}

func NewHub() *Hub {
	return &Hub{
		Register:   make(chan *client.Client),
		Unregister: make(chan *client.Client),
		Broadcast:  make(chan BroadcastMessage),
		Clients:    make(map[*client.Client]struct{}),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.Clients[client] = struct{}{}
		case client := <-h.Unregister:
			delete(h.Clients, client)
			close(client.Buf)
		case br := <-h.Broadcast:
			for cl := range h.Clients {
				if br.Sender == cl.Conn {
					continue
				}
				select {
				case cl.Buf <- br.Message:
				default:
					close(cl.Buf)
					delete(h.Clients, cl)
				}

			}
		}
	}
}
