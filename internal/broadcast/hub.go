package broadcast

import (
	"github.com/drobyshevv/broadcast-server/internal/client"
)

type Hub struct {
	Register   chan *client.Client
	Unregister chan *client.Client
	Broadcast  chan *BroadcastMessage
	Clients    map[*client.Client]struct{}
	Buffer     *MessageBuffer
}

type BroadcastMessage struct {
	Sender  *client.Client
	Message []byte
}

func NewHub() *Hub {
	return &Hub{
		Register:   make(chan *client.Client),
		Unregister: make(chan *client.Client),
		Broadcast:  make(chan *BroadcastMessage),
		Clients:    make(map[*client.Client]struct{}),
		Buffer:     NewMessageBuffer(),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.Clients[client] = struct{}{}
			h.Buffer.ReplayTo(client)
		case client := <-h.Unregister:
			delete(h.Clients, client)
			close(client.Outbound)
		case br := <-h.Broadcast:
			h.Buffer.Add(br.Message)
			for cl := range h.Clients {
				if br.Sender == cl {
					continue
				}
				select {
				case cl.Outbound <- br.Message:
				default:
					close(cl.Outbound)
					delete(h.Clients, cl)
				}

			}
		}
	}
}
