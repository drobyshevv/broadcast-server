package broadcast

import (
	"github.com/drobyshevv/broadcast-server/internal/client"
	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

type Hub struct {
	Register   chan *client.Client
	Unregister chan *client.Client
	Broadcast  chan *BroadcastEvent
	Shutdown   chan struct{}
	Done       chan struct{}
	Clients    map[*client.Client]struct{}
	Buffer     *MessageBuffer
}

type BroadcastEvent struct {
	Sender  *client.Client
	Message *protocol.Message
}

func NewHub() *Hub {
	return &Hub{
		Register:   make(chan *client.Client),
		Unregister: make(chan *client.Client),
		Broadcast:  make(chan *BroadcastEvent),
		Shutdown:   make(chan struct{}),
		Done:       make(chan struct{}),
		Clients:    make(map[*client.Client]struct{}),
		Buffer:     NewMessageBuffer(),
	}
}

func (h *Hub) Run() {
	defer close(h.Done)
	for {
		select {
		case client := <-h.Register:
			if h.Shutdown == nil {
				close(client.Outbound)
				continue
			}
			h.Clients[client] = struct{}{}
			h.Buffer.ReplayTo(client)
		case client := <-h.Unregister:
			if _, ok := h.Clients[client]; ok {
				delete(h.Clients, client)
				close(client.Outbound)
			}
			if h.Shutdown == nil && len(h.Clients) == 0 {
				return
			}
		case br := <-h.Broadcast:
			h.Buffer.Add(br.Message.Payload)
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
		case <-h.Shutdown:
			for cl := range h.Clients {
				cl.Outbound <- protocol.NewMessage(protocol.MessageTypeShutdown, []byte{})
			}
			h.Shutdown = nil

			if len(h.Clients) == 0 {
				return
			}
		}
	}
}
