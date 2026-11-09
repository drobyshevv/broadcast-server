package server

import (
	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

type hub struct {
	register   chan *client
	unregister chan *client
	broadcast  chan *broadcastEvent
	shutdown   chan struct{}
	done       chan struct{}
	clients    map[*client]struct{}
	buffer     *messageBuffer
}

type broadcastEvent struct {
	sender  *client
	message *protocol.Message
}

func newHub() *hub {
	return &hub{
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan *broadcastEvent),
		shutdown:   make(chan struct{}),
		done:       make(chan struct{}),
		clients:    make(map[*client]struct{}),
		buffer:     newMessageBuffer(),
	}
}

func (h *hub) run() {
	defer close(h.done)
	for {
		select {
		case client := <-h.register:
			if h.shutdown == nil {
				close(client.outbound)
				continue
			}
			h.clients[client] = struct{}{}
			h.buffer.replayTo(client)
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.outbound)
			}
			if h.shutdown == nil && len(h.clients) == 0 {
				return
			}
		case br := <-h.broadcast:
			h.buffer.add(br.message.Payload)
			for cl := range h.clients {
				if br.sender == cl {
					continue
				}
				select {
				case cl.outbound <- br.message:
				default:
					close(cl.outbound)
					delete(h.clients, cl)
				}
			}
		case <-h.shutdown:
			for cl := range h.clients {
				select {
				case cl.outbound <- protocol.NewMessage(protocol.MessageTypeShutdown, []byte{}):
				default:
					close(cl.outbound)
					delete(h.clients, cl)
				}
			}
			h.shutdown = nil

			if len(h.clients) == 0 {
				return
			}
		}
	}
}
