package broadcast

import (
	"fmt"
	"maps"
	"net"
	"slices"
	"sync"
	"time"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[net.Conn]struct{}
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[net.Conn]struct{}),
	}
}

func (h *Hub) Add(client net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[client] = struct{}{}
}

func (h *Hub) Delete(client net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, client)
}

func (h *Hub) SendMessage(sender net.Conn, message string) {
	h.mu.RLock()
	snapshot := slices.Collect(maps.Keys(h.clients))
	h.mu.RUnlock()
	for _, v := range snapshot {
		if v == sender {
			continue
		}
		v.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, err := v.Write([]byte(message + "\n"))
		if err != nil {
			fmt.Println(err)
		}
	}
}
