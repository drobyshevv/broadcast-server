package broadcast

import (
	"errors"
	"log/slog"
	"maps"
	"net"
	"os"
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
	client.Close()
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, client)
}

func (h *Hub) SendMessage(sender net.Conn, message string) {
	h.mu.RLock()
	snapshot := slices.Collect(maps.Keys(h.clients))
	h.mu.RUnlock()
	for _, client := range snapshot {
		if client == sender {
			continue
		}
		client.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, err := client.Write([]byte(message + "\n"))
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				slog.Warn("write timeout, dropping client", "addr", client.RemoteAddr())
			} else {
				slog.Error("failed to send message", "addr", client.RemoteAddr(), "error", err)
			}
			h.Delete(client)
		}
	}
}
