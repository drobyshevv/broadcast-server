package server

import (
	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

type messageBuffer struct {
	buffer [10][]byte
	next   int
	size   int
}

func newMessageBuffer() *messageBuffer {
	mb := &messageBuffer{}
	for i := range mb.buffer {
		mb.buffer[i] = make([]byte, 0, 256)
	}
	return mb
}

func (b *messageBuffer) replayTo(client *client) {
	curr := 0
	if b.size == len(b.buffer) {
		curr = b.next
	}
	for i := 0; i < b.size; i++ {
		client.outbound <- protocol.NewMessage(protocol.MessageTypeChat, b.buffer[curr])
		curr = (curr + 1) % len(b.buffer)
	}
}

func (b *messageBuffer) add(message []byte) {
	b.buffer[b.next] = append(b.buffer[b.next][:0], message...)
	b.next = (b.next + 1) % len(b.buffer)
	if b.size < len(b.buffer) {
		b.size++
	}
}
