package broadcast

import (
	"github.com/drobyshevv/broadcast-server/internal/client"
)

type MessageBuffer struct {
	buffer [10][]byte
	next   int
	size   int
}

func NewMessageBuffer() *MessageBuffer {
	mb := &MessageBuffer{}
	for i := range mb.buffer {
		mb.buffer[i] = make([]byte, 0, 256)
	}
	return mb
}

func (b *MessageBuffer) ReplayTo(client *client.Client) {
	curr := 0
	if b.size == len(b.buffer) {
		curr = b.next
	}
	for i := 0; i < b.size; i++ {
		client.Outbound <- b.buffer[curr]
		curr = (curr + 1) % len(b.buffer)
	}
}

func (b *MessageBuffer) Add(message []byte) {
	b.buffer[b.next] = append(b.buffer[b.next][:0], message...)
	b.next = (b.next + 1) % len(b.buffer)
	if b.size < len(b.buffer) {
		b.size++
	}
}
