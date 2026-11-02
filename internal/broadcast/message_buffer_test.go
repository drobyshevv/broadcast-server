package broadcast

import (
	"testing"

	"github.com/drobyshevv/broadcast-server/internal/client"
	"github.com/drobyshevv/broadcast-server/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	mb := NewMessageBuffer()

	messages := [][]byte{
		{10, 29},
		{1, 2, 3},
		{100, 200},
		{42},
		{5, 10, 15, 20},
		{255, 128},
		{7, 14, 21},
		{50, 60, 70},
		{0, 1},
		{99, 98, 97},
	}

	for _, message := range messages {
		mb.Add(message)
	}

	require.Equal(t, 10, mb.size)
	require.Equal(t, 0, mb.next)
}

func TestAddOverflow(t *testing.T) {
	mb := NewMessageBuffer()

	for i := 0; i < 20; i++ {
		mb.Add([]byte{byte(i)})
	}

	require.Equal(t, 10, mb.size)
	require.Equal(t, 0, mb.next)
}

func TestAddHeadWrapAround(t *testing.T) {
	mb := NewMessageBuffer()

	for i := 0; i < 10; i++ {
		mb.Add([]byte{byte(i)})
	}

	require.Equal(t, 0, mb.next)

	mb.Add([]byte{100})

	require.Equal(t, 1, mb.next)

	mb.Add([]byte{101})

	require.Equal(t, 2, mb.next)
}

func TestReplayTo(t *testing.T) {
	mb := NewMessageBuffer()

	for i := 0; i < 10; i++ {
		mb.Add([]byte{byte(i)})
	}

	c := &client.Client{
		Outbound: make(chan *protocol.Message, 10),
	}

	mb.ReplayTo(c)

	assertMessages(t, c.Outbound, 0, 10)
}

func TestReplayToAfterOverflow(t *testing.T) {
	mb := NewMessageBuffer()

	for i := 0; i < 15; i++ {
		mb.Add([]byte{byte(i)})
	}

	c := &client.Client{
		Outbound: make(chan *protocol.Message, 10),
	}

	mb.ReplayTo(c)

	assertMessages(t, c.Outbound, 5, 15)
}

func TestReplayToEmpty(t *testing.T) {
	mb := NewMessageBuffer()

	c := &client.Client{
		Outbound: make(chan *protocol.Message, 10),
	}

	mb.ReplayTo(c)

	require.Empty(t, c.Outbound)
}

func TestReplayToPartialBuffer(t *testing.T) {
	mb := NewMessageBuffer()

	for _, value := range []byte{10, 20, 30} {
		mb.Add([]byte{value})
	}

	c := &client.Client{
		Outbound: make(chan *protocol.Message, 10),
	}

	mb.ReplayTo(c)

	expected := [][]byte{
		{10},
		{20},
		{30},
	}

	for _, want := range expected {
		msg := <-c.Outbound
		require.Equal(t, protocol.MessageTypeChat, msg.Type)
		require.Equal(t, want, msg.Payload)
	}

	require.Empty(t, c.Outbound)
}

func assertMessages(t *testing.T, ch <-chan *protocol.Message, start, end int) {
	t.Helper()

	for i := start; i < end; i++ {
		msg := <-ch

		require.Equal(t, protocol.MessageTypeChat, msg.Type)
		require.Equal(t, []byte{byte(i)}, msg.Payload)
	}

	require.Empty(t, ch)
}
