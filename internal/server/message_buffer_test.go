package server

import (
	"testing"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	mb := newMessageBuffer()

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
		mb.add(message)
	}

	require.Equal(t, 10, mb.size)
	require.Equal(t, 0, mb.next)
}

func TestAddOverflow(t *testing.T) {
	mb := newMessageBuffer()

	for i := 0; i < 20; i++ {
		mb.add([]byte{byte(i)})
	}

	require.Equal(t, 10, mb.size)
	require.Equal(t, 0, mb.next)
}

func TestAddHeadWrapAround(t *testing.T) {
	mb := newMessageBuffer()

	for i := 0; i < 10; i++ {
		mb.add([]byte{byte(i)})
	}

	require.Equal(t, 0, mb.next)

	mb.add([]byte{100})

	require.Equal(t, 1, mb.next)

	mb.add([]byte{101})

	require.Equal(t, 2, mb.next)
}

func TestReplayTo(t *testing.T) {
	mb := newMessageBuffer()

	for i := 0; i < 10; i++ {
		mb.add([]byte{byte(i)})
	}

	c := &client{
		outbound: make(chan *protocol.Message, 10),
	}

	mb.replayTo(c)

	assertMessages(t, c.outbound, 0, 10)
}

func TestReplayToAfterOverflow(t *testing.T) {
	mb := newMessageBuffer()

	for i := 0; i < 15; i++ {
		mb.add([]byte{byte(i)})
	}

	c := &client{
		outbound: make(chan *protocol.Message, 10),
	}

	mb.replayTo(c)

	assertMessages(t, c.outbound, 5, 15)
}

func TestReplayToEmpty(t *testing.T) {
	mb := newMessageBuffer()

	c := &client{
		outbound: make(chan *protocol.Message, 10),
	}

	mb.replayTo(c)

	require.Empty(t, c.outbound)
}

func TestReplayToPartialBuffer(t *testing.T) {
	mb := newMessageBuffer()

	for _, value := range []byte{10, 20, 30} {
		mb.add([]byte{value})
	}

	c := &client{
		outbound: make(chan *protocol.Message, 10),
	}

	mb.replayTo(c)

	expected := [][]byte{
		{10},
		{20},
		{30},
	}

	for _, want := range expected {
		msg := <-c.outbound
		require.Equal(t, protocol.MessageTypeChat, msg.Type)
		require.Equal(t, want, msg.Payload)
	}

	require.Empty(t, c.outbound)
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
