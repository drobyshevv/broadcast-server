package broadcast

import (
	"testing"

	"github.com/drobyshevv/broadcast-server/internal/client"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	mb := NewMessageBuffer()
	messages := [10][]byte{
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
	for i, message := range messages {
		mb.Add(message)
		require.Equal(t, message, mb.buffer[i])
	}
	require.Equal(t, messages, mb.buffer)
}

func TestAddOverflow(t *testing.T) {
	mb := NewMessageBuffer()
	messages := [20][]byte{
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
		{10, 33, 42},
		{33, 25, 16, 88, 33},
		{1},
		{12, 33, 97},
		{1, 4, 77},
		{200, 150, 100},
		{8, 16, 32, 64},
		{77, 88},
		{123, 234, 56},
		{255, 0, 127},
	}
	for i, message := range messages {
		mb.Add(message)
		if i < 10 {
			require.Equal(t, message, mb.buffer[i])
		} else {
			require.Equal(t, message, mb.buffer[i-10])
		}
	}
	require.Equal(t, messages[10:], mb.buffer[:])
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

func TestGive(t *testing.T) {
	mb := NewMessageBuffer()

	for i := 0; i < 10; i++ {
		mb.Add([]byte{byte(i)})
	}

	c := &client.Client{
		Outbound: make(chan []byte, 10),
	}

	mb.ReplayTo(c)

	expected := [][]byte{
		{0}, {1}, {2}, {3}, {4},
		{5}, {6}, {7}, {8}, {9},
	}

	actual := make([][]byte, 0, 10)

	for i := 0; i < 10; i++ {
		actual = append(actual, <-c.Outbound)
	}

	require.Equal(t, expected, actual)
}

func TestGiveAfterOverflow(t *testing.T) {
	mb := NewMessageBuffer()

	for i := 0; i < 15; i++ {
		mb.Add([]byte{byte(i)})
	}

	c := &client.Client{
		Outbound: make(chan []byte, 10),
	}

	mb.ReplayTo(c)

	expected := [][]byte{
		{5}, {6}, {7}, {8}, {9},
		{10}, {11}, {12}, {13}, {14},
	}

	actual := make([][]byte, 0, 10)

	for i := 0; i < 10; i++ {
		actual = append(actual, <-c.Outbound)
	}

	require.Equal(t, expected, actual)
}

func TestGiveEmpty(t *testing.T) {
	mb := NewMessageBuffer()

	c := &client.Client{
		Outbound: make(chan []byte, 10),
	}

	mb.ReplayTo(c)

	require.Empty(t, c.Outbound)
}

func TestGivePartialBuffer(t *testing.T) {
	mb := NewMessageBuffer()

	messages := [][]byte{
		{10},
		{20},
		{30},
	}

	for _, message := range messages {
		mb.Add(message)
	}

	c := &client.Client{
		Outbound: make(chan []byte, 10),
	}

	mb.ReplayTo(c)

	actual := make([][]byte, 0, len(messages))

	for range messages {
		actual = append(actual, <-c.Outbound)
	}

	require.Equal(t, messages, actual)
}
