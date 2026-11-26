package server

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

const testTimeout = 2 * time.Second

func newTestClient(buf int) *client {
	return &client{outbound: make(chan *protocol.Message, buf)}
}

func startHub(t *testing.T) (*hub, func()) {
	t.Helper()
	h := newHub()
	go h.run()
	var once sync.Once
	stop := func() { once.Do(func() { close(h.shutdown) }) }
	t.Cleanup(stop)
	return h, stop
}

func chatMsg(payload string) *protocol.Message {
	return protocol.NewMessage(protocol.MessageTypeChat, []byte(payload))
}

func recv(t *testing.T, ch <-chan *protocol.Message) *protocol.Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("expected message, got closed channel")
		}
		return m
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for message")
		return nil
	}
}

func expectPayload(t *testing.T, ch <-chan *protocol.Message, want string) {
	t.Helper()
	m := recv(t, ch)
	if m.Type != protocol.MessageTypeChat {
		t.Fatalf("type = %v, want chat", m.Type)
	}
	if !bytes.Equal(m.Payload, []byte(want)) {
		t.Fatalf("payload = %q, want %q", m.Payload, want)
	}
}

func expectClosed(t *testing.T, ch <-chan *protocol.Message) {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timeout waiting for channel close")
		}
	}
}

func expectNoMessage(t *testing.T, ch <-chan *protocol.Message) {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		t.Fatalf("unexpected message: type=%v payload=%q", m.Type, m.Payload)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitDone(t *testing.T, h *hub) {
	t.Helper()
	select {
	case <-h.done:
	case <-time.After(testTimeout):
		t.Fatal("hub.run did not exit in time")
	}
}

func expectNotDone(t *testing.T, h *hub) {
	t.Helper()
	select {
	case <-h.done:
		t.Fatal("hub.run exited unexpectedly")
	case <-time.After(50 * time.Millisecond):
	}
}

func sendRegister(t *testing.T, h *hub, cl *client) {
	t.Helper()
	select {
	case h.register <- cl:
	case <-h.done:
		t.Fatal("hub exited before register was processed")
	case <-time.After(testTimeout):
		t.Fatal("hub is not processing register")
	}
}

func sendUnregister(t *testing.T, h *hub, cl *client) {
	t.Helper()
	select {
	case h.unregister <- cl:
	case <-h.done:
	case <-time.After(testTimeout):
		t.Fatal("hub is not processing unregister")
	}
}

func sendBroadcast(t *testing.T, h *hub, ev *broadcastEvent) {
	t.Helper()
	select {
	case h.broadcast <- ev:
	case <-h.done:
		t.Fatal("hub exited before broadcast was processed")
	case <-time.After(testTimeout):
		t.Fatal("hub is not processing broadcast")
	}
}

func TestHubBroadcastDeliversToAllExceptSender(t *testing.T) {
	h, _ := startHub(t)

	sender := newTestClient(16)
	r1 := newTestClient(16)
	r2 := newTestClient(16)
	sendRegister(t, h, sender)
	sendRegister(t, h, r1)
	sendRegister(t, h, r2)

	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("hello")})

	expectPayload(t, r1.outbound, "hello")
	expectPayload(t, r2.outbound, "hello")
	expectNoMessage(t, sender.outbound)
}

func TestHubBroadcastNilSenderDeliversToEveryone(t *testing.T) {
	h, _ := startHub(t)

	c1 := newTestClient(16)
	c2 := newTestClient(16)
	sendRegister(t, h, c1)
	sendRegister(t, h, c2)

	sendBroadcast(t, h, &broadcastEvent{sender: nil, message: chatMsg("system")})

	expectPayload(t, c1.outbound, "system")
	expectPayload(t, c2.outbound, "system")
}

func TestHubBroadcastPreservesOrder(t *testing.T) {
	h, _ := startHub(t)

	sender := newTestClient(16)
	receiver := newTestClient(16)
	sendRegister(t, h, sender)
	sendRegister(t, h, receiver)

	want := []string{"m1", "m2", "m3"}
	for _, p := range want {
		sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg(p)})
	}
	for _, p := range want {
		expectPayload(t, receiver.outbound, p)
	}
}

func TestHubDropsSlowClientOnOverflow(t *testing.T) {
	h, _ := startHub(t)

	sender := newTestClient(16)
	slow := newTestClient(1)
	fast := newTestClient(16)
	sendRegister(t, h, sender)
	sendRegister(t, h, slow)
	sendRegister(t, h, fast)

	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("first")})
	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("second")})
	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("third")})

	expectPayload(t, slow.outbound, "first")
	expectClosed(t, slow.outbound)

	expectPayload(t, fast.outbound, "first")
	expectPayload(t, fast.outbound, "second")
	expectPayload(t, fast.outbound, "third")

	sendUnregister(t, h, slow)
	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("still-alive")})
	expectPayload(t, fast.outbound, "still-alive")
	expectNotDone(t, h)
}

func TestHubUnregisterClosesOutboundAndStopsDelivery(t *testing.T) {
	h, _ := startHub(t)

	sender := newTestClient(16)
	leaver := newTestClient(16)
	stayer := newTestClient(16)
	sendRegister(t, h, sender)
	sendRegister(t, h, leaver)
	sendRegister(t, h, stayer)

	sendUnregister(t, h, leaver)
	expectClosed(t, leaver.outbound)

	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("after-leave")})
	expectPayload(t, stayer.outbound, "after-leave")
	expectNoMessage(t, sender.outbound)
}

func TestHubUnregisterUnknownClientIsNoop(t *testing.T) {
	h, _ := startHub(t)

	cl := newTestClient(16)
	sendRegister(t, h, cl)

	stranger := newTestClient(16)
	sendUnregister(t, h, stranger)

	sendBroadcast(t, h, &broadcastEvent{sender: nil, message: chatMsg("still-alive")})
	expectPayload(t, cl.outbound, "still-alive")
	expectNoMessage(t, stranger.outbound)
	expectNotDone(t, h)
}

func TestHubSurvivesWhenAllClientsLeaveWithoutShutdown(t *testing.T) {
	h, _ := startHub(t)

	c1 := newTestClient(16)
	c2 := newTestClient(16)
	sendRegister(t, h, c1)
	sendRegister(t, h, c2)

	sendUnregister(t, h, c1)
	expectClosed(t, c1.outbound)
	expectNotDone(t, h)

	sendUnregister(t, h, c2)
	expectClosed(t, c2.outbound)
	expectNotDone(t, h)

	c3 := newTestClient(16)
	sendRegister(t, h, c3)
	sendBroadcast(t, h, &broadcastEvent{sender: nil, message: chatMsg("revived")})
	expectPayload(t, c3.outbound, "revived")
}

func TestHubReplaysHistoryToLateJoinerInOrder(t *testing.T) {
	h, _ := startHub(t)

	sender := newTestClient(16)
	sendRegister(t, h, sender)

	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("old-1")})
	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("old-2")})

	late := newTestClient(16)
	sendRegister(t, h, late)

	expectPayload(t, late.outbound, "old-1")
	expectPayload(t, late.outbound, "old-2")

	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("live")})
	expectPayload(t, late.outbound, "live")
	expectNoMessage(t, late.outbound)
}

func TestHubReplayNothingWhenHistoryEmpty(t *testing.T) {
	h, _ := startHub(t)

	cl := newTestClient(16)
	sendRegister(t, h, cl)
	expectNoMessage(t, cl.outbound)
}

func TestHubReplayKeepsOnlyLastTenMessages(t *testing.T) {
	h, _ := startHub(t)

	sender := newTestClient(16)
	sendRegister(t, h, sender)

	for i := 1; i <= 12; i++ {
		sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg(fmt.Sprintf("msg-%d", i))})
	}

	late := newTestClient(16)
	sendRegister(t, h, late)

	for i := 3; i <= 12; i++ {
		expectPayload(t, late.outbound, fmt.Sprintf("msg-%d", i))
	}
	expectNoMessage(t, late.outbound)
}

func TestHubShutdownWithoutClientsExitsImmediately(t *testing.T) {
	h, stop := startHub(t)
	stop()
	waitDone(t, h)
}

func TestHubShutdownNotifiesClientsAndWaitsForThem(t *testing.T) {
	h, stop := startHub(t)

	c1 := newTestClient(16)
	c2 := newTestClient(16)
	sendRegister(t, h, c1)
	sendRegister(t, h, c2)

	stop()

	m1 := recv(t, c1.outbound)
	m2 := recv(t, c2.outbound)
	if m1.Type != protocol.MessageTypeShutdown || m2.Type != protocol.MessageTypeShutdown {
		t.Fatalf("expected shutdown messages, got %v and %v", m1.Type, m2.Type)
	}

	expectNotDone(t, h)

	sendUnregister(t, h, c1)
	expectNotDone(t, h)

	sendUnregister(t, h, c2)
	waitDone(t, h)
}

func TestHubRejectsRegistrationAfterShutdown(t *testing.T) {
	h, stop := startHub(t)

	old := newTestClient(16)
	sendRegister(t, h, old)
	stop()

	m := recv(t, old.outbound)
	if m.Type != protocol.MessageTypeShutdown {
		t.Fatalf("type = %v, want shutdown", m.Type)
	}

	late := newTestClient(16)
	sendRegister(t, h, late)
	expectClosed(t, late.outbound)

	sendUnregister(t, h, old)
	waitDone(t, h)
}

func TestHubShutdownDropsSlowClientWithoutShutdownMessage(t *testing.T) {
	h, stop := startHub(t)

	full := newTestClient(1)
	sendRegister(t, h, full)
	sendBroadcast(t, h, &broadcastEvent{sender: nil, message: chatMsg("fill")})

	stop()

	waitDone(t, h)

	expectPayload(t, full.outbound, "fill")
	expectClosed(t, full.outbound)
}

func TestHubBroadcastAfterShutdownStillWorks(t *testing.T) {
	h, stop := startHub(t)

	sender := newTestClient(16)
	receiver := newTestClient(16)
	sendRegister(t, h, sender)
	sendRegister(t, h, receiver)

	stop()

	m1 := recv(t, sender.outbound)
	m2 := recv(t, receiver.outbound)
	if m1.Type != protocol.MessageTypeShutdown || m2.Type != protocol.MessageTypeShutdown {
		t.Fatalf("expected shutdown messages, got %v and %v", m1.Type, m2.Type)
	}

	sendBroadcast(t, h, &broadcastEvent{sender: sender, message: chatMsg("in-flight")})
	expectPayload(t, receiver.outbound, "in-flight")

	sendUnregister(t, h, sender)
	sendUnregister(t, h, receiver)
	waitDone(t, h)
}
