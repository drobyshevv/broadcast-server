package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

const maxMessageSize = 1 << 8

type Config struct {
	Addr     string
	NClients int
	Delay    time.Duration
	Messages int
}

func main() {
	cfg := loadCfg()
	if cfg == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	for i := 1; i <= cfg.NClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := client(ctx, *cfg, id); err != nil {
				fmt.Printf("client %d: %v\n", id, err)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-done:
		fmt.Println("all clients finished")
	case <-sig.Done():
		cancel()
		select {
		case <-done:
			fmt.Println("loadtest stopped")
		case <-time.After(10 * time.Second):
			fmt.Println("loadtest shutdown timeout")
		}
	}
}

func client(ctx context.Context, cfg Config, id int) error {
	conn, err := net.Dial("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	shutdown := make(chan struct{})
	readDone := make(chan struct{})

	go func() {
		defer close(readDone)
		if err := readMessages(conn, shutdown); err != nil {
			fmt.Printf("client %d read: %v\n", id, err)
		}
	}()

	stop := context.AfterFunc(ctx, func() {
		conn.Close()
	})
	defer stop()

	for i := 1; i <= cfg.Messages; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-shutdown:
			return nil
		default:
		}

		message := []byte(fmt.Sprintf("client %d | message %d", id, i))

		if err := writeMessage(conn, protocol.MessageTypeChat, message); err != nil {
			return fmt.Errorf("failed to send message %d: %w", i, err)
		}

		time.Sleep(cfg.Delay)
	}

	select {
	case <-readDone:
	case <-ctx.Done():
		return nil
	}

	return nil
}

func readMessages(conn net.Conn, shutdown chan<- struct{}) error {
	for {
		var header [5]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read header: %w", err)
		}

		messageType := protocol.MessageType(int8(header[0]))
		size := binary.BigEndian.Uint32(header[1:])

		if size > maxMessageSize {
			return fmt.Errorf("message too large: %d", size)
		}

		payload := make([]byte, int(size))
		if _, err := io.ReadFull(conn, payload); err != nil {
			return fmt.Errorf("failed to read payload: %w", err)
		}

		switch messageType {
		case protocol.MessageTypeChat:
			fmt.Println(string(payload))

		case protocol.MessageTypeShutdown:
			if err := writeMessage(conn, protocol.MessageTypeACK, nil); err != nil {
				return fmt.Errorf("failed to send ACK: %w", err)
			}

			close(shutdown)
			return nil

		default:
			return fmt.Errorf("unknown message type: %d", messageType)
		}
	}
}

func writeMessage(conn net.Conn, messageType protocol.MessageType, payload []byte) error {
	var header [5]byte
	header[0] = byte(messageType)
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))

	bf := net.Buffers{header[:], payload}
	_, err := bf.WriteTo(conn)
	return err
}

func loadCfg() *Config {
	var addr string
	var nClients int
	var delay time.Duration
	var messages int

	flag.StringVar(&addr, "addr", "localhost:8082", "server address")
	flag.IntVar(&nClients, "clients", 1, "number of clients")
	flag.DurationVar(&delay, "delay", time.Millisecond, "delay between messages")
	flag.IntVar(&messages, "messages", 1, "messages per client")
	flag.Parse()

	if addr == "" || nClients < 1 || delay < time.Microsecond || messages < 1 {
		return nil
	}

	return &Config{
		Addr:     addr,
		NClients: nClients,
		Delay:    delay,
		Messages: messages,
	}
}
