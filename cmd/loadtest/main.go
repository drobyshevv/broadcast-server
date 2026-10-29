package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
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

	wg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 1; i <= cfg.NClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client(ctx, *cfg, id)
		}(i)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		wg.Wait()
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-sig.Done()
	stop()
	cancel()

	select {
	case <-done:
		fmt.Println("server has shut down correctly")
	case <-time.After(time.Second * 10):
		os.Exit(0)
	}
}

func client(ctx context.Context, cfg Config, id int) error {
	conn, err := net.Dial("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("Client %d failed to conn to server: %w", id, err)
	}
	defer conn.Close()

	go func() {
		for {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			buf := make([]byte, 4)
			_, err := io.ReadFull(conn, buf)
			if err != nil {
				if errors.Is(err, os.ErrDeadlineExceeded) {
					return
				}
				return
			}

			size := binary.BigEndian.Uint32(buf)
			if size > maxMessageSize {
				return
			}
			payload := make([]byte, int(size))

			_, err = io.ReadFull(conn, payload)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				if errors.Is(err, os.ErrDeadlineExceeded) {
					return
				}
				return
			}
			fmt.Println(string(payload))
		}
	}()

	stop := context.AfterFunc(ctx, func() {
		conn.Close()
	})
	defer stop()

	for i := 1; i <= cfg.Messages; i++ {
		message := fmt.Sprintf("client %d | message %d", id, i)
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(message)))
		bf := net.Buffers{header, []byte(message)}
		_, err := bf.WriteTo(conn)
		if err != nil {
			return fmt.Errorf("failed to send message: %w, client %d | message %d", err, id, i)
		}
		time.Sleep(cfg.Delay)
	}
	return nil
}

func loadCfg() *Config {
	var addr string
	var nClients int
	var delay time.Duration
	var messages int

	flag.StringVar(&addr, "addr", "", "")
	flag.IntVar(&nClients, "clients", 0, "")
	flag.DurationVar(&delay, "delay", time.Millisecond, "")
	flag.IntVar(&messages, "messages", 1, "")
	flag.Parse()
	if addr == "" {
		return nil
	}
	if nClients == 0 {
		return nil
	}
	if delay < time.Microsecond {
		return nil
	}
	if messages < 1 {
		return nil
	}

	return &Config{
		Addr:     addr,
		NClients: nClients,
		Delay:    delay,
		Messages: messages,
	}
}
