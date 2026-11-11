package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxMessageSize = 1 << 8

type config struct {
	Addr     string
	NClients int
	Delay    time.Duration
	Messages int
}

type stats struct {
	totalSent     uint64
	totalReceived uint64
	totalErrors   uint64
	startTime     time.Time
}

func main() {
	cfg := loadCfg()
	if cfg == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	st := &stats{
		totalSent:     0,
		totalReceived: 0,
		totalErrors:   0,
		startTime:     time.Now(),
	}

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				fmt.Println()
				return
			case <-ticker.C:
				fmt.Printf("\033[H")

				fmt.Printf("\033[K\033[32m Sent:     %d\033[0m\n", atomic.LoadUint64(&st.totalSent))
				fmt.Printf("\033[K\033[34m Received: %d\033[0m\n", atomic.LoadUint64(&st.totalReceived))
				fmt.Printf("\033[K\033[31m Errors:   %d\033[0m\n", atomic.LoadUint64(&st.totalErrors))
				fmt.Printf("\033[K\033[33m Time:     %.2fs\033[0m\n", time.Since(st.startTime).Seconds())

				fmt.Printf("\033[J")
			}
		}
	}()

	for i := 1; i <= cfg.NClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrk := newWorker(*cfg, st)
			wrk.run(ctx)
		}()
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
		fmt.Println("\nall clients finished")
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

func loadCfg() *config {
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

	return &config{
		Addr:     addr,
		NClients: nClients,
		Delay:    delay,
		Messages: messages,
	}
}
