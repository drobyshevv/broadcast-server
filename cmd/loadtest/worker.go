package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/drobyshevv/broadcast-server/internal/protocol"
)

type worker struct {
	cfg            config
	conn           net.Conn
	stats          *stats
	remoteShutdown chan struct{}
}

func newWorker(cfg config, stats *stats) *worker {
	return &worker{
		cfg:            cfg,
		stats:          stats,
		remoteShutdown: make(chan struct{}),
	}
}

func (w *worker) run(ctx context.Context) {
	conn, err := net.Dial("tcp", w.cfg.Addr)
	if err != nil {
		atomic.AddUint64(&w.stats.totalErrors, 1)
		return
	}

	w.conn = conn
	defer w.conn.Close()
	go func() {
		<-ctx.Done()
		w.conn.Close()
	}()

	go w.reader()

	for i := 0; i < w.cfg.Messages; i++ {
		select {
		case <-ctx.Done():
			return
		case <-w.remoteShutdown:
			w.sendAck()
			return
		default:
			w.writer()
		}
		time.Sleep(w.cfg.Delay)
	}
}

func (w *worker) reader() {
	for {
		var header [5]byte
		if _, err := io.ReadFull(w.conn, header[:]); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				atomic.AddUint64(&w.stats.totalErrors, 1)
			}
			return
		}

		messageType := protocol.MessageType(int8(header[0]))
		size := binary.BigEndian.Uint32(header[1:])

		if size > maxMessageSize {
			atomic.AddUint64(&w.stats.totalErrors, 1)
			return
		}

		payload := make([]byte, int(size))
		if _, err := io.ReadFull(w.conn, payload); err != nil {
			atomic.AddUint64(&w.stats.totalErrors, 1)
			return
		}

		switch messageType {
		case protocol.MessageTypeChat:
			atomic.AddUint64(&w.stats.totalReceived, 1)
		case protocol.MessageTypeShutdown:
			close(w.remoteShutdown)
			return
		default:
		}
	}
}

func (w *worker) writer() {
	var header [5]byte
	header[0] = byte(protocol.MessageTypeChat)
	payload := []byte("This is a test message for load testing.")
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	bf := net.Buffers{header[:], payload}
	_, err := bf.WriteTo(w.conn)
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			atomic.AddUint64(&w.stats.totalErrors, 1)
		}
		return
	}
	atomic.AddUint64(&w.stats.totalSent, 1)
}

func (w *worker) sendAck() {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 0)
	typeByte := []byte{byte(protocol.MessageTypeACK)}
	bf := net.Buffers{typeByte, header, []byte{}}
	_, err := bf.WriteTo(w.conn)
	if err != nil {
		atomic.AddUint64(&w.stats.totalErrors, 1)
		return
	}
}
