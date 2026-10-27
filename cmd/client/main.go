package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	conn, err := net.Dial("tcp", "localhost:8082")
	if err != nil {
		slog.Error("failed to connection to server", "error", err)
		panic(err)
	}
	go Read(conn)
	scannerWriter := bufio.NewScanner(os.Stdin)
	for scannerWriter.Scan() {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(scannerWriter.Bytes())))
		bf := net.Buffers{header, scannerWriter.Bytes()}
		_, err := bf.WriteTo(conn)
		if err != nil {
			break
		}
	}
}

func Read(conn net.Conn) {
	scannerReader := bufio.NewScanner(conn)
	for scannerReader.Scan() {
		fmt.Println(scannerReader.Text())
		if scannerReader.Err() != nil {
			break
		}
	}
}
