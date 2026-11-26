# Broadcast Server

A TCP broadcast messaging server with an interactive client and a load testing utility.

> Project task: [Broadcast Server – roadmap.sh](https://roadmap.sh/projects/broadcast-server)

## Project Structure

```text
cmd/
├── broadcast-server/    # Main binary (server + interactive client)
└── loadtest/            # Load testing utility

internal/
├── client/              # Client logic (FSM, reader/writer)
├── protocol/            # Message protocol definition
└── server/              # Server logic (hub, message buffer, connections)
```

## Building

The project produces two separate binaries. Build them independently:

```bash
# Server & interactive client
go build -o bin/broadcast-server ./cmd/broadcast-server

# Load test utility
go build -o bin/loadtest ./cmd/loadtest
```
## Usage
### Start the Server
```bash 
./bin/broadcast-server start
```
The server listens on localhost:8082.

### Interactive Client
```bash 
./bin/broadcast-server connect
```

Connects to the server and allows real-time message exchange.

Use Ctrl+C for graceful disconnection.

### Load Testing
``` bash
./bin/loadtest \
  -addr=localhost:8082 \
  -clients=10 \
  -messages=100 \
  -delay=10ms
```

## Architecture
- Server — hub pattern: client registration/unregistration, fan-out broadcasting, message buffering with replay for newly connected clients.

- Client — finite state machine (FSM) with states: Initializing → Connected → ShuttingDown/Disconnecting → Disconnected. Handles both local and remote shutdown gracefully via TCP CloseWrite.

- Protocol — binary format: [1 byte type][4 bytes length BE][payload]. Maximum message size is 256 bytes.

- Graceful shutdown — the server sends a Shutdown message; the client responds with an ACK before closing the connection.

## Requirements
Go 1.21+