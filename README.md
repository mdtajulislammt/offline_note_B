# SuperNote Real-Time Signaling Engine

High-performance, concurrent real-time WebRTC signaling engine and collaborative sync server for the **SuperNote** offline-first collaborative application.

Built in idiomatic Go (1.22+) by a Senior Systems & Distributed Systems Architect.

---

## Architecture Overview

```
                      +---------------------------------------+
                      |       HTTP / WS Reverse Proxy         |
                      +-------------------+-------------------+
                                          |
                                    GET /ws (Upgrade)
                                          |
                             +------------v------------+
                             |     Client Connection   |
                             |  +-------------------+  |
                             |  |   readPump (ws)   |  |
                             |  |  writePump (ws)   |  |
                             |  +---------+---------+  |
                             +------------|------------+
                                          |
                      +-------------------v-------------------+
                      |         Signaling Protocol Router     |
                      |            (snake_case JSON)          |
                      +---+--------------------+----------+---+
                          |                    |          |
            +-------------v----+        +------v---+   +--v---------------+
            |  1-on-1 P2P Room |        | Group CRDT|   |  Public Stream   |
            |   (Pure Relay)   |        | Note Sync |   | (50-msg capped)  |
            +-------------+----+        +------+---+   +--+---------------+
                          |                    |          |
                          +--------------------+----------+
                                          |
                             +------------v------------+
                             |       Hub Coordinator   |
                             |   (Channel Concurrency) |
                             +------------+------------+
                                          |
                      +-------------------v-------------------+
                      |      Redis In-Memory State Store      |
                      |  - Ephemeral 1-on-1: 60s TTL          |
                      |  - Group Members: Sets (24h TTL)      |
                      |  - Public Chat: LPUSH + LTRIM 50      |
                      |  - Atomic Lua scripts for join races  |
                      +---------------------------------------+
```

---

## Core Components

1. **Hub & Connection Manager (`hub.go`)**:
   - Central coordination loop with channel-based concurrency (`register`, `unregister`, `broadcast`).
   - Thread-safe socket descriptor maps (`clients`, `clientsByID`, `groupRooms`, `p2pPairings`).
   - Non-blocking drops for slow consumers to prevent head-of-line blocking across peers.
   - Real-time connection metrics tracking (`PublicMetricsPayload`).

2. **Client Lifecycle & Goroutine Pumps (`client.go`)**:
   - **`readPump`**: Enforces `maxMessageSize` (512 KB), `pongWait` (60s), and continuous pong deadlines.
   - **`writePump`**: Regular heartbeat ticker (`pingPeriod` = 54s) writing `websocket.PingMessage` frames to keep sockets alive and detect half-open TCP connections.
   - Buffer overflow protection and idempotent `Close()` via `sync.Once`.

3. **In-Memory State Store (`redis.go`)**:
   - Ephemeral 1-on-1 WebRTC rooms: `room:p2p:{code}` with strict **60-second TTL**.
   - Atomic pairing via Redis Lua script (`pairP2PRoomLua`) to prevent race conditions when multiple peers attempt to join simultaneously.
   - Group room memberships: Redis Sets (`room:group:{code}`) with automatic key eviction upon last member departure.
   - Public chat stream: Redis List `chat:public:stream` with atomic `LPUSH` + `LTRIM` capped strictly at **50 messages**.

4. **Cryptographic Room Code Generator (`code.go`)**:
   - Generates 8-character codes (`XXXX-XXXX`) using CSPRNG (`crypto/rand.Int`).
   - Uses an unambiguous 31-character alphabet (`23456789ABCDEFGHJKMNPQRSTUVWXYZ`) excluding visually ambiguous characters: `0`, `O`, `1`, `I`, `L`.
   - Free from modulo bias; benchmarked at over 1.4 million codes/sec.

5. **Signaling Protocol Handlers (`signaling.go`)**:
   - **Public Stream**: Join with alias, real-time message broadcast, 50-item message history retrieval, peer presence notifications.
   - **1-on-1 P2P Signaling**:
     - `p2p_create`: Generates 8-char PIN, registers host in Redis (60s TTL).
     - `p2p_join`: Validates PIN, pairs host and peer, notifies both sides.
     - `p2p_signal`: Pure relay of WebRTC SDP offers/answers and ICE candidates as `json.RawMessage` without decoding overhead.
   - **Group Collaboration**:
     - `group_join` & `group_leave`: Joins/leaves group room descriptor.
     - `group_note_sync`: Pure relay of collaborative note deltas (CRDT / OT / markdown) to all members of a room except sender.

---

## Wire Protocol Specification

All WebSocket frames adhere to the snake_case JSON envelope:

```json
{
  "type": "<message_type>",
  "room": "<optional_room_code>",
  "from": "<optional_sender_id>",
  "to": "<optional_recipient_id>",
  "payload": {}
}
```

### 1. 1-on-1 P2P WebRTC Signaling

#### Create Room (`p2p_create`)
```json
// Client -> Server
{ "type": "p2p_create" }

// Server -> Host
{
  "type": "p2p_created",
  "room": "RT92-VJCD",
  "payload": {
    "code": "RT92-VJCD",
    "host_id": "594f802c-ad2d-471d-8061-39b1fc5ce7fd",
    "ttl_seconds": 60
  }
}
```

#### Join Room (`p2p_join`)
```json
// Peer -> Server
{
  "type": "p2p_join",
  "room": "RT92-VJCD"
}

// Server -> Peer
{
  "type": "p2p_joined",
  "room": "RT92-VJCD",
  "payload": {
    "peer_id": "594f802c-ad2d-471d-8061-39b1fc5ce7fd",
    "role": "peer"
  }
}

// Server -> Host
{
  "type": "p2p_peer_joined",
  "room": "RT92-VJCD",
  "payload": {
    "peer_id": "2143135a-1135-47ac-9cce-071a8e45d12b",
    "role": "host"
  }
}
```

#### Relay WebRTC Signal (`p2p_signal`)
Pure relay of arbitrary SDP offers/answers or ICE candidates:
```json
// Host -> Server
{
  "type": "p2p_signal",
  "room": "RT92-VJCD",
  "payload": {
    "sdp": {
      "type": "offer",
      "sdp": "v=0\r\no=- 12345 2 IN IP4 127.0.0.1\r\ns=-\r\n"
    }
  }
}

// Server -> Peer (Forwarded with pure payload preserved)
{
  "type": "p2p_signal",
  "room": "RT92-VJCD",
  "from": "594f802c-ad2d-471d-8061-39b1fc5ce7fd",
  "to": "2143135a-1135-47ac-9cce-071a8e45d12b",
  "payload": {
    "sdp": {
      "type": "offer",
      "sdp": "v=0\r\no=- 12345 2 IN IP4 127.0.0.1\r\ns=-\r\n"
    }
  }
}
```

---

### 2. Group Note Synchronization

```json
// Member -> Server
{
  "type": "group_join",
  "room": "TEAM-ROOM-42"
}

// Relay note deltas (CRDT / Automerge / Yjs updates)
{
  "type": "group_note_sync",
  "room": "TEAM-ROOM-42",
  "payload": {
    "ops": [
      { "insert": "# Project Strategy\n" },
      { "insert": "Distributed consensus achieved." }
    ],
    "clock": { "client_a": 42 }
  }
}
```

---

### 3. Public Chat Stream

```json
// Join public lobby
{
  "type": "public_join",
  "payload": { "alias": "Alice" }
}

// Send public message
{
  "type": "public_chat",
  "payload": { "text": "Hello SuperNote!" }
}
```

---

## Running and Testing

### Prerequisites
- Go 1.22+
- Redis (running on `localhost:6379` or specified via `REDIS_URL`)

### Running the Server
```bash
go run .
```

### Running Tests with Race Detector
```bash
go test -v -race ./...
```

### Running Benchmarks
```bash
go test -bench=. -benchmem
```
# offline_note_B
