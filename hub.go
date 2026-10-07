package main

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Hub maintains the set of active clients and handles channel-based concurrency
// for registration, unregistration, and broadcasting.
type Hub struct {
	// Registered clients indexed by pointer and ID
	clients   map[*Client]bool
	clientsByID map[string]*Client

	// Room tracking: room code -> set of clients
	groupRooms map[string]map[*Client]bool

	// P2P 1-on-1 pairings: code -> P2PPairing
	p2pPairings map[string]*P2PPairing

	// Mutex protecting concurrent reads/writes for fast lookups
	mu sync.RWMutex

	// Inbound events from clients
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte

	// Signaling router and Redis client
	signaling *SignalingRouter
	redis     *RedisStore

	// Metrics
	activeConnections atomic.Int64
	startTime         time.Time

	// Lifecycle
	shutdown chan struct{}
	done     chan struct{}
}

// P2PPairing tracks in-memory paired sockets for 1-on-1 signaling.
type P2PPairing struct {
	Code   string
	Host   *Client
	Peer   *Client
	Status string // "waiting" | "paired"
}

// NewHub creates an unstarted Hub instance.
func NewHub(redisStore *RedisStore) *Hub {
	h := &Hub{
		clients:      make(map[*Client]bool),
		clientsByID:  make(map[string]*Client),
		groupRooms:   make(map[string]map[*Client]bool),
		p2pPairings:  make(map[string]*P2PPairing),
		register:     make(chan *Client, 256),
		unregister:   make(chan *Client, 256),
		broadcast:    make(chan []byte, 512),
		redis:        redisStore,
		startTime:    time.Now(),
		shutdown:     make(chan struct{}),
		done:         make(chan struct{}),
	}
	h.signaling = NewSignalingRouter(h, redisStore)
	return h
}

// Run executes the central coordinator event loop.
func (h *Hub) Run(ctx context.Context) {
	slog.Info("hub coordinator loop started")
	defer func() {
		close(h.done)
		slog.Info("hub coordinator loop stopped")
	}()

	for {
		select {
		case <-ctx.Done():
			h.cleanupAll()
			return

		case <-h.shutdown:
			h.cleanupAll()
			return

		case client := <-h.register:
			h.handleRegister(client)

		case client := <-h.unregister:
			h.handleUnregister(client)

		case message := <-h.broadcast:
			h.handleBroadcast(message)
		}
	}
}

// Register registers a new client with the hub.
func (h *Hub) Register(c *Client) {
	h.register <- c
}

// Unregister unregisters a client from the hub.
func (h *Hub) Unregister(c *Client) {
	h.unregister <- c
}

// Broadcast enqueues a message for broadcast to all connected clients.
func (h *Hub) Broadcast(msg []byte) {
	h.broadcast <- msg
}

func (h *Hub) handleRegister(c *Client) {
	h.mu.Lock()
	h.clients[c] = true
	h.clientsByID[c.ID] = c
	h.mu.Unlock()

	h.activeConnections.Add(1)
	if h.redis != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			defer cancel()
			_, _ = h.redis.IncrActiveConnections(ctx)
		}()
	}

	slog.Debug("client registered", "client_id", c.ID, "active", h.activeConnections.Load())
}

func (h *Hub) handleUnregister(c *Client) {
	h.mu.Lock()
	if !h.clients[c] {
		h.mu.Unlock()
		return
	}

	delete(h.clients, c)
	delete(h.clientsByID, c.ID)

	// Clean up all group rooms the client was in
	for roomCode, members := range h.groupRooms {
		if members[c] {
			delete(members, c)
			if len(members) == 0 {
				delete(h.groupRooms, roomCode)
			}
			// Notify other group members
			go h.signaling.NotifyGroupPeerLeft(roomCode, c.ID)
		}
	}

	// Clean up P2P room if client was host or peer
	p2pCode := c.GetP2PRoom()
	var otherPeer *Client
	if p2pCode != "" {
		if pairing, exists := h.p2pPairings[p2pCode]; exists {
			if pairing.Host == c {
				otherPeer = pairing.Peer
			} else if pairing.Peer == c {
				otherPeer = pairing.Host
			}
			delete(h.p2pPairings, p2pCode)
		}
	}
	h.mu.Unlock()

	// Notify paired peer if any
	if otherPeer != nil {
		go func(p *Client, code, disconnectedID string) {
			notice, _ := BuildEnvelope(TypeP2PPeerLeft, code, disconnectedID, p.ID, map[string]string{
				"peer_id": disconnectedID,
				"reason":  "disconnected",
			})
			p.Send(notice)
			p.SetP2PRoom("")
		}(otherPeer, p2pCode, c.ID)
	}

	// Clean up Redis P2P key if existed
	if p2pCode != "" && h.redis != nil {
		go func(code string) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			defer cancel()
			_ = h.redis.DeleteP2PRoom(ctx, code)
		}(p2pCode)
	}

	// Clean up group memberships in Redis
	for _, roomCode := range c.GetGroupRooms() {
		if h.redis != nil {
			go func(code, clientID string) {
				ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
				defer cancel()
				_, _ = h.redis.RemoveGroupMember(ctx, code, clientID)
			}(roomCode, c.ID)
		}
	}

	// Close the send channel
	close(c.send)

	h.activeConnections.Add(-1)
	if h.redis != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			defer cancel()
			_, _ = h.redis.DecrActiveConnections(ctx)
		}()
	}

	slog.Debug("client unregistered", "client_id", c.ID, "active", h.activeConnections.Load())
}

func (h *Hub) handleBroadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		client.Send(msg)
	}
}

// BroadcastExcept sends a message to all clients except the specified client ID.
func (h *Hub) BroadcastExcept(msg []byte, excludeClientID string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		if client.ID != excludeClientID {
			client.Send(msg)
		}
	}
}

