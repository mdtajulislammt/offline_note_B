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
