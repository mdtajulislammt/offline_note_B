package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Redis Key Namespaces
	KeyPrefixP2P     = "room:p2p:"
	KeyPrefixGroup   = "room:group:"
	KeyPublicStream  = "chat:public:stream"
	KeyMetricConn    = "metrics:active_connections"

	// Strict TTL Policies
	P2PRoomTTL       = 60 * time.Second
	GroupRoomTTL     = 24 * time.Hour
	PublicStreamCap  = 50
	DefaultRedisTimeout = 3 * time.Second
)

// Domain Errors
var (
	ErrRoomNotFound      = errors.New("room not found or expired")
	ErrRoomAlreadyPaired = errors.New("room already has two peers paired")
	ErrRoomCodeConflict  = errors.New("room code collision, please retry")
	ErrInvalidPeer       = errors.New("invalid peer identifier")
)

// P2PRoom represents ephemeral 1-on-1 WebRTC signaling state.
type P2PRoom struct {
	Code      string `json:"code"`
	HostID    string `json:"host_id"`
	PeerID    string `json:"peer_id,omitempty"`
	Status    string `json:"status"` // "waiting" | "paired"
	CreatedAt int64  `json:"created_at"`
}

// RedisStore wraps go-redis/v9 with strict context timeouts and domain logic.
