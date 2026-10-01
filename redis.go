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
type RedisStore struct {
	client *redis.Client
}

// Lua script to atomically pair a P2P room, avoiding race conditions between simultaneous joiners.
var pairP2PRoomLua = redis.NewScript(`
local key = KEYS[1]
local peer_id = ARGV[1]

local val = redis.call('GET', key)
if not val then
    return redis.error_reply('ERR_ROOM_NOT_FOUND')
end

local room = cjson.decode(val)
if room['status'] ~= 'waiting' then
    return redis.error_reply('ERR_ROOM_ALREADY_PAIRED')
end

if room['host_id'] == peer_id then
    return redis.error_reply('ERR_HOST_CANNOT_BE_PEER')
end

room['peer_id'] = peer_id
room['status'] = 'paired'

local new_val = cjson.encode(room)
local ttl = redis.call('TTL', key)
if ttl > 0 then
    redis.call('SET', key, new_val, 'EX', ttl)
else
    redis.call('SET', key, new_val, 'EX', 60)
end

return new_val
`)

// NewRedisStore initializes and pings a new Redis client wrapper.
func NewRedisStore(redisURL string) (*RedisStore, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		// Fallback to simple address if not a full URL
		opt = &redis.Options{
			Addr:         redisURL,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolSize:     50,
			MinIdleConns: 10,
		}
	}

	client := redis.NewClient(opt)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisStore{client: client}, nil
}

// ensureContext ensures the context has a non-nil timeout if none was specified.
func (r *RedisStore) ensureContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, DefaultRedisTimeout)
}

// -----------------------------------------------------------------------------
