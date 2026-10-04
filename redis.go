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
// 1-on-1 Ephemeral P2P Room Operations
// -----------------------------------------------------------------------------

// CreateP2PRoom creates an ephemeral room key `room:p2p:{code}` with 60s TTL.
// Returns ErrRoomCodeConflict if the code already exists.
func (r *RedisStore) CreateP2PRoom(ctx context.Context, code, hostID string) (*P2PRoom, error) {
	ctx, cancel := r.ensureContext(ctx)
	defer cancel()

	room := &P2PRoom{
		Code:      code,
		HostID:    hostID,
		Status:    "waiting",
		CreatedAt: time.Now().Unix(),
	}

	data, err := json.Marshal(room)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal p2p room: %w", err)
	}

	key := KeyPrefixP2P + code
	// SetNX enforces uniqueness: fails if code already exists
	set, err := r.client.SetNX(ctx, key, data, P2PRoomTTL).Result()
	if err != nil {
		return nil, fmt.Errorf("redis setnx error: %w", err)
	}
	if !set {
		return nil, ErrRoomCodeConflict
	}

	return room, nil
}

// GetP2PRoom retrieves room data for the given code.
func (r *RedisStore) GetP2PRoom(ctx context.Context, code string) (*P2PRoom, error) {
	ctx, cancel := r.ensureContext(ctx)
	defer cancel()

	key := KeyPrefixP2P + code
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrRoomNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis get p2p room error: %w", err)
	}

	var room P2PRoom
	if err := json.Unmarshal([]byte(val), &room); err != nil {
		return nil, fmt.Errorf("failed to unmarshal p2p room: %w", err)
	}

	return &room, nil
}

// PairP2PRoom executes atomic pairing via Lua script.
func (r *RedisStore) PairP2PRoom(ctx context.Context, code, peerID string) (*P2PRoom, error) {
	ctx, cancel := r.ensureContext(ctx)
	defer cancel()

	key := KeyPrefixP2P + code
	res, err := pairP2PRoomLua.Run(ctx, r.client, []string{key}, peerID).Result()
	if err != nil {
		errMsg := err.Error()
		switch {
		case strings.Contains(errMsg, "ERR_ROOM_NOT_FOUND") || errors.Is(err, redis.Nil):
			return nil, ErrRoomNotFound
		case strings.Contains(errMsg, "ERR_ROOM_ALREADY_PAIRED"):
			return nil, ErrRoomAlreadyPaired
		case strings.Contains(errMsg, "ERR_HOST_CANNOT_BE_PEER"):
			return nil, errors.New("host cannot join their own room as peer")
		default:
			return nil, fmt.Errorf("redis pair lua error: %w", err)
		}
	}

	rawJSON, ok := res.(string)
	if !ok {
		return nil, errors.New("invalid lua return format")
	}

	var room P2PRoom
	if err := json.Unmarshal([]byte(rawJSON), &room); err != nil {
		return nil, fmt.Errorf("failed to parse paired room json: %w", err)
	}

	return &room, nil
}

// DeleteP2PRoom removes a P2P room key.
func (r *RedisStore) DeleteP2PRoom(ctx context.Context, code string) error {
	ctx, cancel := r.ensureContext(ctx)
	defer cancel()

	return r.client.Del(ctx, KeyPrefixP2P+code).Err()
}

// -----------------------------------------------------------------------------
// Group Room Operations
// -----------------------------------------------------------------------------

// AddGroupMember adds a client ID to key `room:group:{code}` and updates TTL.
func (r *RedisStore) AddGroupMember(ctx context.Context, code, clientID string) error {
	ctx, cancel := r.ensureContext(ctx)
	defer cancel()

	key := KeyPrefixGroup + code
	pipe := r.client.Pipeline()
	pipe.SAdd(ctx, key, clientID)
	pipe.Expire(ctx, key, GroupRoomTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to add group member: %w", err)
	}
	return nil
}

// RemoveGroupMember removes a client ID from key `room:group:{code}`.
// If room becomes empty, the key is deleted immediately.
func (r *RedisStore) RemoveGroupMember(ctx context.Context, code, clientID string) (int64, error) {
	ctx, cancel := r.ensureContext(ctx)
	defer cancel()

	key := KeyPrefixGroup + code
	removed, err := r.client.SRem(ctx, key, clientID).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to remove group member: %w", err)
	}
	if removed == 0 {
		return 0, nil
	}

	count, err := r.client.SCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to inspect group cardinality: %w", err)
	}

	if count == 0 {
		_ = r.client.Del(ctx, key).Err()
	}

	return count, nil
}

// GetGroupMembers returns all active member IDs for a group room.
func (r *RedisStore) GetGroupMembers(ctx context.Context, code string) ([]string, error) {
	ctx, cancel := r.ensureContext(ctx)
	defer cancel()

	members, err := r.client.SMembers(ctx, KeyPrefixGroup+code).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch group members: %w", err)
	}
	return members, nil
}

// -----------------------------------------------------------------------------
