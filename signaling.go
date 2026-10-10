package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
)

// SignalingRouter processes incoming WebSocket messages and routes them
// according to the SuperNote wire protocol.
type SignalingRouter struct {
	hub   *Hub
	redis *RedisStore
}

// NewSignalingRouter creates a new signaling handler.
func NewSignalingRouter(hub *Hub, redisStore *RedisStore) *SignalingRouter {
	return &SignalingRouter{
		hub:   hub,
		redis: redisStore,
	}
}

// HandleMessage decodes the outer envelope and routes to specialized handlers.
func (s *SignalingRouter) HandleMessage(c *Client, rawMessage []byte) {
	var env SignalEnvelope
	if err := json.Unmarshal(rawMessage, &env); err != nil {
		slog.Warn("malformed message frame", "client_id", c.ID, "err", err)
		c.Send(BuildErrorEnvelope("bad_request", "invalid json envelope", ""))
		return
	}

	switch env.Type {
	case TypePing:
		s.handlePing(c)

	// Public Stream Handlers
	case TypePublicJoin:
		s.handlePublicJoin(c, env)
	case TypePublicChat:
		s.handlePublicChat(c, env)
	case TypePublicMetrics:
		s.handlePublicMetrics(c)

	// 1-on-1 P2P WebRTC Signaling Handlers
	case TypeP2PCreate:
		s.handleP2PCreate(c)
	case TypeP2PJoin:
		s.handleP2PJoin(c, env)
	case TypeP2PSignal:
		s.handleP2PSignal(c, env)

	// Group Collaboration Handlers
	case TypeGroupJoin:
		s.handleGroupJoin(c, env)
	case TypeGroupLeave:
		s.handleGroupLeave(c, env)
	case TypeGroupNoteSync:
		s.handleGroupNoteSync(c, env)
	case TypeGroupBroadcast:
		s.handleGroupBroadcast(c, env)

	default:
		slog.Warn("unrecognized message type", "type", env.Type, "client_id", c.ID)
		c.Send(BuildErrorEnvelope("unknown_message_type", fmt.Sprintf("unsupported type %q", env.Type), env.Room))
	}
}

// -----------------------------------------------------------------------------
// Ping / Pong Heartbeat
// -----------------------------------------------------------------------------

func (s *SignalingRouter) handlePing(c *Client) {
	pong, _ := BuildEnvelope(TypePong, "", "system", c.ID, map[string]int64{
		"timestamp": NowMillis(),
	})
	c.Send(pong)
}

// -----------------------------------------------------------------------------
// Public Stream Handlers
// -----------------------------------------------------------------------------

func (s *SignalingRouter) handlePublicJoin(c *Client, env SignalEnvelope) {
	alias := "anonymous"
	if len(env.Payload) > 0 {
		var req PublicJoinPayload
		if err := json.Unmarshal(env.Payload, &req); err == nil && strings.TrimSpace(req.Alias) != "" {
			alias = strings.TrimSpace(req.Alias)
		}
	}
	c.SetAlias(alias)

	// Confirm join to client
	joinedMsg, _ := BuildEnvelope(TypePublicJoined, "", "system", c.ID, map[string]any{
		"client_id": c.ID,
		"alias":     alias,
	})
	c.Send(joinedMsg)

	// Send recent public chat history (capped at 50 from Redis)
	if s.redis != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			defer cancel()

			messages, err := s.redis.GetPublicMessages(ctx, PublicStreamCap)
			if err == nil && len(messages) > 0 {
				historyPayload := make([]json.RawMessage, len(messages))
				for i, m := range messages {
					historyPayload[i] = json.RawMessage(m)
				}
				historyMsg, _ := BuildEnvelope(TypePublicHistory, "", "system", c.ID, historyPayload)
				c.Send(historyMsg)
			}
		}()
	}

	// Broadcast presence to all other public participants
	presenceMsg, _ := BuildEnvelope(TypePublicPeerJoined, "", c.ID, "", map[string]string{
		"peer_id": c.ID,
		"alias":   alias,
	})
	s.hub.BroadcastExcept(presenceMsg, c.ID)
}

func (s *SignalingRouter) handlePublicChat(c *Client, env SignalEnvelope) {
	var chatReq struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(env.Payload, &chatReq); err != nil || strings.TrimSpace(chatReq.Text) == "" {
		c.Send(BuildErrorEnvelope("invalid_chat_payload", "chat message text cannot be empty", ""))
		return
	}

	chatPayload := PublicChatMessagePayload{
		MessageID: uuid.NewString(),
		FromID:    c.ID,
		Alias:     c.GetAlias(),
		Text:      strings.TrimSpace(chatReq.Text),
		Timestamp: NowMillis(),
	}

	payloadBytes, err := json.Marshal(chatPayload)
	if err != nil {
		return
	}

	// Persist to capped Redis list (LPush + LTrim 50)
	if s.redis != nil {
		go func(data []byte) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			defer cancel()
			_ = s.redis.PushPublicMessage(ctx, data)
		}(payloadBytes)
	}

	// Broadcast chat message to all connected clients
	broadcastEnv, _ := BuildEnvelope(TypePublicChatMessage, "", c.ID, "", chatPayload)
	s.hub.Broadcast(broadcastEnv)
}

func (s *SignalingRouter) handlePublicMetrics(c *Client) {
	metrics := s.hub.GetMetrics()
	msg, _ := BuildEnvelope(TypePublicMetrics, "", "system", c.ID, metrics)
	c.Send(msg)
}

// -----------------------------------------------------------------------------
// 1-on-1 Ephemeral P2P WebRTC Signaling Handlers
// -----------------------------------------------------------------------------

// handleP2PCreate generates an 8-character PIN, stores host identifier in Redis with 60s TTL.
func (s *SignalingRouter) handleP2PCreate(c *Client) {
	var code string
	var err error

	// Retry loop for extremely rare room code collision
	for attempt := 0; attempt < 3; attempt++ {
		code, err = GenerateRoomCode()
		if err != nil {
			c.Send(BuildErrorEnvelope("internal_error", "failed to generate secure room code", ""))
			return
		}

		if s.redis != nil {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			_, err = s.redis.CreateP2PRoom(ctx, code, c.ID)
			cancel()

			if err == nil {
				break
			}
			if errors.Is(err, ErrRoomCodeConflict) {
				continue
			}
			slog.Error("redis p2p create error", "err", err)
			c.Send(BuildErrorEnvelope("redis_error", "failed to register p2p room", ""))
			return
		} else {
			// In-memory fallback if Redis is disabled
			break
		}
	}

	s.hub.SetP2PPairing(code, c, nil, "waiting")

	resp, _ := BuildEnvelope(TypeP2PCreated, code, "system", c.ID, P2PCreatedPayload{
		Code:       code,
		HostID:     c.ID,
		TTLSeconds: int(P2PRoomTTL.Seconds()),
	})
	c.Send(resp)
	slog.Info("p2p room created", "code", code, "host_id", c.ID)
}

// handleP2PJoin validates PIN, pairs peer, informs host.
func (s *SignalingRouter) handleP2PJoin(c *Client, env SignalEnvelope) {
	code := strings.TrimSpace(env.Room)
	if code == "" && len(env.Payload) > 0 {
		var req struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(env.Payload, &req)
		code = strings.TrimSpace(req.Code)
	}

	code = NormalizeRoomCode(code)
	if !ValidateRoomCode(code) {
		c.Send(BuildErrorEnvelope("invalid_pin_format", "PIN must follow XXXX-XXXX format with valid characters", code))
		return
	}

	var hostID string
	if s.redis != nil {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
		roomData, err := s.redis.PairP2PRoom(ctx, code, c.ID)
		cancel()

		if err != nil {
			switch {
			case errors.Is(err, ErrRoomNotFound):
				c.Send(BuildErrorEnvelope("room_not_found", "room does not exist or expired", code))
			case errors.Is(err, ErrRoomAlreadyPaired):
				c.Send(BuildErrorEnvelope("room_full", "room is already paired with another peer", code))
			default:
				c.Send(BuildErrorEnvelope("join_failed", err.Error(), code))
			}
			return
		}
		hostID = roomData.HostID
	} else {
		// In-memory fallback
		pairing, exists := s.hub.GetP2PPairing(code)
		if !exists || pairing.Status != "waiting" {
			c.Send(BuildErrorEnvelope("room_not_found", "room does not exist or is already paired", code))
			return
		}
		if pairing.Host.ID == c.ID {
			c.Send(BuildErrorEnvelope("invalid_peer", "host cannot join self", code))
			return
		}
		hostID = pairing.Host.ID
	}

	hostClient, exists := s.hub.GetClient(hostID)
	if !exists {
		c.Send(BuildErrorEnvelope("host_offline", "host disconnected before pairing completed", code))
		return
	}

	// Update pairing in Hub
	s.hub.SetP2PPairing(code, hostClient, c, "paired")

	// 1. Inform joining peer
	peerNotice, _ := BuildEnvelope(TypeP2PJoined, code, "system", c.ID, P2PJoinedPayload{
		PeerID: hostClient.ID,
		Role:   "peer",
	})
	c.Send(peerNotice)

	// 2. Inform waiting host
	hostNotice, _ := BuildEnvelope(TypeP2PPeerJoined, code, "system", hostClient.ID, P2PPeerJoinedPayload{
		PeerID: c.ID,
		Role:   "host",
	})
	hostClient.Send(hostNotice)

	slog.Info("p2p room successfully paired", "code", code, "host_id", hostID, "peer_id", c.ID)
}

// handleP2PSignal is a pure relay of WebRTC SDP (offer/answer) and ICE candidates
// between matched peers without decoding the payload.
func (s *SignalingRouter) handleP2PSignal(c *Client, env SignalEnvelope) {
	code := NormalizeRoomCode(env.Room)
	if code == "" {
		code = c.GetP2PRoom()
	}

	pairing, exists := s.hub.GetP2PPairing(code)
	if !exists || pairing.Status != "paired" {
		c.Send(BuildErrorEnvelope("p2p_not_paired", "room is not actively paired", code))
		return
	}

	// Identify the intended recipient peer
	var targetClient *Client
	if pairing.Host.ID == c.ID {
		targetClient = pairing.Peer
	} else if pairing.Peer.ID == c.ID {
		targetClient = pairing.Host
	} else {
		c.Send(BuildErrorEnvelope("forbidden", "client is not a member of this p2p session", code))
		return
	}

	if targetClient == nil {
		c.Send(BuildErrorEnvelope("peer_offline", "paired peer is currently offline", code))
		return
	}

	// Pure relay: wrap without modifying or decoding env.Payload
	relayBytes, err := BuildRawEnvelope(TypeP2PSignal, code, c.ID, targetClient.ID, env.Payload)
	if err != nil {
		slog.Error("failed to construct signal relay envelope", "err", err)
		return
	}

	if !targetClient.Send(relayBytes) {
		c.Send(BuildErrorEnvelope("peer_buffer_full", "target peer is slow or dropped frame", code))
	}
}

// -----------------------------------------------------------------------------
// Group Collaboration Handlers
// -----------------------------------------------------------------------------

func (s *SignalingRouter) handleGroupJoin(c *Client, env SignalEnvelope) {
	room := strings.TrimSpace(env.Room)
	if room == "" {
		c.Send(BuildErrorEnvelope("invalid_room", "group room identifier required", ""))
		return
	}

	// Update Redis set
	if s.redis != nil {
		go func(r, clientID string) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			defer cancel()
			_ = s.redis.AddGroupMember(ctx, r, clientID)
		}(room, c.ID)
	}

	// Add to hub tracking
	memberIDs := s.hub.AddClientToGroup(room, c)

	// Send membership list to joined client
	welcomeMsg, _ := BuildEnvelope(TypeGroupJoined, room, "system", c.ID, GroupJoinedPayload{
		Room:    room,
		Members: memberIDs,
	})
	c.Send(welcomeMsg)

	// Broadcast member join to others in the room
	peerNotice, _ := BuildEnvelope(TypeGroupPeerJoined, room, c.ID, "", GroupPeerNotification{
		PeerID: c.ID,
		Alias:  c.GetAlias(),
		Room:   room,
	})
	s.hub.BroadcastToGroup(room, peerNotice, c.ID)
}

func (s *SignalingRouter) handleGroupLeave(c *Client, env SignalEnvelope) {
	room := strings.TrimSpace(env.Room)
	if room == "" {
		return
	}

	s.hub.RemoveClientFromGroup(room, c)

	if s.redis != nil {
		go func(r, clientID string) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisTimeout)
			defer cancel()
			_, _ = s.redis.RemoveGroupMember(ctx, r, clientID)
		}(room, c.ID)
	}

	// Broadcast leave notification to remaining room members
	s.NotifyGroupPeerLeft(room, c.ID)
}

func (s *SignalingRouter) handleGroupNoteSync(c *Client, env SignalEnvelope) {
	room := strings.TrimSpace(env.Room)
	if room == "" {
		c.Send(BuildErrorEnvelope("invalid_room", "room code required for note sync", ""))
		return
	}

	// Pure relay of CRDT / OT note synchronization payload to other room participants
	relayBytes, err := BuildRawEnvelope(TypeGroupNoteSync, room, c.ID, "", env.Payload)
	if err != nil {
		return
	}

	s.hub.BroadcastToGroup(room, relayBytes, c.ID)
}

func (s *SignalingRouter) handleGroupBroadcast(c *Client, env SignalEnvelope) {
	room := strings.TrimSpace(env.Room)
	if room == "" {
		c.Send(BuildErrorEnvelope("invalid_room", "room code required for broadcast", ""))
		return
	}

	relayBytes, err := BuildRawEnvelope(TypeGroupBroadcast, room, c.ID, "", env.Payload)
	if err != nil {
		return
	}

	s.hub.BroadcastToGroup(room, relayBytes, c.ID)
}

// NotifyGroupPeerLeft broadcasts peer leave notification to all active room members.
func (s *SignalingRouter) NotifyGroupPeerLeft(room, peerID string) {
	leaveNotice, _ := BuildEnvelope(TypeGroupPeerLeft, room, peerID, "", GroupPeerNotification{
		PeerID: peerID,
		Room:   room,
	})
	s.hub.BroadcastToGroup(room, leaveNotice, peerID)
}
