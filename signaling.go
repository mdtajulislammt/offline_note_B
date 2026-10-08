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
