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

	