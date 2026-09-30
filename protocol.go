package main

import (
	"encoding/json"
	"time"
)

// Standard Wire Protocol Message Types
const (
	// System / Heartbeat
	TypePing  = "ping"
	TypePong  = "pong"
	TypeError = "error"

	// Public Stream
	TypePublicJoin       = "public_join"
	TypePublicJoined     = "public_joined"
	TypePublicPeerJoined = "public_peer_joined"
	TypePublicPeerLeft   = "public_peer_left"
	TypePublicChat       = "public_chat"
	TypePublicChatMessage= "public_chat_message"
	TypePublicHistory    = "public_history"
	TypePublicMetrics    = "public_metrics"

	// 1-on-1 P2P WebRTC Signaling
	TypeP2PCreate     = "p2p_create"
	TypeP2PCreated    = "p2p_created"
	TypeP2PJoin       = "p2p_join"
	TypeP2PJoined     = "p2p_joined"
	TypeP2PPeerJoined = "p2p_peer_joined"
	TypeP2PPeerLeft   = "p2p_peer_left"
	TypeP2PSignal     = "p2p_signal"

	// Group Note Collaboration
	TypeGroupJoin       = "group_join"
	TypeGroupJoined     = "group_joined"
	TypeGroupPeerJoined = "group_peer_joined"
	TypeGroupLeave      = "group_leave"
	TypeGroupPeerLeft   = "group_peer_left"
	TypeGroupNoteSync   = "group_note_sync"
	TypeGroupBroadcast  = "group_broadcast"
)

// SignalEnvelope is the standard snake_case wire frame for all incoming and outgoing messages.
type SignalEnvelope struct {
	Type    string          `json:"type"`
	Room    string          `json:"room,omitempty"`
	From    string          `json:"from,omitempty"`
	To      string          `json:"to,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// ErrorPayload represents a standardized error sent to clients.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// PublicJoinPayload is sent when a client joins public lobby with an optional alias.
type PublicJoinPayload struct {
	Alias string `json:"alias,omitempty"`
}

// PublicChatMessagePayload is the wire representation of a public stream message.
type PublicChatMessagePayload struct {
	MessageID string `json:"message_id"`
	FromID    string `json:"from_id"`
	Alias     string `json:"alias"`
	Text      string `json:"text"`
	Timestamp int64  `json:"timestamp"`
}

// PublicMetricsPayload reports real-time system metrics.
type PublicMetricsPayload struct {
	ActiveConnections int64 `json:"active_connections"`
	ActiveGroupRooms  int64 `json:"active_group_rooms"`
	ActiveP2PRooms    int64 `json:"active_p2p_rooms"`
	UptimeSeconds     int64 `json:"uptime_seconds"`
}

// P2PCreatedPayload confirms P2P room creation with ephemeral TTL.
type P2PCreatedPayload struct {
	Code       string `json:"code"`
	HostID     string `json:"host_id"`
	TTLSeconds int    `json:"ttl_seconds"`
}

// P2PJoinedPayload confirms successful pairing to the joining peer.
type P2PJoinedPayload struct {
	PeerID string `json:"peer_id"`
	Role   string `json:"role"`
}

// P2PPeerJoinedPayload notifies host that peer has connected.
type P2PPeerJoinedPayload struct {
	PeerID string `json:"peer_id"`
	Role   string `json:"role"`
}

// GroupJoinedPayload provides initial membership to a joining group participant.
type GroupJoinedPayload struct {
	Room    string   `json:"room"`
	Members []string `json:"members"`
}

// GroupPeerNotification describes a member joining or leaving a group.
type GroupPeerNotification struct {
	PeerID string `json:"peer_id"`
	Alias  string `json:"alias,omitempty"`
	Room   string `json:"room"`
}

// BuildEnvelope creates a serialized JSON SignalEnvelope with arbitrary struct payload.
func BuildEnvelope(msgType, room, from, to string, payload any) ([]byte, error) {
	var rawPayload json.RawMessage
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		rawPayload = data
	}

	env := SignalEnvelope{
		Type:    msgType,
		Room:    room,
		From:    from,
		To:      to,
		Payload: rawPayload,
	}
	return json.Marshal(env)
}

// BuildRawEnvelope creates a serialized JSON SignalEnvelope with pre-encoded json.RawMessage payload.
func BuildRawEnvelope(msgType, room, from, to string, rawPayload json.RawMessage) ([]byte, error) {
	env := SignalEnvelope{
		Type:    msgType,
		Room:    room,
		From:    from,
		To:      to,
		Payload: rawPayload,
	}
	return json.Marshal(env)
}

// BuildErrorEnvelope creates a standard error envelope bytes.
func BuildErrorEnvelope(code, message, room string) []byte {
	env, _ := BuildEnvelope(TypeError, room, "system", "", ErrorPayload{
		Code:    code,
		Message: message,
	})
	return env
}

// NowMillis returns current epoch timestamp in milliseconds.
func NowMillis() int64 {
	return time.Now().UnixMilli()
}
