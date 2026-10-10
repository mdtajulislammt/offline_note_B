package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func setupTestServer(t *testing.T) (*httptest.Server, *Hub, *RedisStore) {
	store, err := NewRedisStore("localhost:6379")
	if err != nil {
		t.Logf("Redis not available (%v); proceeding with nil redis", err)
		store = nil
	} else {
		_ = store.client.Del(context.Background(), KeyPublicStream).Err()
	}

	hub := NewHub(store)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		hub.Close()
		if store != nil {
			store.Close()
		}
	})

	go hub.Run(ctx)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	}))
	t.Cleanup(func() {
		s.Close()
	})

	return s, hub, store
}

func dialWS(t *testing.T, s *httptest.Server) *websocket.Conn {
	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	return conn
}

func readEnvelope(t *testing.T, conn *websocket.Conn) SignalEnvelope {
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read websocket envelope: %v", err)
	}

	var env SignalEnvelope
	if err := json.Unmarshal(msg, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v (%s)", err, string(msg))
	}
	return env
}

func readUntilEnvelope(t *testing.T, conn *websocket.Conn, targetType string) SignalEnvelope {
	deadline := time.Now().Add(3 * time.Second)
	for {
		_ = conn.SetReadDeadline(deadline)
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("timed out waiting for message type %q: %v", targetType, err)
		}
		if len(msg) == 0 {
			t.Logf("received empty websocket message frame (type=%d)", msgType)
			continue
		}

		var env SignalEnvelope
		if err := json.Unmarshal(msg, &env); err != nil {
			t.Fatalf("failed to unmarshal envelope: %v (%q, type=%d)", err, string(msg), msgType)
		}

		if env.Type == targetType {
			return env
		}
	}
}

func TestSignalingPublicChatAndMetrics(t *testing.T) {
	server, _, _ := setupTestServer(t)

	clientA := dialWS(t, server)
	defer clientA.Close()

	clientB := dialWS(t, server)
	defer clientB.Close()

	// 1. Client A joins public stream with alias "Alice"
	joinPayload, _ := json.Marshal(PublicJoinPayload{Alias: "Alice"})
	joinEnv := SignalEnvelope{
		Type:    TypePublicJoin,
		Payload: joinPayload,
	}
	if err := clientA.WriteJSON(joinEnv); err != nil {
		t.Fatalf("failed to write join: %v", err)
	}

	// Client A should receive public_joined
	envA := readUntilEnvelope(t, clientA, TypePublicJoined)
	if envA.Type != TypePublicJoined {
		t.Fatalf("expected public_joined, got %s", envA.Type)
	}

	// Client B should receive public_peer_joined
	envB := readUntilEnvelope(t, clientB, TypePublicPeerJoined)
	if envB.Type != TypePublicPeerJoined {
		t.Fatalf("expected public_peer_joined, got %s", envB.Type)
	}

	// 2. Client A sends public chat message
	chatPayload, _ := json.Marshal(map[string]string{"text": "Hello SuperNote!"})
	chatEnv := SignalEnvelope{
		Type:    TypePublicChat,
		Payload: chatPayload,
	}
	if err := clientA.WriteJSON(chatEnv); err != nil {
		t.Fatalf("failed to write chat: %v", err)
	}

	// Both A and B should receive the broadcasted chat message
	msgA := readUntilEnvelope(t, clientA, TypePublicChatMessage)
	if msgA.Type != TypePublicChatMessage {
		t.Fatalf("expected public_chat_message for A, got %s", msgA.Type)
	}

	msgB := readUntilEnvelope(t, clientB, TypePublicChatMessage)
	if msgB.Type != TypePublicChatMessage {
		t.Fatalf("expected public_chat_message for B, got %s", msgB.Type)
	}

	var parsedChat PublicChatMessagePayload
	_ = json.Unmarshal(msgB.Payload, &parsedChat)
	if parsedChat.Alias != "Alice" || parsedChat.Text != "Hello SuperNote!" {
		t.Fatalf("unexpected chat payload: %+v", parsedChat)
	}
}

func TestSignalingP2PWorkflowAndPureRelay(t *testing.T) {
	server, _, _ := setupTestServer(t)

	host := dialWS(t, server)
	defer host.Close()

	peer := dialWS(t, server)
	defer peer.Close()

	// 1. Host requests p2p_create
	createEnv := SignalEnvelope{Type: TypeP2PCreate}
	if err := host.WriteJSON(createEnv); err != nil {
		t.Fatalf("host write create failed: %v", err)
	}

	createdEnv := readEnvelope(t, host)
	if createdEnv.Type != TypeP2PCreated {
		t.Fatalf("expected p2p_created, got %s", createdEnv.Type)
	}

	var createdData P2PCreatedPayload
	if err := json.Unmarshal(createdEnv.Payload, &createdData); err != nil {
		t.Fatalf("unmarshal created payload failed: %v", err)
	}

	roomCode := createdData.Code
	if !ValidateRoomCode(roomCode) {
		t.Fatalf("invalid room code: %s", roomCode)
	}

	// 2. Peer requests p2p_join with roomCode
	joinEnv := SignalEnvelope{
		Type: TypeP2PJoin,
		Room: roomCode,
	}
	if err := peer.WriteJSON(joinEnv); err != nil {
		t.Fatalf("peer write join failed: %v", err)
	}

	// Peer receives p2p_joined
	peerJoined := readEnvelope(t, peer)
	if peerJoined.Type != TypeP2PJoined {
		t.Fatalf("expected p2p_joined for peer, got %s", peerJoined.Type)
	}

	// Host receives p2p_peer_joined
	hostPeerJoined := readEnvelope(t, host)
	if hostPeerJoined.Type != TypeP2PPeerJoined {
		t.Fatalf("expected p2p_peer_joined for host, got %s", hostPeerJoined.Type)
	}

	// 3. WebRTC SDP Pure Relay test: Host sends Offer to Peer without decoding payload
	rawOffer := json.RawMessage(`{"sdp":{"type":"offer","sdp":"v=0\r\no=- 12345 2 IN IP4 127.0.0.1\r\ns=-\r\n"},"custom_metadata":{"nested":true}}`)
	signalEnv := SignalEnvelope{
		Type:    TypeP2PSignal,
		Room:    roomCode,
		Payload: rawOffer,
	}
	if err := host.WriteJSON(signalEnv); err != nil {
		t.Fatalf("failed to send p2p_signal: %v", err)
	}

	// Peer should receive the signal with pure payload intact
	peerSignal := readEnvelope(t, peer)
	if peerSignal.Type != TypeP2PSignal {
		t.Fatalf("expected p2p_signal, got %s", peerSignal.Type)
	}
	if string(peerSignal.Payload) != string(rawOffer) {
		t.Fatalf("payload was modified! Expected %s, got %s", string(rawOffer), string(peerSignal.Payload))
	}

	// 4. Peer sends ICE candidate back to Host
	rawCandidate := json.RawMessage(`{"candidate":"candidate:1 1 UDP 2122252543 192.168.1.100 50000 typ host","sdpMid":"0","sdpMLineIndex":0}`)
	answerSignal := SignalEnvelope{
		Type:    TypeP2PSignal,
		Room:    roomCode,
		Payload: rawCandidate,
	}
	if err := peer.WriteJSON(answerSignal); err != nil {
		t.Fatalf("failed to send answer signal: %v", err)
	}

	hostSignal := readEnvelope(t, host)
	if hostSignal.Type != TypeP2PSignal {
		t.Fatalf("expected p2p_signal for host, got %s", hostSignal.Type)
	}
	if string(hostSignal.Payload) != string(rawCandidate) {
		t.Fatalf("candidate payload was modified! Expected %s, got %s", string(rawCandidate), string(hostSignal.Payload))
	}
}

func TestSignalingGroupCollaborationAndNoteSync(t *testing.T) {
	server, _, _ := setupTestServer(t)

	client1 := dialWS(t, server)
	defer client1.Close()

	client2 := dialWS(t, server)
	defer client2.Close()

	groupCode := "TEAM-NOTE-42"

	// Client 1 joins group
	c1Join := SignalEnvelope{
		Type: TypeGroupJoin,
		Room: groupCode,
	}
	_ = client1.WriteJSON(c1Join)

	c1Joined := readEnvelope(t, client1)
	if c1Joined.Type != TypeGroupJoined {
		t.Fatalf("expected group_joined, got %s", c1Joined.Type)
	}

	// Client 2 joins group
	c2Join := SignalEnvelope{
		Type: TypeGroupJoin,
		Room: groupCode,
	}
	_ = client2.WriteJSON(c2Join)

	c2Joined := readEnvelope(t, client2)
	if c2Joined.Type != TypeGroupJoined {
		t.Fatalf("expected group_joined for c2, got %s", c2Joined.Type)
	}

	// Client 1 gets notification that Client 2 joined
	c1Notif := readEnvelope(t, client1)
	if c1Notif.Type != TypeGroupPeerJoined {
		t.Fatalf("expected group_peer_joined for c1, got %s", c1Notif.Type)
	}

	// Client 1 sends CRDT / Note Sync delta
	rawNoteDelta := json.RawMessage(`{"ops":[{"insert":"# SuperNote Title\n"},{"insert":"Meeting notes..."}],"clock":{"c1":1}}`)
	syncEnv := SignalEnvelope{
		Type:    TypeGroupNoteSync,
		Room:    groupCode,
		Payload: rawNoteDelta,
	}
	_ = client1.WriteJSON(syncEnv)

	// Client 2 receives the note sync without decoding distortion
	c2Sync := readEnvelope(t, client2)
	if c2Sync.Type != TypeGroupNoteSync {
		t.Fatalf("expected group_note_sync for c2, got %s", c2Sync.Type)
	}
	if string(c2Sync.Payload) != string(rawNoteDelta) {
		t.Fatalf("note delta altered: expected %s, got %s", string(rawNoteDelta), string(c2Sync.Payload))
	}
}
