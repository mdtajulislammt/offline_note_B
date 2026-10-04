package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func setupTestRedis(t *testing.T) *RedisStore {
	store, err := NewRedisStore("localhost:6379")
	if err != nil {
		t.Skipf("skipping redis test: redis not available at localhost:6379 (%v)", err)
	}
	return store
}

func TestRedisP2PRoomLifecycle(t *testing.T) {
	store := setupTestRedis(t)
	defer store.Close()

	ctx := context.Background()
	code, err := GenerateRoomCode()
	if err != nil {
		t.Fatalf("failed to generate code: %v", err)
	}
	hostID := "host-" + uuid.NewString()
	peerID := "peer-" + uuid.NewString()

	// 1. Create Room with 60s TTL
	room, err := store.CreateP2PRoom(ctx, code, hostID)
	if err != nil {
		t.Fatalf("failed to create p2p room: %v", err)
	}
	if room.Code != code || room.HostID != hostID || room.Status != "waiting" {
		t.Fatalf("unexpected room state: %+v", room)
	}

	// 2. Test collision detection with SetNX
	_, err = store.CreateP2PRoom(ctx, code, "another-host")
	if err != ErrRoomCodeConflict {
		t.Fatalf("expected ErrRoomCodeConflict, got %v", err)
	}

	// 3. Inspect TTL
	ttl, err := store.client.TTL(ctx, KeyPrefixP2P+code).Result()
	if err != nil {
		t.Fatalf("failed to get TTL: %v", err)
	}
	if ttl <= 0 || ttl > P2PRoomTTL {
		t.Fatalf("expected TTL between 0 and 60s, got %v", ttl)
	}

	// 4. Host cannot join their own room as peer
	_, err = store.PairP2PRoom(ctx, code, hostID)
	if err == nil {
		t.Fatalf("expected error when host pairs with self, got nil")
	}

	// 5. Atomic Pair with Peer
	pairedRoom, err := store.PairP2PRoom(ctx, code, peerID)
	if err != nil {
		t.Fatalf("failed to pair p2p room: %v", err)
	}
	if pairedRoom.Status != "paired" || pairedRoom.PeerID != peerID {
		t.Fatalf("unexpected paired room state: %+v", pairedRoom)
	}

	// 6. Second peer cannot join already paired room
	_, err = store.PairP2PRoom(ctx, code, "intruder-id")
	if err != ErrRoomAlreadyPaired {
		t.Fatalf("expected ErrRoomAlreadyPaired, got %v", err)
	}

	// 7. Cleanup
	if err := store.DeleteP2PRoom(ctx, code); err != nil {
		t.Fatalf("failed to delete room: %v", err)
	}

	// 8. Verify room deleted
	_, err = store.GetP2PRoom(ctx, code)
	if err != ErrRoomNotFound {
		t.Fatalf("expected ErrRoomNotFound, got %v", err)
	}
}

func TestRedisP2PRoomConcurrentPairRace(t *testing.T) {
	store := setupTestRedis(t)
	defer store.Close()

	ctx := context.Background()
	code, _ := GenerateRoomCode()
	hostID := "host-" + uuid.NewString()

	_, err := store.CreateP2PRoom(ctx, code, hostID)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	defer store.DeleteP2PRoom(ctx, code)

	// Launch 10 simultaneous peers attempting to pair the same 1-on-1 code
	const peers = 10
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < peers; i++ {
		wg.Add(1)
		pID := fmt.Sprintf("peer-%d-%s", i, uuid.NewString())
		go func(peer string) {
			defer wg.Done()
			_, err := store.PairP2PRoom(ctx, code, peer)
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(pID)
	}

	wg.Wait()

	// Exactly 1 peer must succeed; all other 9 must fail!
	if successCount != 1 {
		t.Fatalf("expected exactly 1 peer to successfully pair, got %d", successCount)
	}
}

func TestRedisGroupRooms(t *testing.T) {
	store := setupTestRedis(t)
	defer store.Close()

	ctx := context.Background()
	groupCode := "GROUP-" + uuid.NewString()[:8]
	client1 := "client-1"
	client2 := "client-2"

	// Add member 1 & 2
	if err := store.AddGroupMember(ctx, groupCode, client1); err != nil {
		t.Fatalf("failed to add member 1: %v", err)
	}
	if err := store.AddGroupMember(ctx, groupCode, client2); err != nil {
		t.Fatalf("failed to add member 2: %v", err)
	}

	members, err := store.GetGroupMembers(ctx, groupCode)
	if err != nil {
		t.Fatalf("failed to get group members: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("expected 2 group members, got %d", len(members))
	}

	// Remove member 1
	rem, err := store.RemoveGroupMember(ctx, groupCode, client1)
	if err != nil {
		t.Fatalf("failed to remove member 1: %v", err)
	}
	if rem != 1 {
		t.Fatalf("expected 1 remaining member, got %d", rem)
	}

	// Remove member 2 (room should be emptied and deleted)
	rem, err = store.RemoveGroupMember(ctx, groupCode, client2)
	if err != nil {
		t.Fatalf("failed to remove member 2: %v", err)
	}
	if rem != 0 {
		t.Fatalf("expected 0 remaining members, got %d", rem)
	}
}

func TestRedisPublicChatStreamCapping(t *testing.T) {
	store := setupTestRedis(t)
	defer store.Close()

	ctx := context.Background()

	// Clean stream before test
	_ = store.client.Del(ctx, KeyPublicStream).Err()

	// Push 65 messages
	for i := 1; i <= 65; i++ {
		msg := fmt.Sprintf("message-%03d", i)
		if err := store.PushPublicMessage(ctx, []byte(msg)); err != nil {
			t.Fatalf("push error at message %d: %v", i, err)
		}
	}

	// Fetch messages: should be capped strictly at 50 messages
	messages, err := store.GetPublicMessages(ctx, 100)
	if err != nil {
		t.Fatalf("fetch error: %v", err)
	}

	if len(messages) != PublicStreamCap {
		t.Fatalf("expected capped count %d, got %d", PublicStreamCap, len(messages))
	}

	// Because we pushed 1..65, messages 16..65 should be kept (50 total).
	// Chronological order: oldest kept should be message-016, newest should be message-065.
	firstMsg := string(messages[0])
	lastMsg := string(messages[len(messages)-1])

	if firstMsg != "message-016" {
		t.Fatalf("expected oldest message 'message-016', got %s", firstMsg)
	}
	if lastMsg != "message-065" {
		t.Fatalf("expected newest message 'message-065', got %s", lastMsg)
	}
}
