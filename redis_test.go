package main

import (
	"testing"
)

func setupTestRedis(t *testing.T) *RedisStore {
	store, err := NewRedisStore("localhost:6379")
	if err != nil {
		t.Skipf("skipping redis test: %v", err)
	}
	return store
}
