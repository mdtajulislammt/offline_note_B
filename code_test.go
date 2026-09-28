package main

import (
	"strings"
	"testing"
)

func TestGenerateRoomCode(t *testing.T) {
	excludedChars := "0O1ILoil"
	for i := 0; i < 100; i++ {
		code, err := GenerateRoomCode()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(code) != 9 {
			t.Fatalf("expected length 9, got %d", len(code))
		}
		for _, ch := range excludedChars {
			if strings.ContainsRune(code, ch) {
				t.Fatalf("contains ambiguous char: %c", ch)
			}
		}
	}
}
