package main

import (
	"strings"
	"sync"
	"testing"
)

func TestGenerateRoomCode(t *testing.T) {
	excludedChars := "0O1ILoil"

	for i := 0; i < 500; i++ {
		code, err := GenerateRoomCode()
		if err != nil {
			t.Fatalf("unexpected error generating code: %v", err)
		}

		if len(code) != 9 {
			t.Fatalf("expected code length 9 (XXXX-XXXX), got %d (%s)", len(code), code)
		}

		if code[4] != '-' {
			t.Fatalf("expected hyphen at index 4, got %c in %s", code[4], code)
		}

		if !ValidateRoomCode(code) {
			t.Fatalf("generated code failed regex validation: %s", code)
		}

		for _, ch := range excludedChars {
			if strings.ContainsRune(code, ch) {
				t.Fatalf("code %s contains ambiguous character %c", code, ch)
			}
		}
	}
}

func TestGenerateRoomCodeConcurrency(t *testing.T) {
	const goroutines = 50
	const perGoroutine = 50

	var wg sync.WaitGroup
	codes := make(chan string, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				code, err := GenerateRoomCode()
				if err != nil {
					t.Errorf("concurrent generation error: %v", err)
					return
				}
				codes <- code
			}
		}()
	}

	wg.Wait()
	close(codes)

	seen := make(map[string]bool)
	for code := range codes {
		if !ValidateRoomCode(code) {
			t.Fatalf("invalid code generated concurrently: %s", code)
		}
		seen[code] = true
	}

	// With 31^8 (~8.52 * 10^11) permutations, 2500 codes should virtually have 0 collisions
	if len(seen) != goroutines*perGoroutine {
		t.Logf("Warning: rare collision detected in %d generated codes: %d unique", goroutines*perGoroutine, len(seen))
	}
}

func TestNormalizeRoomCode(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"abcd-efgh", "ABCD-EFGH"},
		{"abcdefgh", "ABCD-EFGH"},
		{" ABCD-EFGH ", "ABCD-EFGH"},
		{"23456789", "2345-6789"},
	}

	for _, tt := range tests {
		result := NormalizeRoomCode(tt.input)
		if result != tt.expected {
			t.Errorf("NormalizeRoomCode(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}
