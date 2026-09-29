package main

import (
	"crypto/rand"
	"errors"
	"math/big"
	"regexp"
	"strings"
)

// UnambiguousCharset contains 31 characters excluding visually ambiguous
// characters: '0', 'O', '1', 'I', 'L'.
//
// Digits: 2, 3, 4, 5, 6, 7, 8, 9 (8 characters)
// Letters: A, B, C, D, E, F, G, H, J, K, M, N, P, Q, R, S, T, U, V, W, X, Y, Z (23 characters)
const UnambiguousCharset = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

const (
	CodeHalfLength  = 4
	CodeTotalLength = 8
)

var (
	charsetLen = big.NewInt(int64(len(UnambiguousCharset)))
	codeRegex  = regexp.MustCompile(`^[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{4}-[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{4}$`)
)

// ErrCryptoEntropy indicates a failure in reading cryptographic randomness from the OS.
var ErrCryptoEntropy = errors.New("failed to read cryptographic randomness")

// GenerateRoomCode generates a cryptographically secure 8-character room code
// in the formatted pattern "XXXX-XXXX", avoiding ambiguous characters ('0', 'O', '1', 'I', 'L').
//
// Concurrency-safe and free from modulo bias via crypto/rand.Int.
func GenerateRoomCode() (string, error) {
	buf := make([]byte, CodeTotalLength+1) // 8 chars + 1 hyphen

	for i := 0; i < CodeHalfLength; i++ {
		idx, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			return "", errors.Join(ErrCryptoEntropy, err)
		}
		buf[i] = UnambiguousCharset[idx.Int64()]
	}

	buf[CodeHalfLength] = '-'

	for i := 0; i < CodeHalfLength; i++ {
		idx, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			return "", errors.Join(ErrCryptoEntropy, err)
		}
		buf[CodeHalfLength+1+i] = UnambiguousCharset[idx.Int64()]
	}

	return string(buf), nil
}

// ValidateRoomCode checks whether a given string is a strictly valid "XXXX-XXXX" code.
func ValidateRoomCode(code string) bool {
	return codeRegex.MatchString(code)
}

// NormalizeRoomCode converts room codes to uppercase and ensures proper hyphenation
// if provided without a hyphen (e.g. "ABCDEFGH" -> "ABCD-EFGH").
func NormalizeRoomCode(code string) string {
	cleaned := strings.ToUpper(strings.TrimSpace(code))
	if len(cleaned) == CodeTotalLength && !strings.Contains(cleaned, "-") {
		return cleaned[:CodeHalfLength] + "-" + cleaned[CodeHalfLength:]
	}
	return cleaned
}
