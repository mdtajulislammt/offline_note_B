package main

import (
	"crypto/rand"
	"errors"
	"math/big"
)

const UnambiguousCharset = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

const (
	CodeHalfLength  = 4
	CodeTotalLength = 8
)

var (
	charsetLen = big.NewInt(int64(len(UnambiguousCharset)))
)

var ErrCryptoEntropy = errors.New("failed to read cryptographic randomness")

func GenerateRoomCode() (string, error) {
	buf := make([]byte, CodeTotalLength+1)
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
