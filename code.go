package main

import (
	"crypto/rand"
	"errors"
	"math/big"
)

// UnambiguousCharset contains 31 characters excluding visually ambiguous
// characters: '0', 'O', '1', 'I', 'L'.
const UnambiguousCharset = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

const (
	CodeHalfLength  = 4
	CodeTotalLength = 8
)
