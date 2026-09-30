package main

import (
	"encoding/json"
	"time"
)

// SignalEnvelope is the standard snake_case wire frame for all incoming and outgoing messages.
type SignalEnvelope struct {
	Type    string          `json:"type"`
	Room    string          `json:"room,omitempty"`
	From    string          `json:"from,omitempty"`
	To      string          `json:"to,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}
