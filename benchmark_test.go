package main

import (
	"encoding/json"
	"testing"
)

func BenchmarkGenerateRoomCode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := GenerateRoomCode()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateRoomCode(b *testing.B) {
	code := "2345-6789"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !ValidateRoomCode(code) {
			b.Fatal("invalid code")
		}
	}
}

func BenchmarkBuildRawEnvelope(b *testing.B) {
	rawPayload := json.RawMessage(`{"type":"offer","sdp":"v=0\r\no=- 20518 0 IN IP4 203.0.113.1\r\ns=Real-time Note Stream"}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := BuildRawEnvelope(TypeP2PSignal, "ABCD-EFGH", "client-1", "client-2", rawPayload)
		if err != nil {
			b.Fatal(err)
		}
	}
}
