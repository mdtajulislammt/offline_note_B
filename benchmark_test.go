package main

import "testing"

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
