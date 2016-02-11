package api

import (
	"bytes"
	"encoding/json"
	"testing"
)

func BenchmarkEncode(b *testing.B) {
	v := Request{
		Action: Put,
		Key:    "hello",
		Value:  []byte{52, 29, 19, 69, 10},
	}
	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(&v, buf)
		buf.Reset()
	}
}

func BenchmarkDecode(b *testing.B) {
	b.StopTimer()

	data, err := json.Marshal(&Request{
		Action: Put,
		Key:    "hello",
		Value:  []byte{52, 29, 19, 69, 10},
	})

	if err != nil {
		b.Fatal(err)
	}
	buf := bytes.NewBuffer(data)

	var req Request

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Decode(&req, buf)
		buf.Reset()
	}
}
