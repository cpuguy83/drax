package api

import (
	"encoding/json"
	"io"
)

// Encode should be used to encode anything sent to or use by the cluster
func Encode(v interface{}, to io.Writer) error {
	return json.NewEncoder(to).Encode(v)
}

// Decode should be used to decode anything sent to or use by the cluster
func Decode(v interface{}, from io.Reader) error {
	return json.NewDecoder(from).Decode(v)
}
