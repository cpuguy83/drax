package api

import (
	"encoding/json"
	"io"
)

// Decoder performs stream decoding of the given value
type Decoder interface {
	Decode(interface{}) error
}

// Encoder performs stream encoding of the given value
type Encoder interface {
	Encode(interface{}) error
}

// Encode should be used to encode anything sent to or use by the cluster
func Encode(v interface{}, to io.Writer) error {
	return json.NewEncoder(to).Encode(v)
}

// Decode should be used to decode anything sent to or use by the cluster
func Decode(v interface{}, from io.Reader) error {
	return json.NewDecoder(from).Decode(v)
}

// NewDecoder returns a Decoder for performing stream decoding from the given reader
func NewDecoder(from io.Reader) Decoder {
	return json.NewDecoder(from)
}

// NewEncoder returns an Encoder for performing stream encoding from the given reader
func NewEncoder(to io.Writer) Encoder {
	return json.NewEncoder(to)
}
