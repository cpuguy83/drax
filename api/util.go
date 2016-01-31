package api

import (
	"encoding/json"
	"io"
)

func Encode(v interface{}, to io.Writer) error {
	return json.NewEncoder(to).Encode(v)
}

func Decode(v interface{}, from io.Reader) error {
	return json.NewDecoder(from).Decode(v)
}
