package api

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"time"
)

const retryTimeout = 30 // TODO: make configurable

func Encode(v interface{}, to io.Writer) error {
	return json.NewEncoder(to).Encode(v)
}

func Decode(v interface{}, from io.Reader) error {
	return json.NewDecoder(from).Decode(v)
}

func Dial(address string, msgType MessageType, timeout time.Duration, tlsConfig *tls.Config, retry bool) (net.Conn, error) {
	var (
		conn    net.Conn
		err     error
		retries int
	)
	start := time.Now()

	for {
		dialer := &net.Dialer{Timeout: timeout}
		if tlsConfig == nil {
			conn, err = dialer.Dial("tcp", address)
		} else {
			conn, err = tls.DialWithDialer(dialer, "tcp", address, tlsConfig)
		}

		if err != nil {
			if !retry {
				return nil, err
			}
			timeOff := backoff(retries)
			if abort(start, timeOff) {
				return nil, err
			}

			retries++
			time.Sleep(timeOff)
			continue
		}

		if _, err := conn.Write([]byte{byte(msgType)}); err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func backoff(retries int) time.Duration {
	b, max := 1, retryTimeout
	for b < max && retries > 0 {
		b *= 2
		retries--
	}
	if b > max {
		b = max
	}
	return time.Duration(b) * time.Second
}

func abort(start time.Time, timeOff time.Duration) bool {
	return timeOff+time.Since(start) >= time.Duration(retryTimeout)*time.Second
}
