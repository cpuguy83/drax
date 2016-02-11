package errors

import "github.com/docker/distribution/registry/api/errcode"

var (
	// StoreKeyNotFound is an error returned by the API when the requested key is not found in the store
	StoreKeyNotFound = errcode.Register("store", errcode.ErrorDescriptor{
		Value:       "KEYNOTFOUND",
		Message:     "Key not found in store",
		Description: "Key does not exist in the K/V store",
	})

	// StoreKeyModified is an error returned by the API when the key was modified during an atomic operation
	StoreKeyModified = errcode.Register("store", errcode.ErrorDescriptor{
		Value:       "KEYMODIFIED",
		Message:     "Unable to complete atomic operation, key modified",
		Description: "Key was modified while operation was being performed",
	})
)
