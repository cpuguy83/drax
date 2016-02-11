package errors

import "github.com/docker/distribution/registry/api/errcode"

var (
	StoreKeyNotFound = errcode.Register("store", errcode.ErrorDescriptor{
		Value:       "KEYNOTFOUND",
		Message:     "Key not found in store",
		Description: "Key does not exist in the K/V store",
	})

	StoreKeyModified = errcode.Register("store", errcode.ErrorDescriptor{
		Value:       "KEYMODIFIED",
		Message:     "Unable to complete atomic operation, key modified",
		Description: "Key was modified while operation was being performed",
	})
)
