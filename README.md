# Drax
An embeddable distributed k/v store for Go

Drax is a distributed k/v store based on raft (specifically https://github.com/hashicorp/raft)
The intent is to be relatively light-weight and embeddable, while implementing the API from https://github.com/docker/libkv

Drax is NOT intended for production use. Use at your own risk.
This should be considered ALPHA quality.


###TODO:
- Add tests
- Add support for watches
- Serious code cleanup
- Imrpove RPC semantics
- Look at using something other than JSON for encoding/decoding K/V messages, and RPC messages
- Add more documentation (both in code and examples)
