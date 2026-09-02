// Package hash is the FNV-1a name hash shared by the core and extension controllers for pod and pool labels.
package hash

import (
	"fmt"
	"hash/fnv"
)

// Name returns the FNV-1a hash of s as an 8-character hexadecimal string.
func Name(s string) string {
	return fmt.Sprintf("%08x", numeric(s))
}

func numeric(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
