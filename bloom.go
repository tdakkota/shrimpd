package shrimpd

import (
	"hash/fnv"

	"github.com/cespare/xxhash/v2"
)

const (
	bloomBits  = 8192
	bloomBytes = bloomBits / 8
	bloomK     = 4
)

func bloomAdd(b *[bloomBytes]byte, token string) {
	for i := range bloomK {
		h := bloomHash(token, i)
		bit := h % bloomBits
		b[bit/8] |= 1 << (bit % 8)
	}
}

func bloomMightContain(b *[bloomBytes]byte, token string) bool {
	for i := range bloomK {
		h := bloomHash(token, i)
		bit := h % bloomBits
		if b[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func bloomHash(token string, i int) uint64 {
	h1 := xxhash.Sum64String(token)
	fnvHash := fnv.New64a()
	_, _ = fnvHash.Write([]byte(token))
	h2 := fnvHash.Sum64()
	return h1 + uint64(i)*h2 + uint64(i*i)
}
