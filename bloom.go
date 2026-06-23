package shrimpd

import "github.com/zeebo/xxh3"

const (
	bloomBits  = 8192
	bloomBytes = bloomBits / 8
	bloomK     = 4
)

func bloomAdd(b *[bloomBytes]byte, token string) {
	h := xxh3.HashString128(token)
	h1, h2 := h.Lo, h.Hi
	for i := range bloomK {
		setBit(b, h1, h2, i)
	}
}

func bloomMightContain(b *[bloomBytes]byte, token string) bool {
	h := xxh3.HashString128(token)
	h1, h2 := h.Lo, h.Hi
	for i := range bloomK {
		if getBit(b, h1, h2, i) {
			return false
		}
	}
	return true
}

func indexBit(h1, h2 uint64, i int) uint64 {
	return (h1 + uint64(i)*h2) % bloomBits
}

func setBit(b *[bloomBytes]byte, h1, h2 uint64, i int) {
	index := indexBit(h1, h2, i)
	b[index/8] |= 1 << (index % 8)
}

func getBit(b *[bloomBytes]byte, h1, h2 uint64, i int) bool {
	index := indexBit(h1, h2, i)
	val := b[index/8] & (1 << (index % 8))
	return val != 0
}
