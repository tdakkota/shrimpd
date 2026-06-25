package shrimpblock

import (
	"github.com/zeebo/xxh3"

	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func bloomSetHash(b *shrimptypes.BloomFilter, h xxh3.Uint128) {
	h1, h2 := h.Lo, h.Hi
	for i := range shrimptypes.BloomK {
		setBit(b, h1, h2, i)
	}
}

func bloomMightContainHash(b *shrimptypes.BloomFilter, h xxh3.Uint128) bool {
	h1, h2 := h.Lo, h.Hi
	for i := range shrimptypes.BloomK {
		if !getBit(b, h1, h2, i) {
			return false
		}
	}
	return true
}

// BloomAdd adds a token to the given bloom filter. The filter is modified in place.
func BloomAdd(b *shrimptypes.BloomFilter, token string) {
	bloomSetHash(b, xxh3.HashString128(token))
}

// BloomMightContain checks whether the given token might be present in the bloom filter.
// Returns true if it might be present, false if it is definitely not present.
func BloomMightContain(b *shrimptypes.BloomFilter, token string) bool {
	return bloomMightContainHash(b, xxh3.HashString128(token))
}

// BloomAddLabel adds a label key=value pair to the given bloom filter. The filter is modified in place.
func BloomAddLabel[S ~string | ~[]byte](b *shrimptypes.BloomFilter, key, value S) {
	var buf [256]byte
	n := copy(buf[:], "lbl:")
	n += copy(buf[n:], key)
	buf[n] = '='
	n++
	n += copy(buf[n:], value)
	bloomSetHash(b, xxh3.Hash128(buf[:n]))
}

func bloomAddBytes(b *shrimptypes.BloomFilter, token []byte) {
	bloomSetHash(b, xxh3.Hash128(token))
}

func bloomMightContainBytes(b *shrimptypes.BloomFilter, token []byte) bool {
	return bloomMightContainHash(b, xxh3.Hash128(token))
}

// BloomMightContainLabel checks whether a label key=value pair might be present
// in the given bloom filter using a stack-allocated "lbl:key=value" token.
func BloomMightContainLabel(b *shrimptypes.BloomFilter, key, value string) bool {
	var buf [256]byte
	n := copy(buf[:], "lbl:")
	n += copy(buf[n:], key)
	buf[n] = '='
	n++
	n += copy(buf[n:], value)
	return bloomMightContainBytes(b, buf[:n])
}

func indexBit(h1, h2 uint64, i int) uint64 {
	return (h1 + uint64(i)*h2) % shrimptypes.BloomBits
}

func setBit(b *shrimptypes.BloomFilter, h1, h2 uint64, i int) {
	index := indexBit(h1, h2, i)
	b[index/8] |= 1 << (index % 8)
}

func getBit(b *shrimptypes.BloomFilter, h1, h2 uint64, i int) bool {
	index := indexBit(h1, h2, i)
	val := b[index/8] & (1 << (index % 8))
	return val != 0
}
