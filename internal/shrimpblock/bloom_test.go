package shrimpblock

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestBloomAddAndContain(t *testing.T) {
	var b shrimptypes.BloomFilter

	BloomAdd(&b, "compacted")
	require.True(t, BloomMightContain(&b, "compacted"),
		"must contain token that was added")
}

func TestBloomMissing(t *testing.T) {
	var b shrimptypes.BloomFilter

	BloomAdd(&b, "compacted")
	require.False(t, BloomMightContain(&b, "other"),
		"must not contain token that was never added")
}

func TestBloomLabelToken(t *testing.T) {
	var b shrimptypes.BloomFilter

	BloomAdd(&b, "lbl:service_name=svc-a")
	require.True(t, BloomMightContainLabel(&b, "service_name", "svc-a"),
		"must contain label token that was added")
	require.False(t, BloomMightContainLabel(&b, "service_name", "svc-b"),
		"must not contain label token that was never added")
}

func TestBloomMultiple(t *testing.T) {
	var b shrimptypes.BloomFilter

	for _, tok := range []string{"compacted", "index", "parts", "errors", "logs"} {
		BloomAdd(&b, tok)
	}

	for _, tok := range []string{"compacted", "index", "parts", "errors", "logs"} {
		require.True(t, BloomMightContain(&b, tok),
			"must contain added token %q", tok)
	}

	require.False(t, BloomMightContain(&b, "nonexistent"),
		"must not contain unadded token")
}

// Benchmarks

func BenchmarkBloomAdd(b *testing.B) {
	var bloom shrimptypes.BloomFilter
	b.ResetTimer()
	for range b.N {
		BloomAdd(&bloom, "service_name=svc-a")
	}
}

func BenchmarkBloomAddBytes(b *testing.B) {
	var bloom shrimptypes.BloomFilter
	tok := []byte("service_name=svc-a")
	b.ResetTimer()
	for range b.N {
		bloomAddBytes(&bloom, tok)
	}
}

func BenchmarkBloomMightContainHit(b *testing.B) {
	var bloom shrimptypes.BloomFilter
	BloomAdd(&bloom, "compacted")
	b.ResetTimer()
	for range b.N {
		BloomMightContain(&bloom, "compacted")
	}
}

func BenchmarkBloomMightContainMiss(b *testing.B) {
	var bloom shrimptypes.BloomFilter
	BloomAdd(&bloom, "compacted")
	b.ResetTimer()
	for range b.N {
		BloomMightContain(&bloom, "nonexistent")
	}
}

func BenchmarkBloomMightContainLabel(b *testing.B) {
	var bloom shrimptypes.BloomFilter
	BloomAdd(&bloom, "lbl:service_name=svc-a")
	b.ResetTimer()
	for range b.N {
		BloomMightContainLabel(&bloom, "service_name", "svc-a")
	}
}

func BenchmarkBloomMightContainLabelStringConcat(b *testing.B) {
	var bloom shrimptypes.BloomFilter
	BloomAdd(&bloom, "lbl:service_name=svc-a")
	key, val := "service_name", "svc-a"
	b.ResetTimer()
	for range b.N {
		// old string concatenation variant (dynamic strings, no constant folding)
		BloomMightContain(&bloom, "lbl:"+key+"="+val)
	}
}
