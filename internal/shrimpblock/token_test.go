package shrimpblock

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"Hello, World!", []string{"hello", "world"}},
		{"foo BAR foo", []string{"foo", "bar", "foo"}},
		{"123 abc-xyz", []string{"123", "abc", "xyz"}},
		{"foo.BAR.foo", []string{"foo", "bar", "foo"}},
		{"", nil},
	}
	for _, c := range cases {
		got := slices.Collect(Tokenize(c.in))
		require.Equal(t, c.want, got, "tokenize(%q)", c.in)
	}
}

func TestBuildTokenSet(t *testing.T) {
	ents := []shrimptypes.Entry{{Data: "Hello World"}, {Data: "hello foo"}}
	got := BuildTokenSet(ents)
	want := []string{"foo", "hello", "world"}
	require.Equal(t, want, got)
}

func TestHasToken(t *testing.T) {
	toks := []string{"a", "b", "c"}
	require.True(t, HasToken(toks, "B"), "case insensitive")
	require.False(t, HasToken(toks, "z"), "miss")
	require.True(t, HasToken(nil, ""), "empty term")
}

func TestTokenPruning(t *testing.T) {
	ents := []shrimptypes.Entry{
		{Timestamp: 1, Data: "hello world"},
		{Timestamp: 2, Data: "foo bar"},
		{Timestamp: 3, Data: "hello foo"},
	}

	tokens := BuildTokenSet(ents)
	expectedTokens := []string{"bar", "foo", "hello", "world"}
	require.Equal(t, expectedTokens, tokens)

	require.True(t, HasToken(tokens, "hello"))
	require.True(t, HasToken(tokens, "world"))
	require.False(t, HasToken(tokens, "nonexistent"))
	require.True(t, HasToken(tokens, "HELLO"), "case insensitive")
	require.True(t, HasToken(tokens, ""), "empty term")
	require.True(t, HasToken(nil, ""), "nil tokens with empty term")
	require.True(t, HasToken(nil, "hello"), "nil tokens with non-empty term (graceful degradation)")
}
