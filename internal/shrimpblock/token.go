package shrimpblock

import (
	"iter"
	"slices"
	"strings"
	"unicode"

	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// Tokenize splits a string into lowercase tokens, using non-letter and non-number characters as delimiters.
func Tokenize(s string) iter.Seq[string] {
	return func(yield func(tok string) bool) {
		token := func(tok string) bool {
			tok = strings.ToLower(tok)
			return yield(tok)
		}
		seq := strings.FieldsFuncSeq(s, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsNumber(r)
		})
		for tok := range seq {
			if !token(tok) {
				return
			}
		}
	}
}

// MaxTokenSetSize caps the per-part token list stored in PartMeta to prevent
// runaway memory and etcd payload growth on large or diverse parts.
// Parts exceeding this cap fall back to full block-level bloom filter scanning.
const MaxTokenSetSize = 10_000

// BuildTokenSet builds a sorted, deduplicated set of tokens from the given entries.
func BuildTokenSet(entries []shrimptypes.Entry) ([]string, bool) {
	var (
		seen = make(map[string]struct{})
		out  []string
	)
	for _, e := range entries {
		for tok := range Tokenize(e.Data) {
			if _, ok := seen[tok]; ok {
				continue
			}
			if len(seen) == MaxTokenSetSize {
				slices.Sort(out) // deterministic
				return out, true
			}
			seen[tok] = struct{}{}
			out = append(out, tok)
		}
	}
	slices.Sort(out) // deterministic
	return out, false
}

// HasToken checks if the given term can be satisfied by the provided sorted token list.
func HasToken(tokens []string, term string) bool {
	if term == "" || len(tokens) == 0 {
		return true // empty term or no Tokens (legacy part or remote not reindexed locally): cannot prune
	}
	// tokens are sorted; require every sub-token from term to be present (case-insensitive)
	for tok := range Tokenize(term) {
		if _, found := slices.BinarySearch(tokens, tok); !found {
			return false
		}
	}
	return true
}
