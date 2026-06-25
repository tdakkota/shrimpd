package shrimplication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestIndexEngine_LookupAndFlush(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewIndexEngine("node-1", dir)
	require.NoError(t, err)
	defer engine.Close()

	// Case 1: Lookup incomplete initially because parts are not covered
	candidates := []shrimptypes.PartMeta{{ID: "part-1"}}
	_, complete, err := engine.Lookup(context.Background(), "hello", candidates)
	require.NoError(t, err)
	require.False(t, complete, "should be incomplete when candidates are not marked covered")

	// Mark covered
	require.NoError(t, engine.MarkCovered([]string{"part-1"}))

	// Case 2: Lookup in memtable before flush (label tokens only)
	entries := []shrimptypes.IndexEntry{
		{Token: "lbl:msg=hello", DataID: "part-1"},
		{Token: "lbl:msg=world", DataID: "part-1"},
	}
	require.NoError(t, engine.Write(entries))

	matches, complete, err := engine.LookupTokens(context.Background(), []string{"lbl:msg=hello"}, candidates)
	require.NoError(t, err)
	require.True(t, complete)
	require.Contains(t, matches, "part-1")

	// Case 3: Flush and lookup
	require.NoError(t, engine.Flush(context.Background()))

	// Memtable should be empty now, results from flushed part
	matches2, complete2, err := engine.LookupTokens(context.Background(), []string{"lbl:msg=hello"}, candidates)
	require.NoError(t, err)
	require.True(t, complete2)
	require.Contains(t, matches2, "part-1")

	// Check min/max bounds on flushed metadata
	require.Len(t, engine.parts, 1)
	require.Equal(t, "lbl:msg=hello", engine.parts[0].MinToken)
	require.Equal(t, "lbl:msg=world", engine.parts[0].MaxToken)
}

func TestIndexEngine_MultiTokenLookup(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewIndexEngine("node-1", dir)
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.MarkCovered([]string{"part-1", "part-2"}))

	entries := []shrimptypes.IndexEntry{
		{Token: "lbl:k=hello", DataID: "part-1"},
		{Token: "lbl:k=world", DataID: "part-1"},
		{Token: "lbl:k=hello", DataID: "part-2"},
		{Token: "lbl:k=test", DataID: "part-2"},
	}
	require.NoError(t, engine.Write(entries))
	require.NoError(t, engine.Flush(context.Background()))

	candidates := []shrimptypes.PartMeta{{ID: "part-1"}, {ID: "part-2"}}

	// Querying "lbl:k=hello" should return both parts
	m1, c1, err := engine.LookupTokens(context.Background(), []string{"lbl:k=hello"}, candidates)
	require.NoError(t, err)
	require.True(t, c1)
	require.Len(t, m1, 2)
	require.Contains(t, m1, "part-1")
	require.Contains(t, m1, "part-2")

	// Querying intersection of two label tokens should only return part-1
	m2, c2, err := engine.LookupTokens(context.Background(), []string{"lbl:k=hello", "lbl:k=world"}, candidates)
	require.NoError(t, err)
	require.True(t, c2)
	require.Len(t, m2, 1)
	require.Contains(t, m2, "part-1")
}

func TestIndexEngine_Compaction(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewIndexEngine("node-1", dir)
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.MarkCovered([]string{"part-1", "part-2", "part-3"}))

	// Create two L0 index parts (label tokens)
	require.NoError(t, engine.Write([]shrimptypes.IndexEntry{{Token: "lbl:k=hello", DataID: "part-1"}, {Token: "lbl:k=world", DataID: "part-2"}}))
	require.NoError(t, engine.Flush(context.Background()))

	require.NoError(t, engine.Write([]shrimptypes.IndexEntry{{Token: "lbl:k=hello", DataID: "part-2"}, {Token: "lbl:k=test", DataID: "part-3"}}))
	require.NoError(t, engine.Flush(context.Background()))

	require.Len(t, engine.parts, 2)

	// Compact with active data IDs: "part-1" and "part-2" ("part-3" is stale)
	activeIDs := map[string]struct{}{
		"part-1": {},
		"part-2": {},
	}
	require.NoError(t, engine.Compact(context.Background(), activeIDs))

	// Should have merged L0 parts into one Level 1 part
	require.Len(t, engine.parts, 1)
	require.Equal(t, 1, engine.parts[0].Level)

	// Lookup "lbl:k=test" (which was only in part-3) should not find anything and part-3 should be removed from covered
	candidates := []shrimptypes.PartMeta{{ID: "part-1"}, {ID: "part-2"}}
	m, c, err := engine.LookupTokens(context.Background(), []string{"lbl:k=test"}, candidates)
	require.NoError(t, err)
	require.True(t, c)
	require.Empty(t, m)

	// Verify that covered map is cleaned up
	engine.mu.RLock()
	_, cov3 := engine.covered["part-3"]
	engine.mu.RUnlock()
	require.False(t, cov3, "part-3 should be removed from covered after compaction")
}
