package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestParseTime(t *testing.T) {
	// Test default
	val, err := parseTime("", 123)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), val)

	// Test nanoseconds as integer
	val, err = parseTime("1719080000000000000", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1719080000000000000), val)

	// Test relative duration
	now := time.Now().UnixNano()
	val, err = parseTime("5m", 0)
	assert.NoError(t, err)
	// it should be approximately 5 minutes ago
	diff := now - val
	assert.True(t, diff >= 0)
	assert.True(t, diff < int64(6*time.Minute))

	// Test invalid
	_, err = parseTime("invalid", 0)
	assert.Error(t, err)
}

func TestFormatEntry(t *testing.T) {
	// Test plain text
	e1 := shrimptypes.Entry{
		Timestamp: 1000000000,
		Data:      "plain text log",
	}
	formatted := formatEntry(e1)
	assert.Contains(t, formatted, "1970-01-01")
	assert.Contains(t, formatted, "plain text log")

	// Test OTLP JSON log
	otlpJSON := `{
		"severity_text": "ERROR",
		"body": "something failed",
		"attributes": {"key": "value"},
		"trace_id": "12345"
	}`
	e2 := shrimptypes.Entry{
		Timestamp: 1719080000000000000,
		Data:      otlpJSON,
	}
	formatted2 := formatEntry(e2)
	assert.Contains(t, formatted2, "ERROR")
	assert.Contains(t, formatted2, "something failed")
	assert.Contains(t, formatted2, "attrs:")
	assert.Contains(t, formatted2, "trace_id: 12345")
}
