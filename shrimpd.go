// Package shrimpd is the root package for the shrimpd library.
package shrimpd

import "github.com/oteldb/shrimpd/internal/shrimpfilter"

type (
	// LineOp is the operation type for line filters.
	LineOp = shrimpfilter.LineOp
	// LabelOp is the operation type for label filters.
	LabelOp = shrimpfilter.LabelOp
	// LineFilter is a filter for matching lines.
	LineFilter = shrimpfilter.LineFilter
	// LabelFilter is a filter for matching labels.
	LabelFilter = shrimpfilter.LabelFilter
	// Matcher is a compiled set of line and label filters.
	Matcher = shrimpfilter.Matcher
)

// Line and label filter operation aliases.
const (
	OpLineEq    = shrimpfilter.OpLineEq
	OpLineNotEq = shrimpfilter.OpLineNotEq
	OpLineRe    = shrimpfilter.OpLineRe
	OpLineNotRe = shrimpfilter.OpLineNotRe

	OpLabelEq    = shrimpfilter.OpLabelEq
	OpLabelNotEq = shrimpfilter.OpLabelNotEq
	OpLabelRe    = shrimpfilter.OpLabelRe
	OpLabelNotRe = shrimpfilter.OpLabelNotRe
)

// CompileMatcher compiles a set of line and label filters into a Matcher.
var CompileMatcher = shrimpfilter.CompileMatcher
