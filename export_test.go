package shrimpd

import "context"

// FlushForTest exposes the unexported flush method for tests and benchmarks.
func (l *LSM) FlushForTest(ctx context.Context) error {
	return l.flush(ctx)
}

// CompactForTest exposes the unexported compact method for tests and benchmarks.
func (l *LSM) CompactForTest(ctx context.Context, force bool) error {
	return l.compact(ctx, force)
}

// IndexEngineForTest exposes the unexported idxEngine field for tests and benchmarks.
func (l *LSM) IndexEngineForTest() *IndexEngine {
	return l.idxEngine
}
