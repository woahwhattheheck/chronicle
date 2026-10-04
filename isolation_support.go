package chronicle

// Compile-only collaborators for the full sidecar source when the normal root
// package cannot build. The selected handler/target test never invokes storage.
type DB struct{}

func (*DB) WriteBatch([]Point) error { panic("storage is outside this selected test") }

type Point struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Value     float64
}

func (point *Point) ensureTags() {
	if point.Tags == nil {
		point.Tags = make(map[string]string)
	}
}

