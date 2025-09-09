package log

import (
	"fmt"
	"path/filepath"
)

func formatSegmentFileName(stream string, segment uint64) string {
	return filepath.Join(stream, fmt.Sprintf("%020d.log", segment))
}


func formatIndexFileName(stream string, sequenceNumber uint64) string {
	return filepath.Join(stream, fmt.Sprintf("%020d.log", sequenceNumber))
}