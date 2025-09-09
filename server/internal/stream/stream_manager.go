package stream

import (
	"github.com/influxmq/influxmq/server/internal/memory"
	"github.com/influxmq/influxmq/server/internal/storage/fs"
	"github.com/influxmq/influxmq/server/internal/storage/log"
)


type StreamManager struct {
	fs fs.FileSystem
	pool memory.Pool
	writers memory.LazyMap[*log.StreamWriter]
}

func NewStreamManager(fs fs.FileSystem, pool memory.Pool) *StreamManager {
	return &StreamManager{
		fs: fs,
		pool: pool,
	}
}

func (sm *StreamManager) Write(stream string, message []byte) (log.RecordId, error) {

	w, err := sm.writers.GetOrCreate(stream, func() (*log.StreamWriter, error) {
		return log.NewStreamWriter(sm.fs, stream, sm.pool.Get(), sm.pool.Get())
	})

	if err != nil {
		return log.RecordId{}, err
	}

	return w.Write(message)
}

func (sm *StreamManager) Sync() {
	sm.writers.Range(func(key string, val *log.StreamWriter) bool {
		val.Flush()
		return true
	})
}

