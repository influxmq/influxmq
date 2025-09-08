package memory

import (
	"sync"
)

type BufferPool struct {
	pool *sync.Pool
}

func NewBufferPool(pool *sync.Pool) *BufferPool {
	b := &BufferPool{
		pool: pool,
	}

	return b
}

func (b *BufferPool) Get() []byte {
	return b.pool.Get().([]byte)
}

func (b *BufferPool) Return(mem []byte) {
	b.pool.Put(mem[:cap(mem)])
}