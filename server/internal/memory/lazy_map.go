package memory

import "sync"

type LazyMap[T any] struct {
	m sync.Map
}

type entry[T any] struct {
	once sync.Once
	val  T
	err  error
}

// GetOrCreate returns the value for the key or calls the initializer exactly once.
func (lm *LazyMap[T]) GetOrCreate(key string, initFn func() (T, error)) (T, error) {
	actual, _ := lm.m.LoadOrStore(key, &entry[T]{})

	e := actual.(*entry[T])
	e.once.Do(func() {
		e.val, e.err = initFn()
	})

	return e.val, e.err
}

// Delete removes a key from the map
func (lm *LazyMap[T]) Delete(key string) {
	lm.m.Delete(key)
}

// Range allows iteration over the map contents
func (lm *LazyMap[T]) Range(f func(key string, val T) bool) {
	lm.m.Range(func(k, v any) bool {
		e := v.(*entry[T])
		// Note: We assume init has been completed here
		return f(k.(string), e.val)
	})
}