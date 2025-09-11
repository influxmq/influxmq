package memory

type RingBuffer struct {
	buf        []byte
	readPos    int
	writePos   int
	buffered   int
	bufferSize int
}

func NewRingBuffer(b []byte) *RingBuffer {
	return &RingBuffer{
		buf:        b,
		bufferSize: len(b),
	}
}

func (r *RingBuffer) Write(p []byte) (int, bool) {
	n := len(p)
	if n > r.bufferSize-r.buffered {
		return 0, false // not enough space
	}
	for i := 0; i < n; i++ {
		r.buf[(r.writePos+i)%r.bufferSize] = p[i]
	}
	r.writePos = (r.writePos + n) % r.bufferSize
	r.buffered += n
	return n, true
}

func (r *RingBuffer) Peek(n int) ([]byte, bool) {
	if n > r.buffered {
		return nil, false
	}
	if r.readPos+n <= r.bufferSize {
		return r.buf[r.readPos : r.readPos+n], true
	}

	// wrap around
	tmp := make([]byte, n)
	part1 := r.bufferSize - r.readPos
	copy(tmp, r.buf[r.readPos:])
	copy(tmp[part1:], r.buf[:n-part1])
	return tmp, true
}

func (r *RingBuffer) Read(n int) []byte {
	data, ok := r.Peek(n)
	if !ok {
		return nil
	}
	r.readPos = (r.readPos + n) % r.bufferSize
	r.buffered -= n
	return data
}

func (r *RingBuffer) Len() int {
	return r.buffered
}

func (r *RingBuffer) Buffer() []byte {
	return r.buf
}