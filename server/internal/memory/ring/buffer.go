package ring

import "io"

type Buffer struct {
	lowWatermark  uint64
	highWatermark uint64
	buf           []byte
}

func NewBuffer(buf []byte) *Buffer {
	return &Buffer{
		buf: buf,
	}
}

/*
Writes returns the high water mark and if the data was written.
*/
func (b *Buffer) Write(data []byte) (uint64, bool) {
	// must lock before calling
	if uint64(len(data))+b.highWatermark > uint64(len(b.buf)) {
		return b.highWatermark, false
	}

	n := copy(b.buf[b.highWatermark:], data)
	b.highWatermark += uint64(n)

	return b.highWatermark, true
}

func (b *Buffer) Pipe(w io.Writer) (int, error) {
	// must lock before calling

	writen := 0
	
	for b.lowWatermark < b.highWatermark {
		n, err := w.Write(b.buf[b.lowWatermark:b.highWatermark])

		writen += n

		if err != nil {
			return writen, err
		}

		b.lowWatermark += uint64(n)
	}

	return writen, nil
}

func (b *Buffer) Clear() {
	b.lowWatermark = 0
	b.highWatermark = 0
}