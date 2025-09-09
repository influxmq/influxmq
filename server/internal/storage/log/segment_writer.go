package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/influxmq/influxmq/server/internal/storage/fs"
)

const (
	headerSize uint64 = 8
)

type Segment struct {
	file fs.File
	fileBuf bufio.Writer

	buf []byte
	lowWatermark  uint64
	highWatermark uint64
}

func NewSegment(fs fs.FileSystem, stream string, segment uint64, buf []byte) (*Segment, error) {

	file, err := fs.OpenFile(formatSegmentFileName(stream, segment), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)

	if err != nil {
		return nil, err
	}

	pos, err := file.Read(buf)

	if err != nil && err != io.EOF {
		file.Close()// todo: error check
		return nil, err
	}

	seg := &Segment{
		file: file,
		fileBuf: *bufio.NewWriter(file),
		buf: buf,
		lowWatermark: uint64(pos),
	}

	seg.highWatermark = seg.lowWatermark

	return seg, nil
}

/*
Writes to the segment and returns the position the record was written at, if the write was attmpted, and and errors if writing was attempted
*/
func (s *Segment) Write(data []byte) (uint64, bool) {
	size := uint64(len(data))

	if size+s.highWatermark+headerSize > uint64(len(s.buf)) {
		return s.highWatermark, false
	}

	offset := s.highWatermark

	binary.LittleEndian.PutUint64(s.buf[s.highWatermark:], size)
	n := copy(s.buf[s.highWatermark+headerSize:], data)
	s.highWatermark += uint64(n) + headerSize
	
	return offset, true
}

/*
Syncs the segment to disk
*/
func (s *Segment) Flush() (int, error) {
	writen := 0

	for s.lowWatermark < s.highWatermark {
		n, err := s.fileBuf.Write(s.buf[s.lowWatermark:s.highWatermark])
		writen += n
		s.lowWatermark += uint64(n)

		if err != nil {
			return writen, err
		}
	}

	fErr := s.fileBuf.Flush()
	sErr := s.file.Sync()

	return writen, errors.Join(fErr, sErr)
}

func (s *Segment) Close() error {
	_, fErr := s.Flush()
	sErr := s.file.Close()

	return errors.Join(fErr, sErr)
}