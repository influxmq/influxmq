package log

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/influxmq/influxmq/server/internal/storage/fs"
)

type RecordId = UInt128

var ErrMessageTooLarge = errors.New("message too large")

type StreamWriter struct {
	fs fs.FileSystem
	stream        string
	sequenceNumber uint64
	segmentNumber uint64
	segment *Segment
	mu sync.Mutex
	segBuf []byte
}

func NewStreamWriter(fs fs.FileSystem, stream string, segBuf []byte, idxBuf []byte) (*StreamWriter, error) {
	err := fs.MkdirAll(stream, 0666); if err != nil {
		return nil, err
	}
	
	segmentNum, err := getLatestSegmentFile(fs, stream); if err != nil {
		return nil, err
	}

	segment, err := NewSegment(fs, stream, segmentNum, segBuf); if err != nil {
		return nil, err
	}

	//todo: when opening index, if fails close segment

	sw := &StreamWriter{
		fs: fs,
		stream: stream,
		sequenceNumber: 0, // todo
		segmentNumber: segmentNum,
		segment: segment,
		segBuf: segBuf,
	}

	return sw, nil
}

func (sw *StreamWriter) Write(data []byte) (RecordId, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.sequenceNumber++

	// todo: index result
	_, ok := sw.segment.Write(data)

	if !ok {
		// open new segment
		newSegment, err := NewSegment(sw.fs, sw.stream, sw.segmentNumber+1, sw.segBuf)

		if err != nil {
			return RecordId{}, err
		}

		err = sw.segment.Close(); if err != nil {
			err2 := newSegment.Close()
			return RecordId{}, errors.Join(err, err2)
		}

		sw.segment = newSegment
		sw.segmentNumber++

		_, ok = sw.segment.Write(data); if !ok {
			return RecordId{}, ErrMessageTooLarge
		}
	}

	return RecordId{Hi: sw.segmentNumber, Lo: sw.sequenceNumber}, nil
}

func (sw *StreamWriter) Flush() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	//todo: log
	sw.segment.Flush()
}

func getLatestSegmentFile(fs fs.FileSystem, stream string) (uint64, error) {

	entries, err := fs.ReadDir(stream); if err != nil {
		return 0, err
	}

	var segment uint64

	suffix := fmt.Sprintf(".%s", ".log")

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		if !strings.HasSuffix(fileName, suffix) {
			continue
		}

		base := strings.TrimSuffix(fileName, suffix)

		s, err := strconv.ParseUint(base, 10, 10); if err != nil {
			continue
		}

		if  s > segment {
			segment = s
		}
	}

	return segment, nil
}