package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/influxmq/influxmq/server/internal/memory/ring"
	"github.com/influxmq/influxmq/server/internal/storage/fs"
)

type LogWriter struct {
	fs fs.FileSystem
	logBuf *ring.Buffer
	logFile fs.File
	idxBuf *ring.Buffer
	idxFile fs.File
	segment uint64
	mu sync.Mutex
	sequenceNumber RecordId
}

func NewLogWriter(fs fs.FileSystem, segment uint64, logBuf []byte, idxBuf []byte) (*LogWriter, error) {

	logFile, err := fs.OpenFile(fmt.Sprintf("%020d.log", segment), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644);

	if err != nil {
		return nil, err
	}

	idxFile, err := fs.OpenFile(fmt.Sprintf("%020d.idx", segment), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644);

	if err != nil {
		ce := logFile.Close()
		return nil, errors.Join(err, ce)
	}

	lw := &LogWriter{
		fs: fs,
		logBuf: ring.NewBuffer(logBuf),
		logFile: logFile,
		idxBuf: ring.NewBuffer(idxBuf),
		idxFile: idxFile,
		segment: segment,
	}

	return lw, nil
}

func (lw *LogWriter) Write(data []byte) (error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	offset, ok := lw.logBuf.Write(data)

	if !ok {
		// roll over here
		newLogFile, err := lw.fs.OpenFile(fmt.Sprintf("%020d.log", lw.segment+1), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

		if err != nil {
			return  err
		}

		newIdxFile, err := lw.fs.OpenFile(fmt.Sprintf("%020d.idx", lw.segment+1), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

		if err != nil {
			ce := newLogFile.Close()
			return errors.Join(err, ce)
		}

		_, err = lw.logBuf.Pipe(lw.logFile)

		if err != nil {
			newIdxFile.Close()
			newLogFile.Close()
			return err
		}

		_, err = lw.idxBuf.Pipe(lw.idxFile)

		if err != nil {
			newIdxFile.Close()
			newLogFile.Close()
			return err
		}

		lw.logFile.Close()
		lw.idxFile.Close()
		lw.logFile = newLogFile
		lw.idxFile = newIdxFile
		lw.logBuf.Clear()
		lw.idxBuf.Clear()
		lw.segment++

		// write again
		offset, _ = lw.logBuf.Write(data)
	}

	// todo: update this method to not alloc, we can just use a slice on the struct
	b, _ := lw.sequenceNumber.Marshal()

	tmp := make([]byte, 0)
	tmp = append(tmp, b...)
	tmp = binary.BigEndian.AppendUint64(tmp, offset)

	// todo: extract rollover into method, call it again here, we lose a segment, no big deal
	// todo: errors
	lw.idxBuf.Write(tmp)

	if !ok {
		// failed the write, need to test this, probably okay to reject
	}

	return nil
}

func (lw *LogWriter) Sync() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	_, e := lw.logBuf.Pipe(lw.logFile)
	_, e2 := lw.idxBuf.Pipe(lw.idxFile)

	return errors.Join(e, e2)
}

func (lw *LogWriter) Close() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	// todo: errors
	lw.Sync()
	lw.logFile.Close()
	lw.idxFile.Close()

	return nil
}
