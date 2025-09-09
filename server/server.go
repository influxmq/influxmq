package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/influxmq/influxmq/server/internal/engine"
	"github.com/influxmq/influxmq/server/internal/memory"
	"github.com/influxmq/influxmq/server/internal/storage/fs"
	"github.com/influxmq/influxmq/server/internal/stream"
	"github.com/panjf2000/gnet/v2"
)


type Server struct {
	e *engine.Engine

	dataPort int
	ctlPort int
}

func NewServer(dataDir string) (*Server, error){	

	dataPort := 9090
	ctlPort := 9091

	requestPool := &sync.Pool{
		New: func() any {
			return make([]byte, 4096)
		},
	}

	rbp := memory.NewBufferPool(requestPool)

	fs := fs.OSFileSystem{
		Dir: "./data",
	}

	segmentPool := &sync.Pool{
		New: func() any {
			return make([]byte, 4 * 1000 * 1000)
		},
	}

	sbp := memory.NewBufferPool(segmentPool)
	
	sm := stream.NewStreamManager(fs, sbp)

	go func() {
		for {
			//todo: temporary flushing here
			time.Sleep(time.Millisecond*100)
			sm.Sync()
		}
	}()

	return &Server{
		e: engine.NewEngine(sm, rbp, dataPort, ctlPort),
		dataPort: dataPort,
		ctlPort: ctlPort,
	}, nil
}

func (s *Server) ListenAndServe() error {
	return gnet.Rotate(s.e, []string{fmt.Sprintf("tcp://:%d", s.dataPort), fmt.Sprintf("tcp://:%d", s.ctlPort)}, gnet.WithMulticore(true), gnet.WithReusePort(true), gnet.WithEdgeTriggeredIO(false))
}