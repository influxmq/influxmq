package server

import (
	"fmt"
	"sync"

	"github.com/influxmq/influxmq/server/internal/engine"
	"github.com/influxmq/influxmq/server/internal/memory"
	"github.com/influxmq/influxmq/server/internal/storage/fs"
	"github.com/influxmq/influxmq/server/internal/storage/log"
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
	
	fs.MkdirAll(".", 0644)

	w, err := log.NewLogWriter(fs, 0, make([]byte, 1 * 1000 * 1000), make([]byte, 5 * 1000))

	if err != nil {
		return nil, err
	}

	return &Server{
		e: engine.NewEngine(w, rbp, dataPort, ctlPort),
		dataPort: dataPort,
		ctlPort: ctlPort,
	}, nil
}

func (s *Server) ListenAndServe() error {
	return gnet.Rotate(s.e, []string{fmt.Sprintf("tcp://:%d", s.dataPort), fmt.Sprintf("tcp://:%d", s.ctlPort)}, gnet.WithMulticore(true), gnet.WithReusePort(true))
}