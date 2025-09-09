package engine

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"

	"github.com/influxmq/influxmq/server/internal/stream"

	"github.com/influxmq/influxmq/server/internal/memory"
	"github.com/influxmq/influxmq/server/internal/proto"
	"github.com/panjf2000/gnet/v2"
)

type tafficType int

const (
	trafficData tafficType = iota
	trafficCtl
)

type Engine struct {
	*gnet.BuiltinEventEngine
	pool memory.Pool
	sm *stream.StreamManager
	dataPort int
	ctlPort int
}

func NewEngine(sm *stream.StreamManager, pool memory.Pool, dataPort, ctlPort int) *Engine {
	return &Engine{
		sm: sm,
		pool: pool,
		dataPort: dataPort,
		ctlPort: ctlPort,
	}
}

func (e *Engine) OnBoot(_ gnet.Engine) (action gnet.Action) {
	fmt.Print("Server ready")
	return gnet.None
}

func (e *Engine) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	port := c.LocalAddr().(*net.TCPAddr).Port

	switch port {
	case e.dataPort:
		c.SetContext(trafficData)
	case e.ctlPort:
		c.SetContext(trafficCtl)
	}

	return
}

/*
Data messages: [len][message]
Ctl message: [len][message]
*/
func (e *Engine) OnTraffic(c gnet.Conn) (gnet.Action) {

	for {
		if c.InboundBuffered() < 4 {
			return gnet.None
		}

		header, _ := c.Peek(4)
		msgLen := int(binary.BigEndian.Uint32(header))

		if c.InboundBuffered() < 4+msgLen {
			return gnet.None
		}

		c.Discard(4) // skip header

		// read the next message length
		msg, _ := c.Next(msgLen)

		if c.Context() == trafficData {
			e.onData(c, msg)
		} else {
			e.onCtl(c, msg)
		}
	}
}

func (e *Engine) onData(c gnet.Conn, msg []byte) (action gnet.Action) {

	dm := proto.NewDataMessage(e.pool)

	err := dm.Unmarshal(msg); if err != nil {
		log.Printf("could not unmsarhall data message %v", err)
		return gnet.Close
	}

	defer dm.Close()

	switch dm.MessageType {
	case proto.MessagePublish:
		return e.onPublish(c, dm.PublishMsg)
	}

	return
}

func (e *Engine) onPublish(c gnet.Conn, msg *proto.PublishMessage) (action gnet.Action) {
	s := string(msg.Stream)

	rid, err := e.sm.Write(s, msg.Payload)

	if err != nil {
		log.Printf("Error writing %v for stream %s", err, s)
	}

	resp, _ := rid.Marshal()
	c.AsyncWrite(resp, nil)

	return
}

func (e *Engine) onCtl(_ gnet.Conn, _ []byte) (action gnet.Action) {
	return
}