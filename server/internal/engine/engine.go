package engine

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"

	sl "github.com/influxmq/influxmq/server/internal/storage/log"

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
	w *sl.LogWriter
	dataPort int
	ctlPort int
}

func NewEngine(w *sl.LogWriter, pool memory.Pool, dataPort, ctlPort int) *Engine {


	go func(){
		for {
			time.Sleep(time.Millisecond*100)
			w.Sync()
		}
	}()

	return &Engine{
		w: w,
		//mpsc: mpsc,
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

	if c.InboundBuffered() < 4 {
		return gnet.None
	}

	header, _ := c.Peek(4)
	msgLen := int(binary.BigEndian.Uint32(header))

	if c.InboundBuffered() < 4+msgLen {
		return gnet.None
	}

	c.Discard(4)
	msg, _ := c.Next(msgLen)

	if c.Context() == trafficData {
		return e.onData(c, msg)
	} else {
		return e.onCtl(c, msg)
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

	log.Printf("stream %s data %s", string(dm.PublishMsg.Stream), string(dm.PublishMsg.Payload))
	return
}

func (e *Engine) onPublish(_ gnet.Conn, msg *proto.PublishMessage) (action gnet.Action) {

	err := e.w.Write(msg.Payload)

	if err != nil {
		log.Printf("Error writing %v", err)
		return gnet.Close
	}

	return
}

func (e *Engine) onCtl(_ gnet.Conn, _ []byte) (action gnet.Action) {
	return
}