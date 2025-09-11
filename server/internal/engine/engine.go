package engine

import (
	"encoding/binary"
	"fmt"

	"github.com/influxmq/influxmq/server/internal/stream"
	"github.com/influxmq/influxmq/server/proto"

	"github.com/influxmq/influxmq/server/internal/memory"
	"github.com/panjf2000/gnet/v2"
)

type Engine struct {
	*gnet.BuiltinEventEngine
	pool memory.Pool
	sm *stream.StreamManager

}

func NewEngine(sm *stream.StreamManager, pool memory.Pool) *Engine {
	return &Engine{
		sm: sm,
		pool: pool,
	}
}

func (e *Engine) OnBoot(_ gnet.Engine) (action gnet.Action) {
	fmt.Print("Server ready")
	return gnet.None
}

func (e *Engine) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	c.SetContext(memory.NewRingBuffer(e.pool.Get()))
	return
}

func (e *Engine) OnClose(c gnet.Conn, _ error) (action gnet.Action) {
	ring := c.Context().(*memory.RingBuffer)
	e.pool.Return(ring.Buffer())
	return
}


func (e *Engine) OnTraffic(c gnet.Conn) (gnet.Action) {

	buf, err := c.Next(-1); if err != nil {
		return gnet.Close
	}

	ring := c.Context().(*memory.RingBuffer)

	if !ProcessWithRingSupport(buf, ring) {
		return gnet.Close
	}

	return gnet.None
}


func ProcessWithRingSupport(buf []byte, ring *memory.RingBuffer) bool {
	offset := 0
	bufLen := len(buf)

	for {
		if ring.Len() > 0 {
			// Combine ring + buf for logical full message
			if ring.Len() < 4 {
				// Not enough to get length — append what we can
				_, ok := ring.Write(buf[offset:])
				return ok
			}

			lenBytes, _ := ring.Peek(4)
			msgLen := binary.LittleEndian.Uint32(lenBytes)
			totalLen := 4 + int(msgLen)

			available := ring.Len() + (bufLen - offset)
			if available < totalLen {
				// Still not enough — copy rest of buf and wait
				_, ok := ring.Write(buf[offset:])
				return ok
			}

			// Now we can fulfill the message by combining
			temp := make([]byte, totalLen)
			n := copy(temp, ring.Read(ring.Len()))
			copy(temp[n:], buf[offset:offset+(totalLen-n)])
			offset += totalLen - n

			var msg proto.Message
			if _, err := msg.Read(temp); err != nil {
				fmt.Println("Parse error:", err)
				return true
			}

			// ==========================
			// message ready
			// ==========================

			fmt.Print(msg)


		} else {
			// Ring is empty, parse directly from buf
			if bufLen-offset < 4 {
				_, ok := ring.Write(buf[offset:])
				return ok
			}

			msgLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
			totalLen := 4 + int(msgLen)

			if bufLen-offset < totalLen {
				// Partial message — copy into ring
				_, ok := ring.Write(buf[offset:])
				return ok
			}

			// Full message
			msgBuf := buf[offset : offset+totalLen]

			var msg proto.Message
			if _, err := msg.Read(msgBuf); err != nil {
				fmt.Println("Parse error:", err)
				return true
			}

			// ==========================
			// message ready
			// ==========================

			fmt.Print(msg)

			offset += totalLen
		}
	}
}