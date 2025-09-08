package proto

import (
	"encoding/binary"
	"io"

	"github.com/influxmq/influxmq/server/internal/memory"
)

type MessageType byte

const (
	_ MessageType = iota
	MessagePublish
)

type PublishMessage struct {
	Stream []byte
	Payload []byte
}

type DataMessage struct {
	pool memory.Pool

	MessageType
	PublishMsg *PublishMessage
}

func NewDataMessage(pool memory.Pool) *DataMessage {
	return &DataMessage{
		pool: pool,
	}
}

func (m *DataMessage) Unmarshal(msg []byte) error {

	msgType := MessageType(msg[0])
	m.MessageType = msgType

	switch msgType {
	case MessagePublish:
		return m.unmarshalPublish(msg[1:])
	}

	return ErrUnkownMessageType
}

func (m *DataMessage) Close() {
	switch m.MessageType{
	case MessagePublish:
		m.pool.Return(m.PublishMsg.Stream)
		m.pool.Return(m.PublishMsg.Payload)
	}
}

/*
Format: [streamNameLen][stremName][payloadLen][payload]
*/
func (m *DataMessage) unmarshalPublish(msg []byte) error {
	strm := m.pool.Get()
	payload := m.pool.Get()

	strmNameLen := binary.BigEndian.Uint16(msg)
	n := copy(strm, msg[2:2+strmNameLen])

	if n != int(strmNameLen) {
		return io.ErrShortWrite
	}
	

	payloadLen := binary.BigEndian.Uint16(msg[2+strmNameLen:4+strmNameLen])
	n = copy(payload, msg[4+strmNameLen:4+strmNameLen+payloadLen])

	if n != int(payloadLen) {
		m.pool.Return(strm)
		m.pool.Return(payload)
		return io.ErrShortWrite
	}

	m.PublishMsg = &PublishMessage{
		Stream: strm[:strmNameLen],
		Payload: payload[:payloadLen],
	}

	return nil
}