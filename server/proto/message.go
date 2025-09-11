package proto

import (
	"encoding/binary"
	"fmt"
	"io"
)


type MessageId uint16
type MessageType uint8

type StreamId uint16
type PartitionId uint32

const (
	_ MessageType = iota
	MessageAck
	MessageNack

	// client sent messages
	MessageConnect
	MessageDisconnect
	MessagePublish
	MessageSubscribe
	MessageCheckpoint

	// server sent messages
	MessageMoving
	MessageMoved
	MessageData
	MessageSplitting
	MessageMerging
)

const (
	LenFieldSize   = 4 // uint32
	IdFieldSize    = 2 // uint16
	TypeFieldSize  = 1 // uint8
	HeaderSize     = LenFieldSize + IdFieldSize + TypeFieldSize // 7 bytes
)

// WireFormat: [len][id][type][payload]
type Message struct {
	MessageId
	MessageType

	// all optional below based on MessageType
	*ConnectMessage
	*DisconnectMessage
	*PublishMessage
	*SubscribeMessage
	*CheckpointMessage
	*MovingMessage
	*MovedMessage
	*DataMessage
	*SplittingMessage
	*MergingMessage
}

func (m *Message) Read(buf []byte) (int, error) {
	if len(buf) < HeaderSize+LenFieldSize {
		return 0, io.ErrUnexpectedEOF
	}

	offset := 0

	// read len (uint32)
	payloadLen := binary.LittleEndian.Uint32(buf[offset:])
	totalLen := LenFieldSize + int(payloadLen)
	if len(buf) < totalLen {
		return 0, io.ErrUnexpectedEOF
	}
	offset += LenFieldSize

	// id (uint16)
	m.MessageId = MessageId(binary.LittleEndian.Uint16(buf[offset:]))
	offset += IdFieldSize

	// type (uint8)
	m.MessageType = MessageType(buf[offset])
	offset += TypeFieldSize

	// payload
	payload := buf[offset:totalLen]

	switch m.MessageType {
	case MessageConnect:
		var cm ConnectMessage
		if err := cm.read(payload); err != nil {
			return 0, err
		}
		m.ConnectMessage = &cm

	case MessageDisconnect:
		var dm DisconnectMessage
		if err := dm.read(payload); err != nil {
			return 0, err
		}
		m.DisconnectMessage = &dm

	case MessagePublish:
		var pm PublishMessage
		if err := pm.read(payload); err != nil {
			return 0, err
		}
		m.PublishMessage = &pm

	case MessageSubscribe:
		var sm SubscribeMessage
		if err := sm.read(payload); err != nil {
			return 0, err
		}
		m.SubscribeMessage = &sm

	case MessageCheckpoint:
		var cpm CheckpointMessage
		if err := cpm.read(payload); err != nil {
			return 0, err
		}
		m.CheckpointMessage = &cpm

	case MessageMoving:
		var mm MovingMessage
		if err := mm.read(payload); err != nil {
			return 0, err
		}
		m.MovingMessage = &mm

	case MessageMoved:
		var mm MovedMessage
		if err := mm.read(payload); err != nil {
			return 0, err
		}
		m.MovedMessage = &mm

	case MessageData:
		var dm DataMessage
		if err := dm.read(payload); err != nil {
			return 0, err
		}
		m.DataMessage = &dm

	case MessageSplitting:
		var sm SplittingMessage
		if err := sm.read(payload); err != nil {
			return 0, err
		}
		m.SplittingMessage = &sm

	case MessageMerging:
		var mm MergingMessage
		if err := mm.read(payload); err != nil {
			return 0, err
		}
		m.MergingMessage = &mm

	default:
		return 0, fmt.Errorf("unknown message type: %d", m.MessageType)
	}


	return totalLen, nil
}

// Client to server
// The producer or consumer wants to connect to a stream
// Responses: ACK, NACK
// ErrorCodes: ErrorCodeUnknownStream
// WireFormat: [streamId][partitionId][streamName]
type ConnectMessage struct {
	Stream string
	StreamId
	PartitionId
}

func (m *ConnectMessage) read(buf []byte) error {
	if len(buf) < 6 {
		return io.ErrUnexpectedEOF
	}
	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	m.Stream = string(buf[6:])
	return nil
}

func (m *ConnectMessage) ToMessageBytes(msgId MessageId) []byte {
	streamNameBytes := []byte(m.Stream)

	// Payload is: streamId (2) + partitionId (4) + streamName
	payloadLen := 2 + 4 + len(streamNameBytes)

	// Total length field: id (2) + type (1) + payload
	lengthField := IdFieldSize + TypeFieldSize + payloadLen

	// Full message buffer: len (4) + lengthField
	buf := make([]byte, LenFieldSize+lengthField)

	offset := 0

	// len (uint32) — everything after this
	binary.LittleEndian.PutUint32(buf[offset:], uint32(lengthField))
	offset += LenFieldSize

	// id (uint16)
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type (uint8)
	buf[offset] = uint8(MessageConnect)
	offset += TypeFieldSize

	// streamId (uint16)
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId (uint32)
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// streamName (bytes)
	copy(buf[offset:], streamNameBytes)

	return buf
}

// Client to server
// The producer or consumer is disconnecting from the partition
// Responses: ACK
// ErrorCodes: ErrorCodeNotConnectedToStream
// WireFormat: [streamId][partitionId]
type DisconnectMessage struct {
	StreamId
	PartitionId
}

func (m *DisconnectMessage) read(buf []byte) error {
	if len(buf) < 6 {
		return io.ErrUnexpectedEOF
	}
	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	return nil
}

func (m *DisconnectMessage) ToMessageBytes(msgId MessageId) []byte {
	// Payload: streamId (2) + partitionId (4)
	payloadLen := 2 + 4
	lengthField := IdFieldSize + TypeFieldSize + payloadLen

	buf := make([]byte, LenFieldSize+lengthField)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(lengthField))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageDisconnect)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	// offset += 4 // optional, not needed

	return buf
}

// Client to server
// Producer wants to publish to a stream
// Responses: ACK, NACK
// ErrorCodes: ErrorCodeNotConnectedToStream
// WireFormat: [streamId][partitionId][payloadLen][payload]
type PublishMessage struct {
	StreamId
	PartitionId
	Payload []byte
}

func (m *PublishMessage) read(buf []byte) error {
	if len(buf) < 10 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	payloadLen := binary.LittleEndian.Uint32(buf[6:10])

	if len(buf[10:]) < int(payloadLen) {
		return io.ErrUnexpectedEOF
	}

	m.Payload = make([]byte, payloadLen)
	copy(m.Payload, buf[10:10+payloadLen])

	return nil
}


func (m *PublishMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := len(m.Payload)

	// Inner message payload = 2 + 4 + 4 + payloadLen
	innerLen := 2 + 4 + 4 + payloadLen
	totalLen := IdFieldSize + TypeFieldSize + innerLen // full length after len field

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len (everything after this)
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessagePublish)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// payloadLen
	binary.LittleEndian.PutUint32(buf[offset:], uint32(payloadLen))
	offset += 4

	// payload
	copy(buf[offset:], m.Payload)

	return buf
}


// Client to server
// Consumer wants to consume from a stream
// Responses: ACK, NACK
// ErrorCodes: ErrorCodeNotConnectedToStream
// WireFormat: [streamId][partitionId][offset][maxNumMessages]
type SubscribeMessage struct {
	StreamId
	PartitionId
	Offset      uint64
	MaxMessages uint16
}

func (m *SubscribeMessage) read(buf []byte) error {
	if len(buf) < 16 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	m.Offset = binary.LittleEndian.Uint64(buf[6:14])
	m.MaxMessages = binary.LittleEndian.Uint16(buf[14:16])

	return nil
}

func (m *SubscribeMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := 2 + 4 + 8 + 2 // = 16
	totalLen := IdFieldSize + TypeFieldSize + payloadLen

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageSubscribe)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// offset
	binary.LittleEndian.PutUint64(buf[offset:], m.Offset)
	offset += 8

	// maxNumMessages
	binary.LittleEndian.PutUint16(buf[offset:], m.MaxMessages)

	return buf
}


// Client to server
// Consumer wants
// Responses: ACK, NACK
// ErrorCodes: ErrorCodeNotConnectedToStream
// WireFormat: [streamId][partitionId][offset]
type CheckpointMessage struct {
	StreamId
	PartitionId
	Offset uint64
}

func (m *CheckpointMessage) read(buf []byte) error {
	if len(buf) < 14 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	m.Offset = binary.LittleEndian.Uint64(buf[6:14])

	return nil
}

func (m *CheckpointMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := 2 + 4 + 8 // 14 bytes
	totalLen := IdFieldSize + TypeFieldSize + payloadLen

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageCheckpoint)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// offset
	binary.LittleEndian.PutUint64(buf[offset:], m.Offset)

	return buf
}


// Server to client
// Server wants client to connect to another host for this partition, client should connect to that server and continue the current connection until MOVED
// Responses: ACK
// WireFormat: [streamId][partitionId][ipv4]
type MovingMessage struct {
	StreamId
	PartitionId
	Ipv4 uint32
}

func (m *MovingMessage) read(buf []byte) error {
	if len(buf) < 10 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	m.Ipv4 = binary.LittleEndian.Uint32(buf[6:10])

	return nil
}

func (m *MovingMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := 2 + 4 + 4 // 10 bytes
	totalLen := IdFieldSize + TypeFieldSize + payloadLen

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageMoving)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// ipv4
	binary.LittleEndian.PutUint32(buf[offset:], m.Ipv4)

	return buf
}


// Server to client
// Server wants client to connect to another host for this partition, there is no more data here for this partition, it will be dropped from the server
// Responses: none
// WireFormat: [streamId][partitionId][ipv4]
type MovedMessage struct {
	StreamId
	PartitionId
	Ipv4 uint32
}

func (m *MovedMessage) read(buf []byte) error {
	if len(buf) < 10 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	m.Ipv4 = binary.LittleEndian.Uint32(buf[6:10])

	return nil
}

func (m *MovedMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := 2 + 4 + 4 // 10 bytes
	totalLen := IdFieldSize + TypeFieldSize + payloadLen

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageMoving)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// ipv4
	binary.LittleEndian.PutUint32(buf[offset:], m.Ipv4)

	return buf
}

// Server to client
// Server is pushing data in response to a ConsumeMessage
// Responses: none
// ErrorCodes: ErrorCodeNotConnectedToStream
// WireFormat: [streamId][partitionId]<<[payloadLen][payload]>> (repeat)
type DataMessage struct {
	StreamId
	PartitionId
	Messages []byte
}

func (m *DataMessage) read(buf []byte) error {
	if len(buf) < 6 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))

	// The rest is the raw concatenated sub-messages
	m.Messages = make([]byte, len(buf)-6)
	copy(m.Messages, buf[6:])

	return nil
}

func (m *DataMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := len(m.Messages)

	// payload = streamId(2) + partitionId(4) + Messages(payloadLen)
	innerLen := 2 + 4 + payloadLen
	totalLen := IdFieldSize + TypeFieldSize + innerLen

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageData)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// already formatted submessages
	copy(buf[offset:], m.Messages)

	return buf
}


// Server to client
// Server is splitting the current partiton in two
// Responses: none
// WireFormat: [streamId][partitionId][newPartitionId][ipv4]
type SplittingMessage struct {
	StreamId
	PartitionId
	NewPartitionId PartitionId
	Ipv4           uint32
}

func (m *SplittingMessage) read(buf []byte) error {
	if len(buf) < 14 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))
	m.NewPartitionId = PartitionId(binary.LittleEndian.Uint32(buf[6:10]))
	m.Ipv4 = binary.LittleEndian.Uint32(buf[10:14])

	return nil
}

func (m *SplittingMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := 2 + 4 + 4 + 4 // 14 bytes
	totalLen := IdFieldSize + TypeFieldSize + payloadLen

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageSplitting)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))
	offset += 4

	// newPartitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.NewPartitionId))
	offset += 4

	// ipv4
	binary.LittleEndian.PutUint32(buf[offset:], m.Ipv4)

	return buf
}


// Server to client
// Server is closing the current partiton, client should disconnect from it
// Responses: none
// WireFormat: [streamId][partitionId]
type MergingMessage struct {
	StreamId
	PartitionId
}

func (m *MergingMessage) read(buf []byte) error {
	if len(buf) < 6 {
		return io.ErrUnexpectedEOF
	}

	m.StreamId = StreamId(binary.LittleEndian.Uint16(buf[0:2]))
	m.PartitionId = PartitionId(binary.LittleEndian.Uint32(buf[2:6]))

	return nil
}

func (m *MergingMessage) ToMessageBytes(msgId MessageId) []byte {
	payloadLen := 2 + 4 // 6 bytes
	totalLen := IdFieldSize + TypeFieldSize + payloadLen

	buf := make([]byte, LenFieldSize+totalLen)

	offset := 0

	// len
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += LenFieldSize

	// id
	binary.LittleEndian.PutUint16(buf[offset:], uint16(msgId))
	offset += IdFieldSize

	// type
	buf[offset] = uint8(MessageMerging)
	offset += TypeFieldSize

	// streamId
	binary.LittleEndian.PutUint16(buf[offset:], uint16(m.StreamId))
	offset += 2

	// partitionId
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m.PartitionId))

	return buf
}
