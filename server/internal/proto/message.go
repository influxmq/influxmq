package proto

import "errors"

var (
	ErrUnkownMessageType = errors.New("unknown message type")
)

type Message interface {
	Unmarshal(msg []byte) error
}