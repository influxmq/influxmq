package proto

type ErrorCode uint16

const (
	_ ErrorCode = iota
	ErrorCodeUnknownStream
	ErrorCodeNotConnectedToStream
)