package memory

type Pool interface {
	Get() []byte
	Return(mem []byte)
}