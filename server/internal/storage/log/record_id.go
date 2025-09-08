package log

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"math/bits"
)

type RecordId struct {
	Hi uint64
	Lo uint64
}

// Increments the 128-bit counter by 1
func (u *RecordId) Inc() {
	lo, carry := bits.Add64(u.Lo, 1, 0)
	u.Lo = lo
	u.Hi, _ = bits.Add64(u.Hi, 0, carry)
}

// For debug/display
func (u RecordId) String() string {
	if u.Hi == 0 {
		return fmt.Sprintf("%d", u.Lo)
	}
	// Print full 128-bit value as decimal
	return u.ToBigInt().String()
}

func (u RecordId) ToBigInt() *big.Int {
	hi := new(big.Int).Lsh(new(big.Int).SetUint64(u.Hi), 64)
	lo := new(big.Int).SetUint64(u.Lo)
	return hi.Or(hi, lo)
}

func (u RecordId) Marshal() ([]byte, error) {
	buf := make([]byte, 16) // 128 bits = 16 bytes
	binary.BigEndian.PutUint64(buf[0:8], u.Hi)
	binary.BigEndian.PutUint64(buf[8:16], u.Lo)
	return buf, nil
}

func (u *RecordId) Unmarshal(data []byte) error {
	if len(data) != 16 {
		return fmt.Errorf("invalid data length for RecordId: got %d, want 16", len(data))
	}
	u.Hi = binary.BigEndian.Uint64(data[0:8])
	u.Lo = binary.BigEndian.Uint64(data[8:16])
	return nil
}
