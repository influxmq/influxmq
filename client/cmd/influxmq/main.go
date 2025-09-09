package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/bits"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	numProducers = 1
	addr         = "localhost:9090"
	totalMsgs    = 20
	n            = 5 // messages per batch
)


var jsonMessage = `{
  "id": 123,
  "name": "Alice Johnson",
  "email": "alice@example.com",
  "is_active": true,
  "roles": ["admin", "editor"],
  "profile": {
    "age": 29,
    "location": "New York",
    "interests": ["reading", "traveling", "coding"]
  }
}`

func main() {
	var counter int64
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numProducers; i++ {
		producerID := i

		// Build a single framed message
		singleMessage := buildMessage(publish(fmt.Sprintf("%d", producerID), jsonMessage))

		// Create a batch of n messages
		msg := make([]byte, 0, len(singleMessage)*n)
		for j := 0; j < n; j++ {
			msg = append(msg, singleMessage...)
		}

		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatalf("Producer %d: failed to connect: %v", id, err)
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			replyBuf := make([]byte, 16) // fixed 4-byte reply

			for {
				cur := atomic.AddInt64(&counter, 1)
				if cur > totalMsgs {
					return
				}

				// Send the full batch
				_, err := conn.Write(msg)
				if err != nil {
					log.Printf("Producer %d: write error: %v", id, err)
					return
				}

				// Read n replies (one per message)
				for x := 0; x < n; x++ {
					_, err := io.ReadFull(reader, replyBuf)
					if err != nil {
						log.Printf("Producer %d: read error: %v", id, err)
						return
					}
					
					reply := UInt128{}
					reply.Unmarshal(replyBuf)
					
					//fmt.Printf("Producer %d: received reply %s (msg %d)\n", id, reply.String(), x)
				}
			}
		}(producerID)
	}

	wg.Wait()
	duration := time.Since(start)
	throughput := float64(totalMsgs*n) / duration.Seconds()
	log.Printf("Sent %d messages in %.2fs (%.2f msgs/sec)", totalMsgs*n, duration.Seconds(), throughput)
}

func publish(stream, payload string) []byte {
	const msgType byte = 0x01

	streamBytes := []byte(stream)
	messageBytes := []byte(payload)

	streamLen := len(streamBytes)
	messageLen := len(messageBytes)

	buf := make([]byte, 1+2+streamLen+2+messageLen)

	// Write message type
	buf[0] = msgType

	// Write stream length (2 bytes, big-endian)
	binary.BigEndian.PutUint16(buf[1:3], uint16(streamLen))

	// Write stream
	copy(buf[3:3+streamLen], streamBytes)

	// Write payload length (2 bytes, big-endian)
	start := 3 + streamLen
	binary.BigEndian.PutUint16(buf[start:start+2], uint16(messageLen))

	// Write payload
	copy(buf[start+2:], messageBytes)

	return buf
}

func buildMessage(msg []byte) []byte {
	buf := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(msg)))
	copy(buf[4:], msg)
	return buf
}



//todo: move later
type UInt128 struct {
	Hi uint64
	Lo uint64
}

// Increments the 128-bit counter by 1
func (u *UInt128) Inc() {
	lo, carry := bits.Add64(u.Lo, 1, 0)
	u.Lo = lo
	u.Hi, _ = bits.Add64(u.Hi, 0, carry)
}

// For debug/display
func (u UInt128) String() string {
	if u.Hi == 0 {
		return fmt.Sprintf("%d", u.Lo)
	}
	// Print full 128-bit value as decimal
	return u.ToBigInt().String()
}

func (u UInt128) ToBigInt() *big.Int {
	hi := new(big.Int).Lsh(new(big.Int).SetUint64(u.Hi), 64)
	lo := new(big.Int).SetUint64(u.Lo)
	return hi.Or(hi, lo)
}

func (u UInt128) Marshal() ([]byte, error) {
	buf := make([]byte, 16) // 128 bits = 16 bytes
	binary.BigEndian.PutUint64(buf[0:8], u.Hi)
	binary.BigEndian.PutUint64(buf[8:16], u.Lo)
	return buf, nil
}

func (u *UInt128) Unmarshal(data []byte) error {
	if len(data) != 16 {
		return fmt.Errorf("invalid data length for UInt128: got %d, want 16", len(data))
	}
	u.Hi = binary.BigEndian.Uint64(data[0:8])
	u.Lo = binary.BigEndian.Uint64(data[8:16])
	return nil
}
