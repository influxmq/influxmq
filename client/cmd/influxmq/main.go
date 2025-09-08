package main

import (
	"encoding/binary"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)


const (
	numProducers = 25
	addr         = "localhost:9090"
	totalMsgs = 1000000
	n = 20
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

	msg := make([]byte, 0)

	buf := buildMessage(publish("stream123", jsonMessage))

	for i := 0; i < n; i++ {
		msg = append(msg, buf...)
	}

	var counter int64 
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < numProducers; i++ {
		
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatalf("Producer %d: failed to connect: %v", id, err)
			}
			defer conn.Close()

			for {
				cur := atomic.AddInt64(&counter, 1)
				if cur > totalMsgs {
					return
				}

				_, err := conn.Write(msg)
				if err != nil {
					log.Printf("Producer %d: write error: %v", id, err)
					return
				}
			}
		}(i)
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
	binary.BigEndian.PutUint32(buf, uint32(len(msg)))
	copy(buf[4:], msg)
	return buf
}