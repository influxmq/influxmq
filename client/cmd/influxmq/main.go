package main

import (
	"net"
	"time"

	"github.com/influxmq/influxmq/server/proto"
)

func main() {
	
	connectMsg := proto.ConnectMessage{
		Stream: "stream123",
		StreamId: 1234,
		PartitionId: 2,
	}

	conn, err := net.Dial("tcp", "localhost:9090"); if err != nil {
		panic(err)
	}

	_, err = conn.Write(connectMsg.ToMessageBytes(proto.MessageId(22))); if err != nil {
		panic(err)
	}


	b := connectMsg.ToMessageBytes(proto.MessageId(22))



	_, err = conn.Write(b[0:13]); if err != nil {
		panic(err)
	}

	time.Sleep(time.Second)

	_, err = conn.Write(b[13:]); if err != nil {
		panic(err)
	}
}

