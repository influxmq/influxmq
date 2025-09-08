package main

import (
	// "net/http"
	// _ "net/http/pprof"

	"github.com/influxmq/influxmq/server"
)

func main() {

	s, err := server.NewServer("./data"); if err != nil {
		panic(err)
	}

	// go func() {
	// 	err := http.ListenAndServe("localhost:6060", nil)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()

	err = s.ListenAndServe(); if err != nil {
		panic(err)
	}
}