# InfluxMQ

InfluxMQ is a lightweight, high-throughput **pub/sub message broker** designed to support **many independent topics** at scale, with **simple deployments** and tunable **write durability**.

### Project Goals

- **Isolated Topics**  
  Topics should not block or interfere with each other even under high load. Topics are lightweight and be created and deleted as needed.

- **Lightweight and Scalable**  
  Topics should be cheap to create, live indefinitely, and scale automatically when throughput increases.

- **Simple Deployments**  
  One binary hosts the entire service.

- **Quorum-Based Durability**  
  Support write quorum to ensure data is safely replicated across nodes.

- **Supports Queue and Stream Semantics**  
  Fit for both load-balanced consumers and fan-out pub/sub use cases.

- **Auto sharding for hot topics**  
  Topics can be set to automatically shard when the load increases.

### Components

1. **influxmqd** - Server process
1. **influxmq** - Golang client SDK and cli wrapper
1. **influxmq-ctl** - Golang control plane client SDK and cli wrapper

### Development

1. Build server `go run ./server/cmd/influxmqd`
1. Build client `go run ./client/cmd/influxmq`
1. Build ctl `go run ./ctl/cmd/influxmq-ctl`
1. Run server tests
   1. Unit: `go test ./server/internal/...`
   1. Integration: `go test -count=1 -tags=integration ./server/internal/...`
1. Run client tests
   1. Unit: `go test ./client/internal/...`
   1. Integration: `go test -count=1 -tags=integration ./client/internal/...`
1. Run ctl tests
   1. Unit: `go test ./ctl/internal/...`
   1. Integration: `go test -count=1 -tags=integration ./ctl/internal/...`
