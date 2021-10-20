package main

import (
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

func main() {
	// 連線
	stanConn, err := stan.Connect(
		"test-cluster", // Cluster ID
		"clientID2",     // 客戶端自設的 Client ID
		stan.NatsURL("nats://localhost:4223"),
		stan.NatsOptions(
			nats.Name("NATS 連線名稱"),
		),
	)
	if err != nil {
		log.Fatal("連不上 STAN")
	}
	defer stanConn.Close()

	// 發送訊息
	err = stanConn.Publish("channel", []byte("Hello world"))
	if err != nil {
		log.Fatal("送不出去")
	}
}
