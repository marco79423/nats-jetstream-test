package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	// 連線
	natsConn, err := nats.Connect("nats://localhost:4223")
	if err != nil {
		log.Fatal("連不上 NATS")
	}
	defer natsConn.Close()

	// 發送訊息
	err = natsConn.Publish("subject", []byte("Hello world"))
	if err != nil {
		log.Fatal("送不出去")
	}

	// 清空緩衝
	err = natsConn.Flush()
	if err != nil {
		log.Fatal("清空失敗")
	}
}
