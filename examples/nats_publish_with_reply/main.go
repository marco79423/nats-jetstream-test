package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	// 連線
	natsConn, err := nats.Connect("nats://localhost:4223")
	if err != nil {
		log.Fatal("連不上 NATS")
	}
	defer natsConn.Close()

	// 自動建立一個唯一 subject
	reply := nats.NewInbox()
	sub, err := natsConn.SubscribeSync(reply)
	if err != nil {
		log.Fatal("訂閱失敗")
	}

	// 發送訊息
	err = natsConn.PublishRequest("subject", reply, []byte("Hello world"))
	if err != nil {
		log.Fatal("送不出去")
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		log.Fatal("接收失敗")
	}

	fmt.Println(string(msg.Data))
}
