package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
)

func main() {
	// 連線
	natsConn, err := nats.Connect("nats://localhost:4223")
	if err != nil {
		log.Fatal("連不上 NATS")
	}
	defer natsConn.Close()

	// 接收訊息
	_, err = natsConn.Subscribe("subject", func(msg *nats.Msg) {
		fmt.Println("收到了", string(msg.Data))
	})
	if err != nil {
		log.Fatal("訂閱失敗")
	}

	// 程式結束
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
}
