package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

func main() {
	// 連線
	stanConn, err := stan.Connect(
		"test-cluster", // Cluster ID
		"clientID",     // 客戶端自設的 Client ID
		stan.NatsURL("nats://localhost:4223"),
		stan.NatsOptions(
			nats.Name("NATS 連線名稱"),
		),
	)
	if err != nil {
		log.Fatal("連不上 STAN")
	}
	defer stanConn.Close()

	// 接收訊息
	opts := []stan.SubscriptionOption {
		stan.SetManualAckMode(), // 手動 Ack 模式
		stan.StartAtSequence(1),
	}
	_, err = stanConn.QueueSubscribe("channel", "a", func(msg *stan.Msg) {
		fmt.Println("收到了", msg.Sequence, string(msg.Data))
		msg.Ack()
	}, opts...)
	if err != nil {
		log.Fatal("訂閱失敗", err)
	}

	// 程式結束
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
}
