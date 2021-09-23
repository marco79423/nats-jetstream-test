package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
	"golang.org/x/xerrors"
)

func TestStreamingPerformance() error {
	fmt.Println("開始測試 Streaming 的效能")

	conf, err := config.GetConfig()
	if err != nil {
		return xerrors.Errorf("取得設定檔失敗: %w", err)
	}

	// 取得 Streaming 的連線
	stanConn, err := utils.ConnectSTAN(conf, "ray.streaming.performance")
	if err != nil {
		return xerrors.Errorf("取得 STAN 連線失敗: %w", err)
	}

	rand.Seed(time.Now().UnixNano())
	channel := fmt.Sprintf("%s.%d", conf.Testers.StreamingPerformanceTest.Channel, rand.Int())
	fmt.Printf("Channel: %s\n", channel)

	// TestStreamingPublish 測試 Streaming 發布效能
	if err := TestStreamingPublish(conf, stanConn, channel); err != nil {
		return xerrors.Errorf("測試 Streaming 的發布效能失敗: %w", err)
	}

	// 測試 Streaming 訂閱效能
	if err := TestStreamingSubscribe(conf, stanConn, channel); err != nil {
		return xerrors.Errorf("測試 Streaming 的接收效能失敗: %w", err)
	}

	return nil
}

// TestStreamingPublish 測試 Streaming 發布效能
func TestStreamingPublish(conf *config.Config, stanConn stan.Conn, channel string) error {
	fmt.Println("\n開始測試 Streaming 的發布效能")
	now := time.Now()
	for i := 0; i < conf.Testers.StreamingPerformanceTest.Times; i++ {
		err := stanConn.Publish(channel, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			return xerrors.Errorf("發布 %s 失敗: %w", channel, err)
		}
		// fmt.Println(i)
	}
	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆發布花費時間 %v (每筆平均花費 %v)\n",
		conf.Testers.StreamingPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.StreamingPerformanceTest.Times),
	)
	return nil
}

// TestStreamingSubscribe 測試 Streaming 訂閱效能
func TestStreamingSubscribe(conf *config.Config, stanConn stan.Conn, channel string) error {
	fmt.Println("\n開始測試 Streaming 的接收效能")

	now := time.Now()
	quit := make(chan int)
	var counter int32 = 0
	if _, err := stanConn.Subscribe(channel, func(msg *stan.Msg) {
		// fmt.Printf("Received a Streaming message: %s\n", string(msg.Data))

		if atomic.AddInt32(&counter, 1) == int32(conf.Testers.StreamingPerformanceTest.Times) {
			quit <- 1
		}
	}, stan.StartAt(pb.StartPosition_First)); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", channel, err)
	}

	<-quit
	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		conf.Testers.StreamingPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.StreamingPerformanceTest.Times),
	)
	return nil
}

func main() {
	if err := TestStreamingPerformance(); err != nil {
		log.Fatal(err)
	}
}
