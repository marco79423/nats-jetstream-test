package main

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func TestJetStreamPerformance() error {
	fmt.Println("開始測試 JetStream 的效能")

	conf, err := config.GetConfig()
	if err != nil {
		return xerrors.Errorf("取得設定檔失敗: %w", err)
	}

	natsConn, err := nats.Connect(
		strings.Join(conf.NATSJetStream.Servers, ","),
		nats.Name("ray.jetstream.performance"),
		nats.Token(conf.NATSJetStream.Token),
	)
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	// JetStream 需要顯示管理 Stream
	streamName := conf.Testers.JetStreamPerformanceTest.Stream

	// 可透過 StreamInfo 取得 Stream 相關的資訊
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		return xerrors.Errorf("無法取得 JetStream 的 Stream %s: %w", streamName, err)
	}

	// 先把之前的 Stream 刪掉 (正常不用這麼做，這是為了測試用)
	// 雖然有 PurgeStream，但效果好像不太好
	if stream != nil {
		if err := js.DeleteStream(streamName); err != nil {
			return xerrors.Errorf("刪除 Stream %s 失敗: %w", streamName, err)
		}
	}

	// 重新建立一個 Stream
	if stream, err = js.AddStream(&nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			conf.Testers.JetStreamPerformanceTest.Subject, // 例子是用 ray.fuck，但也可以設定 ray.*
		},
	}); err != nil {
		return xerrors.Errorf("建立 Stream %s 失敗: %w", streamName, err)
	}

	// 測試 JetStream 發布效能
	if err := TestJetStreamPublish(conf, js); err != nil {
		return xerrors.Errorf("測試 JetStream 的發布效能失敗: %w", err)
	}

	// 測試 JetStream 訂閱效能 (Subscribe)
	if err := TestJetStreamSubscribe(conf, js); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	// 測試 JetStream 訂閱效能 (Chan Subscribe)
	if err := TestJetStreamChanSubscribe(conf, js); err != nil {
		return xerrors.Errorf("測試 JetStream (Chan Subscribe) 的接收效能失敗: %w", err)
	}

	// 測試 JetStream 訂閱效能 (Pull Subscribe)
	if err := TestJetStreamPullSubscribe(conf, 1, js); err != nil {
		return xerrors.Errorf("測試 JetStream (Pull Subscribe) 的接收效能失敗: %w", err)
	}
	if err := TestJetStreamPullSubscribe(conf, 10, js); err != nil {
		return xerrors.Errorf("測試 JetStream (Pull Subscribe) 的接收效能失敗: %w", err)
	}
	if err := TestJetStreamPullSubscribe(conf, 100, js); err != nil {
		return xerrors.Errorf("測試 JetStream (Pull Subscribe) 的接收效能失敗: %w", err)
	}

	return nil
}

// TestJetStreamPublish 測試 JetStream 發布效能
func TestJetStreamPublish(conf *config.Config, jetStreamCtx nats.JetStreamContext) error {
	fmt.Println("開始測試 JetStream 的發布效能")
	now := time.Now()
	for i := 0; i < conf.Testers.JetStreamPerformanceTest.Times; i++ {
		_, err := jetStreamCtx.Publish(conf.Testers.JetStreamPerformanceTest.Subject, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			return xerrors.Errorf("發布 %s 失敗: %w", conf.Testers.JetStreamPerformanceTest.Subject, err)
		}
		// fmt.Println(i)
	}
	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆發布花費時間 %v (每筆平均花費 %v)\n",
		conf.Testers.JetStreamPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.JetStreamPerformanceTest.Times),
	)
	return nil
}

// TestJetStreamSubscribe 測試 JetStream 訂閱效能 (Subscribe)
func TestJetStreamSubscribe(conf *config.Config, jetStreamCtx nats.JetStreamContext) error {
	fmt.Println("開始測試 JetStream 的接收效能")

	now := time.Now()
	quit := make(chan int)
	var counter int32 = 0
	if _, err := jetStreamCtx.Subscribe(conf.Testers.JetStreamPerformanceTest.Subject, func(msg *nats.Msg) {
		// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))

		if atomic.AddInt32(&counter, 1) == int32(conf.Testers.JetStreamPerformanceTest.Times) {
			quit <- 1
		}
	}); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", conf.Testers.JetStreamPerformanceTest.Subject, err)
	}

	<-quit
	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		conf.Testers.JetStreamPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.JetStreamPerformanceTest.Times),
	)
	return nil
}

// TestJetStreamChanSubscribe 測試 JetStream 訂閱效能 (Chan Subscribe)
func TestJetStreamChanSubscribe(conf *config.Config, jetStreamCtx nats.JetStreamContext) error {
	fmt.Println("開始測試 JetStream (Chan Subscribe) 的接收效能")

	now := time.Now()
	msgChan := make(chan *nats.Msg, 10000)
	if _, err := jetStreamCtx.ChanSubscribe(conf.Testers.JetStreamPerformanceTest.Subject, msgChan); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", conf.Testers.JetStreamPerformanceTest.Subject, err)
	}

	var counter int32 = 0
	for msg := range msgChan {
		_ = msg
		// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))

		if atomic.AddInt32(&counter, 1) == int32(conf.Testers.JetStreamPerformanceTest.Times) {
			break
		}
	}

	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		conf.Testers.JetStreamPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.JetStreamPerformanceTest.Times),
	)
	return nil
}

// TestJetStreamPullSubscribe 測試 JetStream 訂閱效能 (Pull Subscribe)
func TestJetStreamPullSubscribe(conf *config.Config, fetchCount int, jetStreamCtx nats.JetStreamContext) error {
	fmt.Println("開始測試 JetStream (Pull Subscribe) 的接收效能")

	now := time.Now()

	sub, err := jetStreamCtx.PullSubscribe(conf.Testers.JetStreamPerformanceTest.Subject, "ray-jetstream-performance")
	if err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", conf.Testers.JetStreamPerformanceTest.Subject, err)
	}

	var counter = 0
	for counter < conf.Testers.JetStreamPerformanceTest.Times {
		msgs, _ := sub.Fetch(fetchCount)  // 不同數量也會有區別

		for _, msg := range msgs {
			_ = msg
			// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))
			counter++
		}
	}

	elapsedTime := time.Since(now)
	fmt.Printf("一次抓 %d 筆，全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		fetchCount,
		conf.Testers.JetStreamPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.JetStreamPerformanceTest.Times),
	)
	return nil
}

func main() {
	if err := TestJetStreamPerformance(); err != nil {
		log.Fatal(err)
	}
}
