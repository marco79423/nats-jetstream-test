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

	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	// 需要顯示管理 JetStream 的 Stream
	streamName := conf.Testers.JetStreamPerformanceTest.Stream

	stream, err := js.StreamInfo(streamName)
	if err != nil {
		return xerrors.Errorf("無法取得 JetStream 的 Stream %s: %w", streamName, err)
	}

	// 先把之前的 Stream 刪掉 (雖然有 PurgeStream，但效果好像不太好)
	if stream != nil {
		if err := js.DeleteStream(streamName); err != nil {
			return xerrors.Errorf("刪除 Stream %s 失敗: %w", streamName, err)
		}
	}

	if stream, err = js.AddStream(&nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			conf.Testers.JetStreamPerformanceTest.Subject, // 例子是用 ray.fuck，但也可以設定 ray.*
		},
	}); err != nil {
		return xerrors.Errorf("建立 Stream %s 失敗: %w", streamName, err)
	}

	if err := TestJetStreamPublish(conf, js); err != nil {
		return xerrors.Errorf("測試 JetStream 的發布效能失敗: %w", err)
	}

	if err := TestJetStreamSubscribe(conf, js); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	if err := TestJetStreamChanSubscribe(conf, js); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	return nil
}

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
	fmt.Printf("%d 筆發布花費時間 %v (每筆平均花費 %v)\n",
		conf.Testers.JetStreamPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.JetStreamPerformanceTest.Times),
	)
	return nil
}

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
	fmt.Printf("%d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		conf.Testers.JetStreamPerformanceTest.Times,
		elapsedTime,
		elapsedTime/time.Duration(conf.Testers.JetStreamPerformanceTest.Times),
	)
	return nil
}

func TestJetStreamChanSubscribe(conf *config.Config, jetStreamCtx nats.JetStreamContext) error {
	fmt.Println("開始測試 JetStream (Chan Subscribe) 的接收效能")

	now := time.Now()
	var counter int32 = 0
	msgChan := make(chan *nats.Msg, 10000)
	if _, err := jetStreamCtx.ChanSubscribe(conf.Testers.JetStreamPerformanceTest.Subject, msgChan); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", conf.Testers.JetStreamPerformanceTest.Subject, err)
	}

	for msg := range msgChan {
		_ = msg
		// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))

		if atomic.AddInt32(&counter, 1) == int32(conf.Testers.JetStreamPerformanceTest.Times) {
			break
		}
	}

	elapsedTime := time.Since(now)
	fmt.Printf("%d 筆接收花費時間 %v (每筆平均花費 %v)\n",
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
