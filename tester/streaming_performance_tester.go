package tester

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
	"golang.org/x/xerrors"
)

func NewStreamingPerformanceTester(conf *config.Config) ITester {
	return &streamingPerformanceTester{
		conf: conf,
	}
}

type streamingPerformanceTester struct {
	conf *config.Config
}

func (tester *streamingPerformanceTester) Enabled() bool {
	return tester.conf.Testers.StreamingPerformanceTester != nil
}

func (tester *streamingPerformanceTester) Name() string {
	return "測試 Streaming 的發布和接收的效能"
}

func (tester *streamingPerformanceTester) Test() error {
	// 取得 Streaming 的連線
	stanConn, err := utils.ConnectSTAN(tester.conf, tester.Name())
	if err != nil {
		return xerrors.Errorf("取得 STAN 連線失敗: %w", err)
	}

	rand.Seed(time.Now().UnixNano())
	channel := fmt.Sprintf("%s.%d", tester.conf.Testers.StreamingPerformanceTester.Channel, rand.Int())
	times := tester.conf.Testers.StreamingPerformanceTester.Times
	messageSize := tester.conf.Testers.StreamingPerformanceTester.MessageSize
	fmt.Printf("Channel: %s\n", channel)
	fmt.Printf("Times: %d\n", times)
	fmt.Printf("MessageSize: %d\n", messageSize)


	// 測試 Streaming 發布效能
	if err := tester.TestStreamingPublish(stanConn, channel, times, messageSize); err != nil {
		return xerrors.Errorf("測試 Streaming 的發布效能失敗: %w", err)
	}

	// 測試 Streaming 訂閱效能
	if err := tester.TestStreamingSubscribe(stanConn, channel, times); err != nil {
		return xerrors.Errorf("測試 Streaming 的接收效能失敗: %w", err)
	}

	return nil
}

// TestStreamingPublish 測試 Streaming 發布效能
func (tester *streamingPerformanceTester) TestStreamingPublish(stanConn stan.Conn, channel string, times, messageSize int) error {
	fmt.Println("\n開始測試 Streaming 的發布效能")

	message := utils.GenerateRandomString(messageSize)
	now := time.Now()
	for i := 0; i < times; i++ {
		err := stanConn.Publish(channel, []byte(message))
		if err != nil {
			return xerrors.Errorf("發布 %s 失敗: %w", channel, err)
		}
		// fmt.Println(i)
	}
	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆發布花費時間 %v (訊息大小： %v, 每筆平均花費 %v)\n",
		times,
		elapsedTime,
		messageSize,
		elapsedTime/time.Duration(times),
	)
	return nil
}

// TestStreamingSubscribe 測試 Streaming 訂閱效能
func (tester *streamingPerformanceTester) TestStreamingSubscribe(stanConn stan.Conn, channel string, times int) error {
	fmt.Println("\n開始測試 Streaming 的接收效能")

	now := time.Now()
	quit := make(chan int)

	if _, err := stanConn.Subscribe(channel, func(msg *stan.Msg) {
		// fmt.Printf("Received a Streaming message: %s\n", string(msg.Data))
		quit <- 1
	}, stan.StartAt(pb.StartPosition_First)); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", channel, err)
	}

	for i := 0; i < times; i++ {
		<-quit
	}

	<-quit
	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		times,
		elapsedTime,
		elapsedTime/time.Duration(times),
	)
	return nil
}
