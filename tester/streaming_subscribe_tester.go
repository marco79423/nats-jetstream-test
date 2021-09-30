package tester

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"golang.org/x/xerrors"
)

func NewStreamingSubscribeTester(conf *config.Config) ITester {
	return &streamingSubscribeTester{
		conf: conf,
	}
}

type streamingSubscribeTester struct {
	conf *config.Config
}

func (tester *streamingSubscribeTester) Name() string {
	return "測試 Streaming 的接收的效能"
}

func (tester *streamingSubscribeTester) Key() string {
	return "streaming_subscribe_tester"
}

func (tester *streamingSubscribeTester) Test() error {
	// 取得 Streaming 的連線
	stanConn, err := utils.ConnectSTAN(tester.conf, tester.Key())
	if err != nil {
		return xerrors.Errorf("取得 STAN 連線失敗: %w", err)
	}
	defer stanConn.Close()

	rand.Seed(time.Now().UnixNano())
	channel := tester.conf.Testers.StreamingSubscribeTester.Channel
	times := tester.conf.Testers.StreamingSubscribeTester.Times
	messageSizes := tester.conf.Testers.StreamingSubscribeTester.MessageSizes
	fmt.Printf("Channel: %s, Times: %d, MessageSizes: %v\n", channel, times, messageSizes)

	for _, messageSize := range messageSizes {
		channel := fmt.Sprintf("%s.%d", channel, rand.Int())

		// 測試 Streaming 訂閱效能
		if err := utils.MeasureStreamingSubscribeTime(stanConn, channel, times, messageSize); err != nil {
			return xerrors.Errorf("測量 Streaming 的接收效能失敗: %w", err)
		}
	}

	return nil
}
