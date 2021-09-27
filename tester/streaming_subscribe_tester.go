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
	stanConn, err := utils.ConnectSTAN(tester.conf, tester.Name())
	if err != nil {
		return xerrors.Errorf("取得 STAN 連線失敗: %w", err)
	}
	defer stanConn.Close()

	rand.Seed(time.Now().UnixNano())
	channel := fmt.Sprintf("%s.%d", tester.conf.Testers.StreamingSubscribeTester.Channel, rand.Int())
	times := tester.conf.Testers.StreamingSubscribeTester.Times
	messageSize := tester.conf.Testers.StreamingSubscribeTester.MessageSize
	fmt.Printf("Channel: %s, Times: %d, MessageSize: %d\n", channel, times, messageSize)

	// 發布大量訊息
	if err := utils.PublishStreamingMessagesWithSize(stanConn, channel, times, messageSize); err != nil {
		return xerrors.Errorf("發布大量訊息失敗: %w", err)
	}

	// 測試 Streaming 訂閱效能
	if err := utils.MeasureStreamingSubscribeTime(stanConn, channel, times); err != nil {
		return xerrors.Errorf("測量 Streaming 的接收效能失敗: %w", err)
	}

	return nil
}
