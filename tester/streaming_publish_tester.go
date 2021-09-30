package tester

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"golang.org/x/xerrors"
)

func NewStreamingPublishTester(conf *config.Config) ITester {
	return &streamingPublishTester{
		conf: conf,
	}
}

type streamingPublishTester struct {
	conf *config.Config
}

func (tester *streamingPublishTester) Name() string {
	return "測試 Streaming 的發布效能"
}

func (tester *streamingPublishTester) Key() string {
	return "streaming_publish_tester"
}

func (tester *streamingPublishTester) Test() error {
	// 取得 Streaming 的連線
	stanConn, err := utils.ConnectSTAN(tester.conf, tester.Key())
	if err != nil {
		return xerrors.Errorf("取得 STAN 連線失敗: %w", err)
	}
	defer stanConn.Close()

	rand.Seed(time.Now().UnixNano())
	channel := fmt.Sprintf("%s.%d", tester.conf.Testers.StreamingPublishTester.Channel, rand.Int())
	times := tester.conf.Testers.StreamingPublishTester.Times
	messageSize := tester.conf.Testers.StreamingPublishTester.MessageSize
	fmt.Printf("Channel: %s, Times: %d, MessageSize: %d\n", channel, times, messageSize)

	// 測試 Streaming 發布效能
	if err := utils.MeasureStreamingPublishTime(stanConn, channel, times, messageSize); err != nil {
		return xerrors.Errorf("測量 Streaming 的發布效能失敗: %w", err)
	}

	return nil
}
