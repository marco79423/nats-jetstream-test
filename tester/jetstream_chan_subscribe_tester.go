package tester

import (
	"fmt"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func NewJetStreamChanSubscribeTester(conf *config.Config) ITester {
	return &jetStreamChanSubscribeTester{
		conf: conf,
	}
}

type jetStreamChanSubscribeTester struct {
	conf *config.Config
}


func (tester *jetStreamChanSubscribeTester) Name() string {
	return "測試 JetStream (Chan Subscribe) 的接收效能"
}

func (tester *jetStreamChanSubscribeTester) Key() string {
	return "jetstream_chan_subscribe_tester"
}

func (tester *jetStreamChanSubscribeTester) Test() error {
	natsConn, err := utils.ConnectNATS(tester.conf, tester.Key())
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}
	defer natsConn.Close()

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	streamName := tester.conf.Testers.JetStreamChanSubscribeTester.Stream
	subject := tester.conf.Testers.JetStreamChanSubscribeTester.Subject
	times := tester.conf.Testers.JetStreamChanSubscribeTester.Times
	messageSize := tester.conf.Testers.JetStreamChanSubscribeTester.MessageSize
	fmt.Printf("Stream: %s, Subject: %s, Times: %d, MessageSize: %d\n", streamName, subject, times, messageSize)

	// 重建 Stream 測試用 (JetStream 需要顯示管理 Stream)
	if _, err := utils.RecreateJetStreamStreamIfExists(js, &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			subject,
		},
	}); err != nil {
		return xerrors.Errorf("重建 Stream %s 失敗: %w", streamName, err)
	}

	// 發布大量訊息
	if err := utils.PublishJetStreamMessagesWithSize(js, subject, times, messageSize); err != nil {
		return xerrors.Errorf("發布大量訊息失敗: %w", subject, err)
	}

	// 測量 JetStream 訂閱效能 (Chan Subscribe)
	if err := utils.MeasureJetStreamChanSubscribeTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	return nil
}
