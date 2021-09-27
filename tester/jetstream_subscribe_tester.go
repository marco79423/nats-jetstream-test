package tester

import (
	"fmt"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func NewJetStreamSubscribeTester(conf *config.Config) ITester {
	return &jetStreamSubscribeTester{
		conf: conf,
	}
}

type jetStreamSubscribeTester struct {
	conf *config.Config
}

func (tester *jetStreamSubscribeTester) Enabled() bool {
	return tester.conf.Testers.JetStreamSubscribeTester != nil
}

func (tester *jetStreamSubscribeTester) Name() string {
	return "測試 JetStream (Subscribe) 的接收效能"
}

func (tester *jetStreamSubscribeTester) Test() error {
	natsConn, err := utils.ConnectNATS(tester.conf, "ray.jetstream.performance")
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}
	defer natsConn.Close()

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	streamName := tester.conf.Testers.JetStreamSubscribeTester.Stream
	subject := tester.conf.Testers.JetStreamSubscribeTester.Subject
	times := tester.conf.Testers.JetStreamSubscribeTester.Times
	messageSize := tester.conf.Testers.JetStreamSubscribeTester.MessageSize
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

	// 測量 JetStream 訂閱效能 (Subscribe)
	if err := utils.MeasureJetStreamSubscribeTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	return nil
}
