package tester

import (
	"fmt"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func NewJetStreamPublishTester(conf *config.Config) ITester {
	return &jetStreamPublishTester{
		conf: conf,
	}
}

type jetStreamPublishTester struct {
	conf *config.Config
}

func (tester *jetStreamPublishTester) Name() string {
	return "測試 JetStream 的發布效能"
}

func (tester *jetStreamPublishTester) Key() string {
	return "jetstream_publish_tester"
}

func (tester *jetStreamPublishTester) Test() error {
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

	streamName := tester.conf.Testers.JetStreamPublishTester.Stream
	subject := tester.conf.Testers.JetStreamPublishTester.Subject
	times := tester.conf.Testers.JetStreamPublishTester.Times
	messageSizes := tester.conf.Testers.JetStreamPublishTester.MessageSizes
	fmt.Printf("Stream: %s, Subject: %s, Times: %d, MessageSizes: %v\n", streamName, subject, times, messageSizes)

	for _, messageSize := range messageSizes {
		// 重建 Stream 測試用 (JetStream 需要顯示管理 Stream)
		if _, err := utils.RecreateJetStreamStreamIfExists(js, &nats.StreamConfig{
			Name: streamName,
			Subjects: []string{
				subject,
			},
		}); err != nil {
			return xerrors.Errorf("重建 Stream %s 失敗: %w", streamName, err)
		}

		// 測量 JetStream 發布效能
		if err := utils.MeasureJetStreamPublishMsgTime(js, subject, times, messageSize); err != nil {
			return xerrors.Errorf("測試 JetStream 的發布效能失敗: %w", err)
		}
	}

	return nil
}
