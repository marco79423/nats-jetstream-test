package tester

import (
	"fmt"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func NewJetStreamAsyncPublishTester(conf *config.Config) ITester {
	return &jetStreamAsyncPublishTester{
		conf: conf,
	}
}

type jetStreamAsyncPublishTester struct {
	conf *config.Config
}

func (tester *jetStreamAsyncPublishTester) Enabled() bool {
	return tester.conf.Testers.JetStreamPublishTester != nil
}

func (tester *jetStreamAsyncPublishTester) Name() string {
	return "測試 JetStream 的發布效能 (Async)"
}

func (tester *jetStreamAsyncPublishTester) Test() error {
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

	streamName := tester.conf.Testers.JetStreamPublishTester.Stream
	subject := tester.conf.Testers.JetStreamPublishTester.Subject
	times := tester.conf.Testers.JetStreamPublishTester.Times
	messageSize := tester.conf.Testers.JetStreamPublishTester.MessageSize
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

	// 測量 JetStream 發布效能
	if err := utils.MeasureJetStreamAsyncPublishMsgTime(js, subject, times, messageSize); err != nil {
		return xerrors.Errorf("測試 JetStream 的發布 (Async) 效能失敗: %w", err)
	}

	return nil
}
