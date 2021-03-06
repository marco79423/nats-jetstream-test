package tester

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
)

func NewJetStreamPurgeStreamTester(conf *config.Config) ITester {
	return &jetStreamPurgeStreamTester{
		conf: conf,
	}
}

type jetStreamPurgeStreamTester struct {
	conf *config.Config
}

func (tester *jetStreamPurgeStreamTester) Name() string {
	return "測試 JetStream Purge Stream 的效能"
}

func (tester *jetStreamPurgeStreamTester) Key() string {
	return "jetstream_purge_stream_tester"
}

func (tester *jetStreamPurgeStreamTester) Test() error {
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

	streamName := tester.conf.Testers.JetStreamPurgeStreamTester.Stream
	subject := tester.conf.Testers.JetStreamPurgeStreamTester.Subject
	counts := tester.conf.Testers.JetStreamPurgeStreamTester.Counts
	messageSizes := tester.conf.Testers.JetStreamPurgeStreamTester.MessageSizes
	fmt.Printf("Stream: %s, Subject: %s, Counts: %d, MessageSize: %v\n", streamName, subject, counts, messageSizes)

	for _, count := range counts {
		for _, messageSize := range messageSizes {
			if err := tester.MeasurePurgeStreamTime(js, streamName, subject, count, messageSize); err != nil {
				return xerrors.Errorf("測試 Purge Stream 失敗: %w", err)
			}
		}
	}

	return nil
}

func (tester *jetStreamPurgeStreamTester) MeasurePurgeStreamTime(js nats.JetStreamContext, streamName, subject string, count, messageSize int) error {
	fmt.Printf("\n開始測量 JetStream 的 Purge Stream 效能 (次數： %d, 訊息大小：%d)\n", count, messageSize)

	// 重建 Stream
	if _, err := utils.RecreateJetStreamStreamIfExists(js, &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			subject,
		},
	}); err != nil {
		return xerrors.Errorf("測量 JetStream 發布訊息所需的時間失敗: %w", err)
	}

	// 發布足夠的訊息
	if err := utils.PublishJetStreamMessagesWithSize(js, subject, count, messageSize); err != nil {
		return xerrors.Errorf("測量 JetStream 發布訊息所需的時間失敗: %w", err)
	}

	// 清空資訊
	now := time.Now()
	if err := js.PurgeStream(streamName); err != nil {
		return xerrors.Errorf("Purge Stream 失敗: %w", err)
	}
	elapsedTime := time.Since(now)
	fmt.Printf("清除 %d 筆花費時間 %v (每筆平均花費 %v)\n",
		count,
		elapsedTime,
		elapsedTime/time.Duration(count),
	)
	return nil
}
