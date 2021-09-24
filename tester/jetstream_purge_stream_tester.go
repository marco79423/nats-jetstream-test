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

func (tester *jetStreamPurgeStreamTester) Enabled() bool {
	return tester.conf.Testers.JetStreamPurgeStreamTester != nil
}

func (tester *jetStreamPurgeStreamTester) Name() string {
	return "測試 JetStream Purge Stream 的效能"
}

func (tester *jetStreamPurgeStreamTester) Test() error {
	natsConn, err := utils.ConnectNATS(tester.conf, tester.Name())
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	streamName := tester.conf.Testers.JetStreamPurgeStreamTester.Stream
	subject := tester.conf.Testers.JetStreamPurgeStreamTester.Subject
	counts := tester.conf.Testers.JetStreamPurgeStreamTester.Counts
	fmt.Printf("Stream: %s\n", streamName)
	fmt.Printf("Subject: %s\n", subject)
	fmt.Printf("Counts: %v\n", counts)

	for _, count := range counts {
		if err := tester.MeasurePurgeStreamTime(js, streamName, subject, count); err != nil {
			return xerrors.Errorf("測試 Purge Stream 失敗: %w", err)
		}
	}

	return nil
}

func (tester *jetStreamPurgeStreamTester) MeasurePurgeStreamTime(js nats.JetStreamContext, streamName, subject string, count int) error {
	fmt.Printf("\n開始測試 JetStream 的 Purge Stream (%d 筆) 的效能\n", count)

	// 重建 Stream
	if _, err := utils.RecreateStreamIfExists(js, &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			subject,
		},
	}); err != nil {
		return xerrors.Errorf("測量 JetStream 發布訊息所需的時間失敗: %w", err)
	}

	// 發布足夠的訊息
	if err := utils.PublishMassiveMessages(js, subject, count); err != nil {
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
