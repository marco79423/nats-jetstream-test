package tester

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
)

func NewJetStreamMemoryStorageTester(conf *config.Config) ITester {
	return &jetStreamMemoryStorageTester{
		conf: conf,
	}
}

type jetStreamMemoryStorageTester struct {
	conf *config.Config
}

func (tester *jetStreamMemoryStorageTester) Enabled() bool {
	return tester.conf.Testers.JetStreamMemoryStorageTester != nil
}

func (tester *jetStreamMemoryStorageTester) Name() string {
	return "測試 JetStream Memory Storage 的效能"
}

func (tester *jetStreamMemoryStorageTester) Test() error {
	natsConn, err := utils.ConnectNATS(tester.conf, "ray.jetstream.memory_storage")
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	// JetStream 需要顯示管理 Stream
	streamName := tester.conf.Testers.JetStreamMemoryStorageTester.Stream
	subject := tester.conf.Testers.JetStreamMemoryStorageTester.Subject
	times := tester.conf.Testers.JetStreamMemoryStorageTester.Times
	fmt.Printf("Stream: %s\n", streamName)
	fmt.Printf("Subject: %s\n", subject)
	fmt.Printf("Times: %d\n", times)

	if err := tester.TestJetStreamMemoryStoragePerformance(js, streamName, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream MemoryStorage 的效能: %w", err)
	}

	if err := tester.TestJetStreamFileStoragePerformance(js, streamName, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream FileStorage 的效能: %w", err)
	}

	return nil
}

func (tester *jetStreamMemoryStorageTester) TestJetStreamFileStoragePerformance(js nats.JetStreamContext, streamName, subject string, times int) error {
	fmt.Println("\n開始測試 JetStream FileStorage 的效能")

	if _, err := utils.RecreateStreamIfExists(js, &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			subject,
		},
		Storage: nats.FileStorage, // 預設
	}); err != nil {
		return xerrors.Errorf("建立 Stream %s 失敗: %w", streamName, err)
	}

	// 測量 JetStream 發布效能
	if err := utils.MeasurePublishMsgTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream 的發布效能失敗: %w", err)
	}

	// 測量 JetStream 訂閱效能 (Subscribe)
	if err := utils.MeasureSubscribeTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	return nil
}

func (tester *jetStreamMemoryStorageTester) TestJetStreamMemoryStoragePerformance(js nats.JetStreamContext, streamName, subject string, times int) error {
	fmt.Println("\n開始測試 JetStream MemoryStorage 的效能")

	if _, err := utils.RecreateStreamIfExists(js, &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			subject,
		},
		Storage: nats.MemoryStorage,
	}); err != nil {
		return xerrors.Errorf("建立 Stream %s 失敗: %w", streamName, err)
	}

	// 測量 JetStream 發布效能
	if err := utils.MeasurePublishMsgTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream 的發布效能失敗: %w", err)
	}

	// 測量 JetStream 訂閱效能 (Subscribe)
	if err := utils.MeasureSubscribeTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	return nil
}
