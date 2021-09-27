package tester

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func NewJetStreamPerformanceTester(conf *config.Config) ITester {
	return &jetStreamPerformanceTester{
		conf: conf,
	}
}

type jetStreamPerformanceTester struct {
	conf *config.Config
}

func (tester *jetStreamPerformanceTester) Enabled() bool {
	return tester.conf.Testers.JetStreamPerformanceTester != nil
}

func (tester *jetStreamPerformanceTester) Name() string {
	return "測試 JetStream 的發布和接收的效能"
}

func (tester *jetStreamPerformanceTester) Test() error {
	natsConn, err := utils.ConnectNATS(tester.conf, "ray.jetstream.performance")
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	streamName := tester.conf.Testers.JetStreamPerformanceTester.Stream
	subject := tester.conf.Testers.JetStreamPerformanceTester.Subject
	times := tester.conf.Testers.JetStreamPerformanceTester.Times
	messageSize := tester.conf.Testers.JetStreamPerformanceTester.MessageSize
	fmt.Printf("Stream: %s\n", streamName)
	fmt.Printf("Subject: %s\n", subject)
	fmt.Printf("Times: %d\n", times)
	fmt.Printf("MessageSize: %d\n", messageSize)

	// 重建 Stream 測試用 (JetStream 需要顯示管理 Stream)
	if _, err := utils.RecreateStreamIfExists(js, &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			subject,
		},
	}); err != nil {
		return xerrors.Errorf("重建 Stream %s 失敗: %w", streamName, err)
	}

	// 測量 JetStream 發布效能
	if err := utils.MeasurePublishMsgTime(js, subject, times, messageSize); err != nil {
		return xerrors.Errorf("測試 JetStream 的發布效能失敗: %w", err)
	}

	// 測量 JetStream 訂閱效能 (Subscribe)
	if err := utils.MeasureSubscribeTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream 的接收效能失敗: %w", err)
	}

	// 測試 JetStream 訂閱效能 (Chan Subscribe)
	if err := utils.MeasureChanSubscribeTime(js, subject, times); err != nil {
		return xerrors.Errorf("測試 JetStream (Chan Subscribe) 的接收效能失敗: %w", err)
	}

	// 測試 JetStream 訂閱效能 (Pull Subscribe)
	rand.Seed(time.Now().UnixNano())
	for _, fetchCount := range []int{1, 10, 100} {
		durableName := fmt.Sprintf("ray-jetstream-performance-%d", fetchCount)
		if err := utils.MeasureChanPullSubscribeTime(js, durableName, subject, times, fetchCount); err != nil {
			return xerrors.Errorf("測試 JetStream (Pull Subscribe) 的接收效能失敗: %w", err)
		}
	}

	return nil
}
