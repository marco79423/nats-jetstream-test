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

func NewJetStreamPullSubscribeTester(conf *config.Config) ITester {
	return &jetStreamPullSubscribeTester{
		conf: conf,
	}
}

type jetStreamPullSubscribeTester struct {
	conf *config.Config
}

func (tester *jetStreamPullSubscribeTester) Enabled() bool {
	return tester.conf.Testers.JetStreamPullSubscribeTester != nil
}

func (tester *jetStreamPullSubscribeTester) Name() string {
	return "測試 JetStream (Pull Subscribe) 的接收效能"
}

func (tester *jetStreamPullSubscribeTester) Test() error {
	natsConn, err := utils.ConnectNATS(tester.conf, "ray.jetstream.performance")
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	streamName := tester.conf.Testers.JetStreamPullSubscribeTester.Stream
	subject := tester.conf.Testers.JetStreamPullSubscribeTester.Subject
	times := tester.conf.Testers.JetStreamPullSubscribeTester.Times
	messageSize := tester.conf.Testers.JetStreamPullSubscribeTester.MessageSize
	fetchCounts := tester.conf.Testers.JetStreamPullSubscribeTester.FetchCounts
	fmt.Printf("Stream: %s, Subject: %s, Times: %d, MessageSize: %d, fetchCounts: %v\n", streamName, subject, times, messageSize, fetchCounts)

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

	// 測量 JetStream 訂閱效能 (Pull Subscribe)
	rand.Seed(time.Now().UnixNano())
	for idx, fetchCount := range fetchCounts {
		durableName := fmt.Sprintf("ray-jetstream-performance-%d", fetchCount)
		if err := utils.MeasureJetStreamPullSubscribeTime(js, durableName, subject, times, fetchCount); err != nil {
			return xerrors.Errorf("測試 JetStream (Pull Subscribe) 的接收效能失敗: %w", err)
		}

		if idx+1 < len(fetchCounts) {
			fmt.Println()
		}
	}

	return nil
}
