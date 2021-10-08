package tester

import (
	"fmt"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"golang.org/x/xerrors"
)

func NewNATSPublishTester(conf *config.Config) ITester {
	return &natsPublishTester{
		conf: conf,
	}
}

type natsPublishTester struct {
	conf *config.Config
}

func (tester *natsPublishTester) Name() string {
	return "測試 NATS 的發布效能"
}

func (tester *natsPublishTester) Key() string {
	return "nats_publish_tester"
}

func (tester *natsPublishTester) Test() error {
	natsConn, err := utils.ConnectNATS(tester.conf, tester.Key())
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}
	defer natsConn.Close()

	subject := tester.conf.Testers.NATSPublishTester.Subject
	times := tester.conf.Testers.NATSPublishTester.Times
	messageSizes := tester.conf.Testers.NATSPublishTester.MessageSizes
	fmt.Printf("Subject: %s, Times: %d, MessageSizes: %v\n", subject, times, messageSizes)

	for _, messageSize := range messageSizes {
		// 測量 NATS 發布效能
		if err := utils.MeasureNATSPublishMsgTime(natsConn, subject, times, messageSize); err != nil {
			return xerrors.Errorf("測試 NATS 的發布效能失敗: %w", err)
		}
	}

	return nil
}
