package tester

import (
	"fmt"

	"github.com/marco79423/nats-jetstream-test/config"
	"golang.org/x/xerrors"
)

type ITester interface {
	Enabled() bool
	Name() string
	Test() error
}

func RunTesters() error {
	conf, err := config.GetConfig()
	if err != nil {
		return xerrors.Errorf("取得設定檔失敗: %w", err)
	}

	testers := []ITester{
		NewStreamingPublishTester(conf),
		NewStreamingSubscribeTester(conf),
		NewJetStreamPublishTester(conf),
		NewJetStreamAsyncPublishTester(conf),
		NewJetStreamSubscribeTester(conf),
		NewJetStreamChanSubscribeTester(conf),
		NewJetStreamPullSubscribeTester(conf),
		NewJetStreamPurgeStreamTester(conf),
		NewJetStreamMemoryStorageTester(conf),
	}

	for _, tester := range testers {
		if !tester.Enabled() {
			continue
		}

		fmt.Printf("======== 開始 %s ========\n", tester.Name())
		if err := tester.Test(); err != nil {
			return xerrors.Errorf("測試 %s 失敗: %w", tester.Name(), err)
		}
		fmt.Printf("======== 結束 %s ========\n\n", tester.Name())
	}

	return nil
}
