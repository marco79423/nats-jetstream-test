package tester

import (
	"fmt"

	"github.com/marco79423/nats-jetstream-test/config"
	"golang.org/x/xerrors"
)

type ITester interface {
	Name() string
	Key() string
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
		NewStreamingLatencyTester(conf),
		NewJetStreamPublishTester(conf),
		NewJetStreamAsyncPublishTester(conf),
		NewJetStreamLatencyTester(conf),
		NewJetStreamSubscribeTester(conf),
		NewJetStreamChanSubscribeTester(conf),
		NewJetStreamPullSubscribeTester(conf),
		NewJetStreamPurgeStreamTester(conf),
		NewJetStreamMemoryStorageTester(conf),
	}

	for idx, testerKey := range conf.EnabledTesters {
		for _, tester := range testers {
			if tester.Key() == testerKey {
				fmt.Printf("======== [%d] 開始 %s ========\n", idx+1, tester.Name())
				if err := tester.Test(); err != nil {
					return xerrors.Errorf("測試 %s 失敗: %w", tester.Name(), err)
				}
				fmt.Printf("======== [%d] 結束 %s ========\n\n", idx+1, tester.Name())

				break
			}
		}
	}

	return nil
}
