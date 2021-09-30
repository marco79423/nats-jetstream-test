package tester

import (
	"fmt"
	"sync"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func NewJetStreamLatencyTester(conf *config.Config) ITester {
	return &jetStreamLatencyTester{
		conf: conf,
	}
}

type jetStreamLatencyTester struct {
	conf *config.Config
}

func (tester *jetStreamLatencyTester) Name() string {
	return "測試 JetStream 的延遲"
}

func (tester *jetStreamLatencyTester) Key() string {
	return "jetstream_latency_tester"
}

func (tester *jetStreamLatencyTester) Test() error {
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

	streamName := tester.conf.Testers.JetStreamLatencyTester.Stream
	subject := tester.conf.Testers.JetStreamLatencyTester.Subject
	times := tester.conf.Testers.JetStreamLatencyTester.Times
	fmt.Printf("Stream: %s, Subject: %s, Times: %d\n", streamName, subject, times)

	// 重建 Stream 測試用 (JetStream 需要顯示管理 Stream)
	if _, err := utils.RecreateJetStreamStreamIfExists(js, &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			subject,
		},
	}); err != nil {
		return xerrors.Errorf("重建 Stream %s 失敗: %w", streamName, err)
	}

	fmt.Println("開始測量 JetStream 的延遲")

	wg := sync.WaitGroup{}
	wg.Add(times)

	var elapsedTimeList []time.Duration
	if _, err := js.Subscribe(subject, func(msg *nats.Msg) {
		startTime, err := time.Parse(time.RFC3339Nano, string(msg.Data))
		if err != nil {
			fmt.Printf("%+v", xerrors.Errorf("解析訊息失敗: %w", err))
		}
		elapsedTimeList = append(elapsedTimeList, time.Since(startTime))
		wg.Done()
	}); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", subject, err)
	}

	for i := 0; i < times; i++ {
		message := fmt.Sprintf("%s", time.Now().Format(time.RFC3339Nano))
		if _, err := js.Publish(subject, []byte(message)); err != nil {
			return xerrors.Errorf("發布訊息失敗: %w", err)
		}
	}

	wg.Wait()

	var totalElapsedTime time.Duration
	var maxElapsedTime time.Duration
	var minElapsedTime time.Duration

	for _, elapsedTime := range elapsedTimeList {
		totalElapsedTime += elapsedTime

		if maxElapsedTime < elapsedTime {
			maxElapsedTime = elapsedTime
		}

		if minElapsedTime == 0 || minElapsedTime > elapsedTime {
			minElapsedTime = elapsedTime
		}
	}

	fmt.Printf("全部 %d 筆訊息平均延遲 %v (最大延遲： %v, 最小延遲： %v)\n",
		times,
		totalElapsedTime/time.Duration(times),
		maxElapsedTime,
		minElapsedTime,
	)

	return nil
}
