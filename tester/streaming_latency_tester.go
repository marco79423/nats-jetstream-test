package tester

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/stan.go"
	"golang.org/x/xerrors"
)

func NewStreamingLatencyTester(conf *config.Config) ITester {
	return &streamingLatencyTester{
		conf: conf,
	}
}

type streamingLatencyTester struct {
	conf *config.Config
}

func (tester *streamingLatencyTester) Name() string {
	return "測試 Streaming 的延遲"
}

func (tester *streamingLatencyTester) Key() string {
	return "streaming_latency_tester"
}

func (tester *streamingLatencyTester) Test() error {
	stanConn, err := utils.ConnectSTAN(tester.conf, tester.Key())
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}
	defer stanConn.Close()

	rand.Seed(time.Now().UnixNano())
	channel := fmt.Sprintf("%s.%d", tester.conf.Testers.StreamingLatencyTester.Channel, rand.Int())
	times := tester.conf.Testers.StreamingLatencyTester.Times
	fmt.Printf("Channel: %s, Times: %d\n", channel, times)

	fmt.Println("開始測量 Streaming 的延遲")

	wg := sync.WaitGroup{}
	wg.Add(times)

	var elapsedTimeList []time.Duration
	if _, err := stanConn.Subscribe(channel, func(msg *stan.Msg) {
		startTime, err := time.Parse(time.RFC3339Nano, string(msg.Data))
		if err != nil {
			fmt.Printf("%+v", xerrors.Errorf("解析訊息失敗: %w", err))
		}
		elapsedTimeList = append(elapsedTimeList, time.Since(startTime))
		wg.Done()
	}); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", channel, err)
	}

	for i := 0; i < times; i++ {
		message := fmt.Sprintf("%s", time.Now().Format(time.RFC3339Nano))
		if err := stanConn.Publish(channel, []byte(message)); err != nil {
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
