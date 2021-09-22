package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func TestJetStreamPurgeStream() error {
	fmt.Println("開始測試 JetStream Purge Stream 的效能")

	conf, err := config.GetConfig()
	if err != nil {
		return xerrors.Errorf("取得設定檔失敗: %w", err)
	}

	natsConn, err := nats.Connect(
		strings.Join(conf.NATSJetStream.Servers, ","),
		nats.Name("ray.jetstream.purge_stream"),
		nats.Token(conf.NATSJetStream.Token),
	)
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	// 取得 JetStream 的 Context
	js, err := natsConn.JetStream()
	if err != nil {
		return xerrors.Errorf("取得 JetStream 的 Context 失敗: %w", err)
	}

	for _, count := range conf.Testers.JetStreamPurgeStreamTest.Counts {
		if err := TestPurgeStream(conf, js, count); err != nil {
			return xerrors.Errorf("測試 Purge Stream 失敗: %w", err)
		}
	}

	return nil
}

func TestPurgeStream(conf *config.Config, js nats.JetStreamContext, count int) error {
	fmt.Printf("\n開始測試 JetStream 的 Purge Stream (%d 筆) 的效能\n", count)

	// JetStream 需要顯示管理 Stream
	streamName := conf.Testers.JetStreamPurgeStreamTest.Stream

	// 可透過 StreamInfo 取得 Stream 相關的資訊
	stream, _ := js.StreamInfo(streamName)

	// 先把之前的 Stream 刪掉 (正常不用這麼做，這是為了測試用)
	if stream != nil {
		if err := js.DeleteStream(streamName); err != nil {
			return xerrors.Errorf("刪除 Stream %s 失敗: %w", streamName, err)
		}
	}

	// 重新建立一個 Stream
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			conf.Testers.JetStreamPurgeStreamTest.Subject, // 例子是用 ray.fuck，但也可以設定 ray.*
		},
	}); err != nil {
		return xerrors.Errorf("建立 Stream %s 失敗: %w", streamName, err)
	}

	for i := 0; i < count; i++ {
		_, err := js.Publish(conf.Testers.JetStreamPurgeStreamTest.Subject, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			return xerrors.Errorf("發布 %s 失敗: %w", conf.Testers.JetStreamPurgeStreamTest.Subject, err)
		}
		// fmt.Println(i)
	}

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


func main() {
	if err := TestJetStreamPurgeStream(); err != nil {
		log.Fatal(err)
	}
}
