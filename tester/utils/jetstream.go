package utils

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func RecreateStreamIfExists(js nats.JetStreamContext, config *nats.StreamConfig) (*nats.StreamInfo, error) {
	streamName := config.Name

	// 可透過 StreamInfo 確認 Stream 是否存在
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		if err != nats.ErrStreamNotFound {
			return nil, xerrors.Errorf("重建 Stream 失敗: %w", err)
		}
	}

	// 如果 Stream 存在就刪掉
	if stream != nil {
		if err := js.DeleteStream(streamName); err != nil {
			return nil, xerrors.Errorf("重建 Stream 失敗: %w", err)
		}
	}

	// 建立 Stream
	stream, err = js.AddStream(config)
	if err != nil {
		return nil, xerrors.Errorf("重建 Stream 失敗: %w", err)
	}

	return stream, nil
}

// PublishMessagesWithSize 發布大量訊息 (Subject, 數量)
func PublishMessagesWithSize(jetStreamCtx nats.JetStreamContext, subject string, times, messageSize int) error {
	message := GenerateRandomString(messageSize)
	for i := 0; i < times; i++ {
		if _, err := jetStreamCtx.Publish(subject, []byte(message)); err != nil {
			return xerrors.Errorf("發布大量訊息 (Subject: %s, 數量： %d): %w", subject, times, err)
		}
		// fmt.Println(i)
	}
	return nil
}

// MeasurePublishMsgTime 測試 JetStream 發布效能
func MeasurePublishMsgTime(jetStreamCtx nats.JetStreamContext, subject string, times, messageSize int) error {
	fmt.Println("開始測試 JetStream 的發布效能")

	now := time.Now()
	if err := PublishMessagesWithSize(jetStreamCtx, subject, times, messageSize); err != nil {
		return xerrors.Errorf("測量 JetStream 發布訊息所需的時間失敗: %w", subject, err)
	}
	elapsedTime := time.Since(now)

	fmt.Printf("全部 %d 筆發布花費時間 %v (訊息大小： %v, 每筆平均花費 %v)\n",
		times,
		elapsedTime,
		messageSize,
		elapsedTime/time.Duration(times),
	)

	return nil
}

// MeasureSubscribeTime 測量 JetStream 訂閱效能 (Subscribe)
func MeasureSubscribeTime(jetStreamCtx nats.JetStreamContext, subject string, messageCount int) error {
	fmt.Println("開始測量 JetStream (Subscribe) 的接收效能")

	now := time.Now()
	quit := make(chan int)
	if _, err := jetStreamCtx.Subscribe(subject, func(msg *nats.Msg) {
		// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))
		quit <- 1
	}); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", subject, err)
	}

	for i := 0; i < messageCount; i++ {
		<-quit
	}

	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		messageCount,
		elapsedTime,
		elapsedTime/time.Duration(messageCount),
	)
	return nil
}

// MeasureChanSubscribeTime 測量 JetStream 訂閱效能 (Chan Subscribe)
func MeasureChanSubscribeTime(jetStreamCtx nats.JetStreamContext, subject string, messageCount int) error {
	fmt.Println("開始測量 JetStream (Chan Subscribe) 的接收效能")

	now := time.Now()
	msgChan := make(chan *nats.Msg, 10000)
	if _, err := jetStreamCtx.ChanSubscribe(subject, msgChan); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", subject, err)
	}

	receiveCount := 0
	for msg := range msgChan {
		_ = msg
		// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))

		receiveCount += 1
		if receiveCount == messageCount {
			break
		}
	}

	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		messageCount,
		elapsedTime,
		elapsedTime/time.Duration(messageCount),
	)
	return nil
}

// MeasurePullSubscribeTime 測量 JetStream 訂閱效能 (Pull Subscribe)
func MeasurePullSubscribeTime(jetStreamCtx nats.JetStreamContext, durableName, subject string, messageCount, fetchCount int) error {
	fmt.Println("開始測量 JetStream (Pull Subscribe) 的接收效能")

	now := time.Now()
	sub, err := jetStreamCtx.PullSubscribe(subject, durableName)
	if err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", subject, err)
	}

	receiveCount := 0
	for receiveCount < messageCount {
		msgs, _ := sub.Fetch(fetchCount) // 不同數量也會有區別

		for _, msg := range msgs {
			_ = msg
			// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))
			receiveCount++
		}
	}

	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (一次抓 %d 筆，每筆平均花費 %v)\n",
		messageCount,
		elapsedTime,
		fetchCount,
		elapsedTime/time.Duration(messageCount),
	)
	return nil
}
