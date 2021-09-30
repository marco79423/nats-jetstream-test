package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func RecreateJetStreamStreamIfExists(js nats.JetStreamContext, config *nats.StreamConfig) (*nats.StreamInfo, error) {
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

// PublishJetStreamMessagesWithSize 發布大量訊息 (Subject, 數量)
func PublishJetStreamMessagesWithSize(jetStreamCtx nats.JetStreamContext, subject string, messageCount, messageSize int) error {
	message := GenerateRandomString(messageSize)
	for i := 0; i < messageCount; i++ {
		if _, err := jetStreamCtx.Publish(subject, []byte(message)); err != nil {
			return xerrors.Errorf("發布大量訊息 (Subject: %s, 數量： %d): %w", subject, messageCount, err)
		}
		// fmt.Println(i)
	}
	return nil
}

// AsyncPublishJetStreamMessagesWithSize 發布大量訊息 Async (Subject, 數量)
func AsyncPublishJetStreamMessagesWithSize(jetStreamCtx nats.JetStreamContext, subject string, messageCount, messageSize int) error {
	message := GenerateRandomString(messageSize)
	for i := 0; i < messageCount; i++ {
		if _, err := jetStreamCtx.PublishAsync(subject, []byte(message)); err != nil {
			return xerrors.Errorf("發布大量訊息 (Subject: %s, 數量： %d): %w", subject, messageCount, err)
		}
		// fmt.Println(i)
	}

	<-jetStreamCtx.PublishAsyncComplete()
	return nil
}

// MeasureJetStreamPublishMsgTime 測試 JetStream 發布效能
func MeasureJetStreamPublishMsgTime(jetStreamCtx nats.JetStreamContext, subject string, messageCount, messageSize int) error {
	fmt.Printf("開始測試 JetStream 的發布 (Publish) 效能 (次數: %d, 訊息大小： %d)\n", messageCount, messageSize)

	now := time.Now()
	if err := PublishJetStreamMessagesWithSize(jetStreamCtx, subject, messageCount, messageSize); err != nil {
		return xerrors.Errorf("測量 JetStream 發布訊息所需的時間失敗: %w", subject, err)
	}
	elapsedTime := time.Since(now)

	fmt.Printf("全部 %d 筆發布花費時間 %v (訊息大小： %v, 每筆平均花費 %v)\n",
		messageCount,
		elapsedTime,
		messageSize,
		elapsedTime/time.Duration(messageCount),
	)

	return nil
}

// MeasureJetStreamAsyncPublishMsgTime 測試 JetStream 發布效能 (Async)
func MeasureJetStreamAsyncPublishMsgTime(jetStreamCtx nats.JetStreamContext, subject string, messageCount, messageSize int) error {
	fmt.Printf("開始測試 JetStream 的發布 (AsyncPublish) 效能 (次數: %d, 訊息大小： %d)\n", messageCount, messageSize)

	now := time.Now()
	if err := AsyncPublishJetStreamMessagesWithSize(jetStreamCtx, subject, messageCount, messageSize); err != nil {
		return xerrors.Errorf("測量 JetStream 發布訊息所需的時間失敗: %w", subject, err)
	}
	elapsedTime := time.Since(now)

	fmt.Printf("全部 %d 筆發布花費時間 %v (訊息大小： %v, 每筆平均花費 %v)\n",
		messageCount,
		elapsedTime,
		messageSize,
		elapsedTime/time.Duration(messageCount),
	)

	return nil
}

// MeasureJetStreamSubscribeTime 測量 JetStream 訂閱效能 (Subscribe)
func MeasureJetStreamSubscribeTime(jetStreamCtx nats.JetStreamContext, subject string, messageCount, messageSize int) error {
	fmt.Printf("開始測量 JetStream (Subscribe) 的接收效能 (次數： %d, 訊息大小：%d)\n", messageCount, messageSize)

	if err := PublishJetStreamMessagesWithSize(jetStreamCtx, subject, messageCount, messageSize); err != nil {
		return xerrors.Errorf("測量 JetStream 訂閱所需的時間失敗: %w", subject, err)
	}

	wg := sync.WaitGroup{}
	wg.Add(messageCount)
	now := time.Now()
	if _, err := jetStreamCtx.Subscribe(subject, func(msg *nats.Msg) {
		// fmt.Printf("Received a JetStream message: %s\n", string(msg.Data))
		wg.Done()
	}); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", subject, err)
	}
	wg.Wait()

	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		messageCount,
		elapsedTime,
		elapsedTime/time.Duration(messageCount),
	)
	return nil
}

// MeasureJetStreamChanSubscribeTime 測量 JetStream 訂閱效能 (Chan Subscribe)
func MeasureJetStreamChanSubscribeTime(jetStreamCtx nats.JetStreamContext, subject string, messageCount, messageSize int) error {
	fmt.Printf("開始測量 JetStream (Chan Subscribe) 的接收效能 (次數： %d, 訊息大小：%d)\n", messageCount, messageSize)

	if err := PublishJetStreamMessagesWithSize(jetStreamCtx, subject, messageCount, messageSize); err != nil {
		return xerrors.Errorf("測量 JetStream 訂閱所需的時間失敗: %w", subject, err)
	}

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

// MeasureJetStreamPullSubscribeTime 測量 JetStream 訂閱效能 (Pull Subscribe)
func MeasureJetStreamPullSubscribeTime(jetStreamCtx nats.JetStreamContext, durableName, subject string, messageCount, messageSize, fetchCount int) error {
	fmt.Printf("開始測量 JetStream (Pull Subscribe) 的接收效能 (次數： %d, 訊息大小：%d)\n", messageCount, messageSize)

	if err := PublishJetStreamMessagesWithSize(jetStreamCtx, subject, messageCount, messageSize); err != nil {
		return xerrors.Errorf("測量 JetStream 訂閱所需的時間失敗: %w", subject, err)
	}

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

			msg.Ack()
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
