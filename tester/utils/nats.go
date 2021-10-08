package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

// PublishNATSMessagesWithSize 發布大量訊息 (Subject, 數量)
func PublishNATSMessagesWithSize(natsConn *nats.Conn, subject string, times, messageSize int) error {
	message := GenerateRandomString(messageSize)
	for i := 0; i < times; i++ {
		err := natsConn.Publish(subject, []byte(message))
		if err != nil {
			return xerrors.Errorf("發布 %s 失敗: %w", subject, err)
		}
		// fmt.Println(i)
	}
	return nil
}

// MeasureNATSPublishMsgTime 測試 NATS 發布效能
func MeasureNATSPublishMsgTime(natsConn *nats.Conn, subject string, times, messageSize int) error {
	fmt.Printf("開始測量 NATS 的發布效能 (次數： %d, 訊息大小：%d)\n", times, messageSize)

	now := time.Now()
	if err := PublishNATSMessagesWithSize(natsConn, subject, times, messageSize); err != nil {
		return xerrors.Errorf("測量 NATS 發布效能失敗: %w", err)
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

// MeasureNATSSubscribeTime 測試 NATS 訂閱效能
func MeasureNATSSubscribeTime(natsConn *nats.Conn, subject string, messageCount, messageSize int) error {
	fmt.Printf("開始測量 NATS 的接收效能 (次數： %d, 訊息大小：%d)\n", messageCount, messageSize)

	if err := PublishNATSMessagesWithSize(natsConn, subject, messageCount, messageSize); err != nil {
		return xerrors.Errorf("發布大量訊息失敗: %w", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(messageCount)

	now := time.Now()
	if _, err := natsConn.Subscribe(subject, func(msg *nats.Msg) {
		// fmt.Printf("Received a NATS message: %s\n", string(msg.Data))
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
