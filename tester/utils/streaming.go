package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
	"golang.org/x/xerrors"
)

// PublishStreamingMessagesWithSize 發布大量訊息 (Subject, 數量)
func PublishStreamingMessagesWithSize(stanConn stan.Conn, channel string, times, messageSize int) error {
	message := GenerateRandomString(messageSize)
	for i := 0; i < times; i++ {
		err := stanConn.Publish(channel, []byte(message))
		if err != nil {
			return xerrors.Errorf("發布 %s 失敗: %w", channel, err)
		}
		// fmt.Println(i)
	}
	return nil
}

// MeasureStreamingPublishTime 測試 Streaming 發布效能
func  MeasureStreamingPublishTime(stanConn stan.Conn, channel string, times, messageSize int) error {
	fmt.Println("開始測量 Streaming 的發布效能")

	now := time.Now()
	if err := PublishStreamingMessagesWithSize(stanConn, channel, times, messageSize); err != nil {
		return xerrors.Errorf("測量 Streaming 發布訊息效能失敗: %w", channel, err)
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

// MeasureStreamingSubscribeTime 測試 Streaming 訂閱效能
func MeasureStreamingSubscribeTime(stanConn stan.Conn, channel string, times int) error {
	fmt.Println("開始測量 Streaming 的接收效能")

	now := time.Now()

	wg := sync.WaitGroup{}
	wg.Add(times)
	if _, err := stanConn.Subscribe(channel, func(msg *stan.Msg) {
		// fmt.Printf("Received a Streaming message: %s\n", string(msg.Data))
		wg.Done()
	}, stan.StartAt(pb.StartPosition_First)); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", channel, err)
	}

	wg.Wait()

	elapsedTime := time.Since(now)
	fmt.Printf("全部 %d 筆接收花費時間 %v (每筆平均花費 %v)\n",
		times,
		elapsedTime,
		elapsedTime/time.Duration(times),
	)
	return nil
}
