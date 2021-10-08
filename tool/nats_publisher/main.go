package main

import (
	"fmt"
	"log"
	"time"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"golang.org/x/xerrors"
)

func PublishNATSMessagesForever() error {
	conf, err := config.GetConfig()
	if err != nil {
		return xerrors.Errorf("取得設定檔失敗: %w", err)
	}

	natsConn, err := utils.ConnectNATS(conf, "nats-publisher")
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	// message := utils.GenerateRandomString(80000)
	for i := 0; ; i++ {
		message := fmt.Sprintf("%d", i)

		fmt.Println("publish", message)
		if err := natsConn.Publish("message-publisher", []byte(message)); err != nil {
			return xerrors.Errorf("發布失敗: %w", err)
		}

		time.Sleep(1 * time.Second)
	}
}

func main() {
	if err := PublishNATSMessagesForever(); err != nil {
		log.Fatalf("%+v", err)
	}
}
