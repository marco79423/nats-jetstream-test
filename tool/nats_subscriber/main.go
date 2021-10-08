package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/marco79423/nats-jetstream-test/tester/utils"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

func SubscribeNATSMessages() error {
	conf, err := config.GetConfig()
	if err != nil {
		return xerrors.Errorf("取得設定檔失敗: %w", err)
	}

	natsConn, err := utils.ConnectNATS(conf, "nats-subscriber")
	if err != nil {
		return xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	subject := "message-publisher"
	if _, err := natsConn.Subscribe(subject, func(msg *nats.Msg) {
		fmt.Println(string(msg.Data))
	}); err != nil {
		return xerrors.Errorf("訂閱 %s 失敗: %w", subject, err)
	}

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	return nil
}

func main() {
	if err := SubscribeNATSMessages(); err != nil {
		log.Fatalf("%+v", err)
	}
}
