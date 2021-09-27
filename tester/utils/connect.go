package utils

import (
	"fmt"
	"strings"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"golang.org/x/xerrors"
)

// ConnectNATS 取得 NATS 的連線
func ConnectNATS(conf *config.Config, name string) (*nats.Conn, error) {
	natsConn, err := nats.Connect(
		strings.Join(conf.NATSJetStream.Servers, ","),
		nats.Name(name),
		nats.Token(conf.NATSJetStream.Token),

		nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			fmt.Println("NATS 重連成功")
		}),
		nats.ErrorHandler(func(conn *nats.Conn, subscription *nats.Subscription, err error) {
			fmt.Println("NATS 連線錯誤: %w", err)
		}),
	)
	if err != nil {
		return nil, xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	return natsConn, nil
}

// ConnectSTAN 取得 NATS Streaming 的連線
func ConnectSTAN(conf *config.Config, name string) (stan.Conn, error) {
	stanConn, err := stan.Connect(
		conf.NATSStreaming.ClusterID,
		conf.NATSStreaming.ClientID,
		stan.NatsURL(strings.Join(conf.NATSStreaming.Servers, ",")),
		stan.NatsOptions(
			nats.Name(name),
			nats.Token(conf.NATSStreaming.Token),
		),
	)
	if err != nil {
		return nil, xerrors.Errorf("取得 STAN 連線失敗: %w", err)
	}

	return stanConn, nil
}
