package utils

import (
	"strings"

	"github.com/marco79423/nats-jetstream-test/config"
	"github.com/nats-io/nats.go"
	"golang.org/x/xerrors"
)

// ConnectNATS 取得 NATS 的連線
func ConnectNATS(conf *config.Config, name string) (*nats.Conn, error) {
	natsConn, err := nats.Connect(
		strings.Join(conf.NATSJetStream.Servers, ","),
		nats.Name(name),
		nats.Token(conf.NATSJetStream.Token),
	)
	if err != nil {
		return nil, xerrors.Errorf("取得 NATS 連線失敗: %w", err)
	}

	return natsConn, nil
}
