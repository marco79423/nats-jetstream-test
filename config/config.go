package config

import (
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"golang.org/x/xerrors"
)

func GetConfig() (*Config, error) {
	// 讀取設定檔
	config := &Config{}
	if err := loadConfig(config, "conf.d/config.yml"); err != nil {
		return nil, xerrors.Errorf("無法取得設定檔: %w", err)
	}

	return config, nil
}

type Config struct {
	NATSStreaming NATSStreamingConfig `mapstructure:"nats_streaming"`
	NATSJetStream NATSJetStreamConfig `mapstructure:"nats_jet_stream"`

	Testers Testers `mapstructure:"testers"`
}

type NATSStreamingConfig struct {
	Servers   []string `mapstructure:"servers"`
	Token     string   `mapstructure:"token"`
	ClusterID string   `mapstructure:"cluster_id"`
	ClientID  string   `mapstructure:"client_id"`
}

type NATSJetStreamConfig struct {
	Servers []string `mapstructure:"servers"`
	Token   string   `mapstructure:"token"`
}

type Testers struct {
	StreamingPerformanceTester   *StreamingPerformanceTesterConfig   `mapstructure:"streaming_performance_tester"`
	JetStreamPurgeStreamTester   *JetStreamPurgeStreamTesterConfig   `mapstructure:"jetstream_purge_stream_tester"`
	JetStreamMemoryStorageTester *JetStreamMemoryStorageTesterConfig `mapstructure:"jetstream_memory_storage_tester"`
	JetStreamPerformanceTester   *JetStreamPerformanceTesterConfig   `mapstructure:"jetstream_performance_tester"`
}

type StreamingPerformanceTesterConfig struct {
	Channel     string `mapstructure:"channel"`
	Times       int    `mapstructure:"times"`
	MessageSize int    `mapstructure:"message_size"`
}

type JetStreamPerformanceTesterConfig struct {
	Stream      string `mapstructure:"stream"`
	Subject     string `mapstructure:"subject"`
	Times       int    `mapstructure:"times"`
	MessageSize int    `mapstructure:"message_size"`
}

type JetStreamPurgeStreamTesterConfig struct {
	Stream      string `mapstructure:"stream"`
	Subject     string `mapstructure:"subject"`
	Counts      []int  `mapstructure:"counts"`
	MessageSize int    `mapstructure:"message_size"`
}

type JetStreamMemoryStorageTesterConfig struct {
	Stream      string `mapstructure:"stream"`
	Subject     string `mapstructure:"subject"`
	Times       int    `mapstructure:"times"`
	MessageSize int    `mapstructure:"message_size"`
}

// loadConfig 讀取設定檔
func loadConfig(rawConfig interface{}, configPath string) error {
	absConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return xerrors.Errorf("無法讀取設定檔 %s: %w", configPath, err)
	}

	if _, err := os.Stat(absConfigPath); os.IsNotExist(err) {
		return xerrors.Errorf("無法讀取設定檔 %s: %w", configPath, err)
	}

	viper.SetConfigType("yaml")
	viper.SetConfigFile(absConfigPath)

	if err := viper.ReadInConfig(); err != nil {
		return xerrors.Errorf("無法讀取設定檔 %s: %w", configPath, err)
	}

	if err := viper.Unmarshal(rawConfig); err != nil {
		return xerrors.Errorf("無法讀取設定檔 %s: %w", configPath, err)
	}

	return nil
}
