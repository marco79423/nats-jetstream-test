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

	EnabledTesters []string `mapstructure:"enabled_testers"`
	Testers        Testers  `mapstructure:"testers"`
}

type NATSStreamingConfig struct {
	Servers   []string `mapstructure:"servers"`
	Token     string   `mapstructure:"token"`
	ClusterID string   `mapstructure:"cluster_id"`
	ClientID  string   `mapstructure:"client_id"`
}

type NATSJetStreamConfig struct {
	Servers  []string `mapstructure:"servers"`
	Token    string   `mapstructure:"token"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
}

type Testers struct {
	JetStreamPublishTester *JetStreamPublishTesterConfig `mapstructure:"jetstream_publish_tester"`
	StreamingPublishTester *StreamingPublishTesterConfig `mapstructure:"streaming_publish_tester"`
	NATSPublishTester      *NATSPublishTesterConfig      `mapstructure:"nats_publish_tester"`

	StreamingSubscribeTester     *StreamingSubscribeTesterConfig     `mapstructure:"streaming_subscribe_tester"`
	StreamingLatencyTester       *StreamingLatencyTesterConfig       `mapstructure:"streaming_latency_tester"`
	JetStreamPurgeStreamTester   *JetStreamPurgeStreamTesterConfig   `mapstructure:"jetstream_purge_stream_tester"`
	JetStreamMemoryStorageTester *JetStreamMemoryStorageTesterConfig `mapstructure:"jetstream_memory_storage_tester"`
	JetStreamLatencyTester       *JetStreamLatencyTesterConfig       `mapstructure:"jetstream_latency_tester"`
	JetStreamAsyncPublishTester  *JetStreamAsyncPublishTesterConfig  `mapstructure:"jetstream_async_publish_tester"`
	JetStreamSubscribeTester     *JetStreamSubscribeTesterConfig     `mapstructure:"jetstream_subscribe_tester"`
	JetStreamChanSubscribeTester *JetStreamChanSubscribeTesterConfig `mapstructure:"jetstream_chan_subscribe_tester"`
	JetStreamPullSubscribeTester *JetStreamPullSubscribeTesterConfig `mapstructure:"jetstream_pull_subscribe_tester"`
}

type JetStreamPublishTesterConfig struct {
	Stream       string `mapstructure:"stream"`
	Subject      string `mapstructure:"subject"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type NATSPublishTesterConfig struct {
	Subject      string `mapstructure:"subject"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type StreamingPublishTesterConfig struct {
	Channel      string `mapstructure:"channel"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type StreamingSubscribeTesterConfig struct {
	Channel      string `mapstructure:"channel"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type JetStreamAsyncPublishTesterConfig struct {
	Stream       string `mapstructure:"stream"`
	Subject      string `mapstructure:"subject"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type JetStreamSubscribeTesterConfig struct {
	Stream       string `mapstructure:"stream"`
	Subject      string `mapstructure:"subject"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type JetStreamChanSubscribeTesterConfig struct {
	Stream       string `mapstructure:"stream"`
	Subject      string `mapstructure:"subject"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type JetStreamPullSubscribeTesterConfig struct {
	Stream       string `mapstructure:"stream"`
	Subject      string `mapstructure:"subject"`
	Times        int    `mapstructure:"times"`
	FetchCounts  []int  `mapstructure:"fetch_counts"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type JetStreamLatencyTesterConfig struct {
	Stream  string `mapstructure:"stream"`
	Subject string `mapstructure:"subject"`
	Times   int    `mapstructure:"times"`
}

type StreamingLatencyTesterConfig struct {
	Channel string `mapstructure:"channel"`
	Times   int    `mapstructure:"times"`
}

type JetStreamPurgeStreamTesterConfig struct {
	Stream       string `mapstructure:"stream"`
	Subject      string `mapstructure:"subject"`
	Counts       []int  `mapstructure:"counts"`
	MessageSizes []int  `mapstructure:"message_sizes"`
}

type JetStreamMemoryStorageTesterConfig struct {
	Stream       string `mapstructure:"stream"`
	Subject      string `mapstructure:"subject"`
	Times        int    `mapstructure:"times"`
	MessageSizes []int  `mapstructure:"message_sizes"`
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
