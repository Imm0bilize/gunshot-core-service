package config

import (
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

type MLServiceConfig struct {
	Host string `env:"MLSERVICE_HOST"`
	Port string `env:"MLSERVICE_PORT"`
}

type OTELConfig struct {
	Host string `env:"OTEL_HOST"`
	Port string `env:"OTEL_PORT"`
}

type KafkaConsumerConfig struct {
	Peers     string `env:"CONSUMER_PEERS"`
	Topic     string `env:"CONSUMER_TOPIC"`
	GroupName string `env:"CONSUMER_GROUP_NAME"`
}

type KafkaProducerConfig struct {
	Peers string `env:"PRODUCER_PEERS"`
	Topic string `env:"PRODUCER_TOPIC"`
}

type Config struct {
	MLService MLServiceConfig
	OTEL      OTELConfig
	Producer  KafkaProducerConfig
	Consumer  KafkaConsumerConfig
}

func New(envFiles ...string) (*Config, error) {
	err := godotenv.Load(envFiles...)
	if err != nil {
		return nil, errors.Wrap(err, "error while load from .env file")
	}

	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, errors.Wrap(err, "error while transfer env to config")
	}

	return &cfg, nil
}
