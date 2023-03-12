package brokerconsumer

import (
	"context"
	"encoding/json"
	"github.com/Imm0bilize/gunshot-api-service/pkg/api/brokerschemas"
	"github.com/Imm0bilize/gunshot-core-service/internal/config"
	"github.com/Imm0bilize/gunshot-core-service/internal/entities"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"strings"
	"time"
)

type MessageProcessor interface {
	Process(ctx context.Context, msg entities.ProcessingRequest) error
}

type KafkaConsumer struct {
	logger    *zap.Logger
	processor MessageProcessor
	group     sarama.ConsumerGroup
	ready     chan struct{}
	peers     []string
}

const gunshotDetectionType = "gunshot_detection"

func NewKafkaConsumer(
	cfg config.KafkaConsumerConfig, logger *zap.Logger, processor MessageProcessor,
) (*KafkaConsumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V3_3_0_0
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Session.Timeout = 20 * time.Second
	saramaConfig.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	saramaConfig.Consumer.MaxProcessingTime = 3 * time.Second

	group, err := sarama.NewConsumerGroup(strings.Split(cfg.Peers, ","), cfg.GroupName, saramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "error creating consumer group client")
	}

	return &KafkaConsumer{
		logger:    logger.Named("kafka-consumer"),
		group:     group,
		processor: processor,
		peers:     strings.Split(cfg.Peers, ","),
		ready:     make(chan struct{}),
	}, nil
}

func (k KafkaConsumer) Run(ctx context.Context) error {
	errChan := make(chan error)

	go func() {
		for {
			if err := k.group.Consume(ctx, []string{"ApiServiceOutput"}, &k); err != nil {
				k.logger.Error("error from consumer", zap.Error(err))
				errChan <- err
			}

			if ctx.Err() != nil {
				return
			}

			k.ready = make(chan struct{})
		}
	}()

	<-k.ready

	select {
	case <-ctx.Done():
		k.logger.Info("terminating consume: context canceled")
	case err := <-errChan:
		return err
	}

	if err := k.group.Close(); err != nil {
		return errors.Wrap(err, "error closing consumer group")
	}

	return nil
}

func (k KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	k.logger.Debug("setup")
	return nil
}

func (k KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	k.logger.Debug("cleanup")
	return nil
}

func (k KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			go func() {
				var msg brokerschemas.AudioMessage

				if err := json.Unmarshal(message.Value, &msg); err != nil {
					k.logger.Error(
						"error unmarshalling Kafka message",
						zap.ByteString("data", message.Value),
						zap.Error(err),
					)
				}

				ctx := otel.GetTextMapPropagator().Extract(
					context.Background(), otelsarama.NewConsumerMessageCarrier(message),
				)

				if err := k.processor.Process(ctx, entities.ProcessingRequest{
					ClientID:         msg.Payload.ID.Hex(),
					TypeOfPrediction: gunshotDetectionType,
					Payload:          msg.Payload.Payload,
				}); err != nil {
					k.logger.Error("error processing request",
						zap.String("requestID", msg.RequestID.String()),
						zap.Error(err),
					)
				}

				session.MarkMessage(message, "")
			}()
		case <-session.Context().Done():
			return nil
		}
	}
}
