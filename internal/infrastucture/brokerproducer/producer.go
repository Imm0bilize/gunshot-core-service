package brokerproducer

import (
	"context"
	"encoding/json"
	"github.com/Imm0bilize/gunshot-core-service/internal/entities"
	"github.com/Imm0bilize/gunshot-core-service/pkg/brokerschemas"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"time"
)

type KafkaProducer struct {
	topic    string
	tracer   trace.Tracer
	producer sarama.SyncProducer
	logger   *zap.Logger
}

func NewKafkaProducer(logger *zap.Logger, producer sarama.SyncProducer, topic string) *KafkaProducer {
	tracer := otel.Tracer("msbroker")

	return &KafkaProducer{
		tracer:   tracer,
		producer: producer,
		logger:   logger,
		topic:    topic,
	}
}

func (k KafkaProducer) Notify(ctx context.Context, request entities.ProcessingRequest) error {
	ctx, span := k.tracer.Start(ctx, "msbroker.Send")
	defer span.End()

	msg := brokerschemas.NotificationMessage{
		NotificationMethods: []string{"telegram"},
		Payload:             request.Payload,
		Timestamp:           request.Timestamp,
		RequestID:           request.RequestID,
		ClientID:            request.ClientID,
		MessageType:         request.MessageType,
	}

	msgBytes, err := json.Marshal(&msg)
	if err != nil {
		return errors.Wrap(err, "json.Marshal")
	}

	producerMsg := &sarama.ProducerMessage{
		Topic:     k.topic,
		Key:       sarama.StringEncoder(msg.RequestID.String()),
		Value:     sarama.ByteEncoder(msgBytes),
		Timestamp: time.Now(),
	}

	otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(producerMsg))

	partition, offset, err := k.producer.SendMessage(producerMsg)
	if err != nil {
		return errors.Wrap(err, "can't send message into kafka")
	}

	k.logger.Info(
		"message successfully send to broker",
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return nil
}
