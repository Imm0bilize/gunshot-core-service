package app

import (
	"context"
	"github.com/Imm0bilize/gunshot-core-service/internal/config"
	"github.com/Imm0bilize/gunshot-core-service/internal/controller/brokerconsumer"
	"github.com/Imm0bilize/gunshot-core-service/internal/infrastucture/brokerproducer"
	"github.com/Imm0bilize/gunshot-core-service/internal/ucase"
	"github.com/Imm0bilize/gunshot-core-service/pkg/mlservice"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func createTraceProvider(cfg config.OTELConfig) func(context.Context) error {
	exporter, err := otlptrace.New(
		context.Background(),
		otlptracegrpc.NewClient(
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(net.JoinHostPort(cfg.Host, cfg.Port)),
		),
	)

	if err != nil {
		log.Fatal(err)
	}
	resources, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", "gunshot-main-worker-service"),
			attribute.String("library.language", "go"),
		),
	)
	if err != nil {
		log.Printf("Could not set resources: ", err)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	otel.SetTracerProvider(
		sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(resources),
		),
	)

	return exporter.Shutdown
}

func createKafkaProducer(cfg config.KafkaProducerConfig) (sarama.SyncProducer, error) {
	kfkCfg := sarama.NewConfig()
	kfkCfg.Version = sarama.V3_3_0_0
	kfkCfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(cfg.Peers, ","), kfkCfg)
	if err != nil {
		return nil, errors.Wrap(err, "error during create producer")
	}

	producer = otelsarama.WrapSyncProducer(kfkCfg, producer)

	return producer, nil
}

func Run(cfg *config.Config) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	logger.Debug("logger initialized")

	shutdownTraceProvider := createTraceProvider(cfg.OTEL)

	predictor, err := mlservice.NewClient(cfg.MLService, logger)
	if err != nil {
		logger.Fatal("error creating ml service client", zap.Error(err))
	}
	logger.Debug("ml service client created")

	producer, err := createKafkaProducer(cfg.Producer)
	if err != nil {
		logger.Fatal("")
	}

	notifier := brokerproducer.NewKafkaProducer(logger, producer, "GunshotNotificationInput")

	uCase := ucase.NewUseCase(
		predictor,
		ucase.NewAnalyzeUseCase(),
		notifier,
	)

	consumer, err := brokerconsumer.NewKafkaConsumer(cfg.Consumer, logger, uCase)
	if err != nil {
		logger.Fatal("error creating Kafka consumer", zap.Error(err))
	}

	go func() {
		if err = consumer.Run(ctx); err != nil {
			logger.Fatal("error running consumer", zap.Error(err))
		}
	}()

	logger.Debug("kafka consumer run")

	<-ctx.Done()

	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shCancel()

	if err = predictor.Shutdown(shCtx); err != nil {
		logger.Error("error stopping ml service client", zap.Error(err))
	}

	if err = shutdownTraceProvider(shCtx); err != nil {
		logger.Error("error stopping trace provider", zap.Error(err))
	}
}
