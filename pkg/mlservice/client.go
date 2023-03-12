package mlservice

import (
	"context"
	"github.com/Imm0bilize/gunshot-core-service/internal/config"
	"github.com/Imm0bilize/gunshot-core-service/pkg/mlservice/api"
	"github.com/avast/retry-go"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"time"
)

type Client struct {
	client api.InferenceAPIsServiceClient
	tracer trace.Tracer
	conn   *grpc.ClientConn
	logger *zap.Logger
}

func NewClient(cfg config.MLServiceConfig, logger *zap.Logger) (*Client, error) {
	conn, err := grpc.Dial(
		net.JoinHostPort(cfg.Host, cfg.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "can`t create connection to ml-service")
	}

	c := api.NewInferenceAPIsServiceClient(conn)

	if _, err := c.Ping(context.Background(), &emptypb.Empty{}); err != nil {
		logger.Fatal(err.Error())
	}

	return &Client{
		client: c,
		conn:   conn,
		logger: logger.Named("ml-service-client"),
		tracer: otel.Tracer("mlservice-client"),
	}, nil
}

func (c Client) Predict(ctx context.Context, predictionType string, data []byte) ([]byte, error) {
	ctx, span := c.tracer.Start(ctx, "MLServiceClient.Predict")
	defer span.End()

	var (
		response *api.PredictionResponse
		request  = &api.PredictionsRequest{
			ModelName: predictionType,
			Input:     map[string][]byte{"data": data},
		}
	)

	if err := retry.Do(
		func() error {
			var err error

			response, err = c.client.Predictions(ctx, request)
			if err != nil {
				// TODO: использовать ретрай для конкретных ошибок, а не для всех
				span.AddEvent("prediction error, use retry")
				return err
			}

			return nil
		},
		retry.Attempts(3),
		retry.Delay(50*time.Millisecond),
	); err != nil {
		span.RecordError(err)
		c.logger.Error("error during make prediction", zap.Error(err))
		return nil, errors.Wrap(err, "error during make prediction")
	}

	return response.Prediction, nil
}

func (c Client) Shutdown(_ context.Context) error {
	return c.conn.Close()
}
