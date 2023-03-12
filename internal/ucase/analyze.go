package ucase

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type AnalyzeUseCase struct {
	tracer trace.Tracer
}

var (
	_ AnalyzerUCase = AnalyzeUseCase{}
)

var ErrUnknownModelType = errors.New("unknown model type")

func NewAnalyzeUseCase() *AnalyzeUseCase {
	return &AnalyzeUseCase{
		otel.GetTracerProvider().Tracer("AnalyzeUseCase"),
	}
}

func (a AnalyzeUseCase) Analyze(ctx context.Context, modelType string, result []byte) (bool, error) {
	var (
		needToNotify bool
		err          error
	)

	ctx, span := a.tracer.Start(ctx, "Analyze")
	defer span.End()

	switch modelType {
	case "gunshot_detection":
		needToNotify, err = a.binaryClassification(result)
	default:
		span.RecordError(ErrUnknownModelType)
		return false, ErrUnknownModelType
	}

	if err != nil {
		span.RecordError(errors.Wrap(err, "can't analyze result"))
		return false, errors.Wrap(err, "can't analyze result")
	}

	return needToNotify, nil
}

func (a AnalyzeUseCase) binaryClassification(result []byte) (bool, error) {
	logger, _ := zap.NewDevelopment()

	convertedResult := [1]float64{}
	if err := json.Unmarshal(result, &convertedResult); err != nil {
		return false, err
	}
	logger.Info("info", zap.Float64("prob", convertedResult[0]))
	if convertedResult[0] > 0.5 {
		return true, nil
	}

	return false, nil
}
