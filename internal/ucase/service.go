package ucase

import (
	"context"
	"github.com/Imm0bilize/gunshot-core-service/internal/entities"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type (
	PredictorUCase interface {
		Predict(ctx context.Context, modelType string, data []byte) ([]byte, error)
	}

	AnalyzerUCase interface {
		Analyze(ctx context.Context, modelType string, result []byte) (bool, error)
	}

	NotifierUCase interface {
		Notify(ctx context.Context, msg entities.ProcessingRequest) error
	}
)

type UseCase struct {
	analyzer  AnalyzerUCase
	predictor PredictorUCase
	notifier  NotifierUCase

	tracer trace.Tracer
}

func NewUseCase(
	predictor PredictorUCase,
	analyzer AnalyzerUCase,
	notifier NotifierUCase,
) *UseCase {
	return &UseCase{
		analyzer:  analyzer,
		predictor: predictor,
		notifier:  notifier,
		tracer:    otel.GetTracerProvider().Tracer("uCase"),
	}
}

func (u UseCase) Process(ctx context.Context, request entities.ProcessingRequest) error {
	ctx, span := u.tracer.Start(ctx, "ProcessRequest")
	defer span.End()

	res, err := u.predictor.Predict(ctx, request.TypeOfPrediction, request.Payload)
	if err != nil {
		return errors.Wrap(err, "predictor.Predict")
	}

	_, err = u.analyzer.Analyze(ctx, request.TypeOfPrediction, res)
	if err != nil {
		return errors.Wrap(err, "analyzer.Analyze")
	}

	//needNotify, err := u.analyzer.Analyze(ctx, request.TypeOfPrediction, res)
	//if err != nil {
	//	return errors.Wrap(err, "analyzer.Analyze")
	//}
	//
	//if !needNotify {
	//	return nil
	//}

	if err = u.notifier.Notify(ctx, request); err != nil {
		return errors.Wrap(err, "notifier.Notify")
	}

	return nil
}
