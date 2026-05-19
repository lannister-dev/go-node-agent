package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Options struct {
	Endpoint       string
	Insecure       bool
	ServiceName    string
	ServiceVersion string
	NodeID         string
}

type Shutdown func(context.Context) error

func Setup(ctx context.Context, opts Options) (Shutdown, error) {
	if opts.Endpoint == "" {
		return noopShutdown, nil
	}
	if opts.ServiceName == "" {
		return nil, errors.New("telemetry: ServiceName required")
	}

	dialOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(opts.Endpoint),
		otlptracegrpc.WithTimeout(5 * time.Second),
	}
	if opts.Insecure {
		dialOpts = append(dialOpts, otlptracegrpc.WithInsecure())
	}

	exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(dialOpts...))
	if err != nil {
		return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(opts.ServiceName),
			semconv.ServiceVersion(opts.ServiceVersion),
			semconv.ServiceInstanceID(opts.NodeID),
		),
		resource.WithHost(),
		resource.WithProcess(),
		resource.WithOS(),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(ctx context.Context) error {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(c)
	}, nil
}

func noopShutdown(_ context.Context) error { return nil }
