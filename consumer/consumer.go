package consumer

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// NewOption configures [Instrumentation] initialization.
type NewOption interface {
	applyNewOption(i *Instrumentation)
}

// WithTracer sets a custom [trace.Tracer] for [Instrumentation].
func WithTracer(tracer trace.Tracer) *OptionWithTracer { return &OptionWithTracer{tracer: tracer} }

// OptionWithTracer is a [NewOption] that injects a [trace.Tracer].
type OptionWithTracer struct{ tracer trace.Tracer }

var (
	_ NewOption = (*OptionWithTracer)(nil)
)

func (o *OptionWithTracer) applyNewOption(i *Instrumentation) {
	i.tracer = o.tracer
}

const (
	defaultTracerName = "github.com/aereal/otelawssqs/consumer.Instrumentation"
)

// New creates an [Instrumentation] instance with the provided options.
//
// If no tracer is explicitly set, a default tracer is used.
func New(opts ...NewOption) *Instrumentation {
	i := &Instrumentation{}
	for _, o := range opts {
		o.applyNewOption(i)
	}
	if i.tracer == nil {
		i.tracer = otel.GetTracerProvider().Tracer(defaultTracerName)
	}
	return i
}

// Instrumentation provides OpenTelemetry instrumentation for Amazon SQS consumers.
type Instrumentation struct {
	tracer trace.Tracer
}

// StartEventSpan starts a span for an Amazon SQS event containing multiple messages.
//
// It is useful for representing the entire batch processing operation.
func (i *Instrumentation) StartEventSpan(ctx context.Context, ev *events.SQSEvent) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		semconv.FaaSTriggerPubsub,
		semconv.MessagingOperationTypeDeliver,
		semconv.MessagingSystemAWSSqs,
		semconv.MessagingBatchMessageCount(len(ev.Records)),
	}
	spanName := "multiple_sources process"
	return i.tracer.Start(ctx, spanName,
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
}

// StartMessageSpan starts a span for an individual Amazon SQS message.
//
// It extracts remote context using AWS X-Ray propagation and links it to the span.
func (i *Instrumentation) StartMessageSpan(ctx context.Context, msg *events.SQSMessage) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		semconv.FaaSTriggerPubsub,
		semconv.MessagingOperationTypeDeliver,
		semconv.MessagingSystemAWSSqs,
		semconv.MessagingMessageID(msg.MessageId),
		semconv.MessagingDestinationName(msg.EventSource),
	}
	remoteCtx := xray.Propagator{}.Extract(ctx, propagation.MapCarrier(msg.Attributes))
	spanName := msg.EventSource + " process"
	return i.tracer.Start(ctx, spanName,
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(trace.LinkFromContext(remoteCtx)),
	)
}
