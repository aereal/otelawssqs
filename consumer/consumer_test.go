package consumer_test

import (
	"encoding/json"
	"testing"

	"github.com/aereal/otelawssqs/consumer"
	"github.com/aws/aws-lambda-go/events"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestInstrumentation_StartMessageSpan(t *testing.T) {
	testCases := []struct {
		name      string
		input     events.SQSMessage
		wantSpans tracetest.SpanStubs
	}{
		{
			name: "no X-Ray trace header",
			input: events.SQSMessage{
				EventSource: "queue_1",
				MessageId:   "0xdeadbeaf",
			},
			wantSpans: tracetest.SpanStubs{
				{
					Name:     "queue_1 process",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						attribute.Key("faas.trigger").String("pubsub"),
						attribute.Key("messaging.operation.type").String("process"),
						attribute.Key("messaging.system").String("aws_sqs"),
						attribute.Key("messaging.message.id").String("0xdeadbeaf"),
						attribute.Key("messaging.destination.name").String("queue_1"),
					},
				},
			},
		},
		{
			name: "X-Ray trace header supplied",
			input: events.SQSMessage{
				EventSource: "queue_1",
				MessageId:   "0xdeadbeaf",
				Attributes: map[string]string{
					"X-Amzn-Trace-Id": "Root=1-abcdef12-1234567890abcdef12345678;Parent=1234567890abcdef;Sampled=1",
				},
			},
			wantSpans: tracetest.SpanStubs{
				{
					Name:     "queue_1 process",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						attribute.Key("faas.trigger").String("pubsub"),
						attribute.Key("messaging.operation.type").String("process"),
						attribute.Key("messaging.system").String("aws_sqs"),
						attribute.Key("messaging.message.id").String("0xdeadbeaf"),
						attribute.Key("messaging.destination.name").String("queue_1"),
					},
					Links: []sdktrace.Link{
						{
							SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
								TraceID:    must(trace.TraceIDFromHex("abcdef121234567890abcdef12345678")),
								SpanID:     must(trace.SpanIDFromHex("1234567890abcdef")),
								TraceFlags: trace.FlagsSampled,
								Remote:     true,
							}),
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			ctx := t.Context()
			tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
			inst := consumer.New(consumer.WithTracer(tp.Tracer("test")))
			_, span := inst.StartMessageSpan(ctx, tc.input)
			span.End()
			if err := tp.ForceFlush(ctx); err != nil {
				t.Fatal(err)
			}
			gotSpans := exporter.GetSpans()
			if diff := cmpSpans(tc.wantSpans, gotSpans); diff != "" {
				t.Errorf("spans (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestInstrumentation_StartEventSpan(t *testing.T) {
	testCases := []struct {
		name      string
		input     events.SQSEvent
		wantSpans tracetest.SpanStubs
	}{
		{
			name: "single message",
			input: events.SQSEvent{
				Records: []events.SQSMessage{
					{},
					{},
				},
			},
			wantSpans: tracetest.SpanStubs{
				{
					Name:     "multiple_sources process",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						attribute.Key("faas.trigger").String("pubsub"),
						attribute.Key("messaging.batch.message_count").Int64(2),
						attribute.Key("messaging.operation.type").String("process"),
						attribute.Key("messaging.system").String("aws_sqs"),
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			ctx := t.Context()
			tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
			inst := consumer.New(consumer.WithTracer(tp.Tracer("test")))
			_, span := inst.StartEventSpan(ctx, tc.input)
			span.End()
			if err := tp.ForceFlush(ctx); err != nil {
				t.Fatal(err)
			}
			gotSpans := exporter.GetSpans()
			if diff := cmpSpans(tc.wantSpans, gotSpans); diff != "" {
				t.Errorf("spans (-want, +got):\n%s", diff)
			}
		})
	}
}

func cmpSpans(want, got tracetest.SpanStubs) string {
	return cmp.Diff(want, got,
		cmp.Transformer("attribute.KeyValue.slice", func(attrs []attribute.KeyValue) attributeMap {
			return fromSet(attribute.NewSet(attrs...))
		}),
		cmp.Transformer("attribute.Set", func(attrs attribute.Set) attributeMap {
			return fromSet(attrs)
		}),
		cmp.Transformer("trace.SpanContext", func(sc trace.SpanContext) map[string]any {
			jv, err := json.Marshal(sc)
			if err != nil {
				panic(err)
			}
			var m map[string]any
			if err := json.Unmarshal(jv, &m); err != nil {
				panic(err)
			}
			return m
		}),
		cmpopts.IgnoreFields(sdktrace.Event{}, "Time"),
		cmpopts.IgnoreFields(tracetest.SpanStub{},
			"EndTime",
			"Parent",
			"SpanContext",
			"StartTime",
			"Resource", // Resource contains telemetry.sdk.version that may change if the library updated
			"InstrumentationScope",
			"InstrumentationLibrary", // InstrumentationLibrary is deprecated
		),
	)
}

func fromSet(set attribute.Set) attributeMap {
	r := attributeMap{}
	for _, kv := range set.ToSlice() {
		r[string(kv.Key)] = attributeValue{Type: kv.Value.Type().String(), Value: kv.Value.Emit()}
	}
	return r
}

type attributeMap map[string]attributeValue

type attributeValue struct {
	Type  string
	Value string
}

func must[V any](value V, err error) V {
	if err != nil {
		panic(err)
	}
	return value
}
