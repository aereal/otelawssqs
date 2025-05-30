package producer_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aereal/otelawssqs/internal/carrier"
	"github.com/aereal/otelawssqs/internal/testutils"
	"github.com/aereal/otelawssqs/producer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestConfigureMiddleware_SendMessage(t *testing.T) {
	queueURL := "http://sqs.test/queue_1"
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
	ctx := t.Context()
	ctx, span := tp.Tracer("test").Start(ctx, "span")
	traceHeaderCarrier := &carrier.ScalarCarrier{}
	xray.Propagator{}.Inject(ctx, traceHeaderCarrier)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		input := new(sqs.SendMessageInput)
		if err := json.NewDecoder(r.Body).Decode(input); err != nil {
			t.Error(err)
		}
		if *input.QueueUrl != queueURL {
			t.Errorf("QueueURL:\n\twant: %q\n\t got: %q", queueURL, *input.QueueUrl)
		}
		wantSystemAttrs := map[string]types.MessageSystemAttributeValue{
			string(types.MessageSystemAttributeNameAWSTraceHeader): {
				DataType:    aws.String("String"),
				StringValue: aws.String(traceHeaderCarrier.Value()),
			},
		}
		if diff := diffSQSSystemAttrs(wantSystemAttrs, input.MessageSystemAttributes); diff != "" {
			t.Errorf("MessageSystemAttributes (-want, +got):\n%s", diff)
		}
	}))
	t.Cleanup(srv.Close)

	cfg := aws.Config{}
	producer.ConfigureMiddleware(&cfg.APIOptions)
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = &srv.URL
		o.Credentials = aws.AnonymousCredentials{}
	})
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(`{}`),
	})
	span.End()
	if err != nil {
		t.Fatal(err)
	}
	if err := tp.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	wantSpans := tracetest.SpanStubs{
		{
			Name:     "span",
			SpanKind: trace.SpanKindInternal,
		},
	}
	gotSpans := exporter.GetSpans()
	if diff := testutils.DiffSpans(wantSpans, gotSpans); diff != "" {
		t.Errorf("spans (-want, +got):\n%s", diff)
	}
}

func TestConfigureMiddleware_SendMessageBatch(t *testing.T) {
	queueURL := "http://sqs.test/queue_1"
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
	ctx := t.Context()
	ctx, span := tp.Tracer("test").Start(ctx, "span")
	traceHeaderCarrier := &carrier.ScalarCarrier{}
	xray.Propagator{}.Inject(ctx, traceHeaderCarrier)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		input := new(sqs.SendMessageBatchInput)
		if err := json.NewDecoder(r.Body).Decode(input); err != nil {
			t.Error(err)
		}
		wantSystemAttrs := map[string]types.MessageSystemAttributeValue{
			string(types.MessageSystemAttributeNameAWSTraceHeader): {
				DataType:    aws.String("String"),
				StringValue: aws.String(traceHeaderCarrier.Value()),
			},
		}
		want := &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries: []types.SendMessageBatchRequestEntry{
				{
					Id:                      aws.String("1"),
					MessageBody:             aws.String(`{}`),
					MessageSystemAttributes: wantSystemAttrs,
				},
				{
					Id:                      aws.String("2"),
					MessageBody:             aws.String(`{}`),
					MessageSystemAttributes: wantSystemAttrs,
				},
			},
		}
		if diff := diffSendMessageBatchInput(want, input); diff != "" {
			t.Errorf("SendMessageBatchInput (-want, +got):\n%s", diff)
		}
	}))
	t.Cleanup(srv.Close)

	cfg := aws.Config{}
	producer.ConfigureMiddleware(&cfg.APIOptions)
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = &srv.URL
		o.Credentials = aws.AnonymousCredentials{}
	})
	_, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries: []types.SendMessageBatchRequestEntry{
			{
				Id:          aws.String("1"),
				MessageBody: aws.String(`{}`),
			},
			{
				Id:          aws.String("2"),
				MessageBody: aws.String(`{}`),
			},
		},
	})
	span.End()
	if err != nil {
		t.Fatal(err)
	}
	if err := tp.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	wantSpans := tracetest.SpanStubs{
		{
			Name:     "span",
			SpanKind: trace.SpanKindInternal,
		},
	}
	gotSpans := exporter.GetSpans()
	if diff := testutils.DiffSpans(wantSpans, gotSpans); diff != "" {
		t.Errorf("spans (-want, +got):\n%s", diff)
	}
}

func diffSQSSystemAttrs(want, got map[string]types.MessageSystemAttributeValue) string {
	return cmp.Diff(want, got, cmpopts.IgnoreUnexported(types.MessageSystemAttributeValue{}))
}

func diffSendMessageBatchInput(want, got *sqs.SendMessageBatchInput) string {
	return cmp.Diff(want, got,
		cmpopts.IgnoreUnexported(
			sqs.SendMessageBatchInput{},
			types.MessageSystemAttributeValue{},
			types.SendMessageBatchRequestEntry{},
		),
	)
}
