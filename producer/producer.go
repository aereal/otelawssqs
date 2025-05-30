package producer

import (
	"context"

	"github.com/aereal/otelawssqs/internal/carrier"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
)

// ConfigureMiddleware injects AWS X-Ray trace context into Amazon SQS message system attributes
// by registering a middleware into the provided API options slice.
//
// This enables trace propagation from producers to consumers via Amazon SQS.
func ConfigureMiddleware(apiOptions *[]func(*middleware.Stack) error) {
	prop := xray.Propagator{}
	*apiOptions = append(*apiOptions, func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("github.com/aereal/otelawssqs/producer", func(ctx context.Context, input middleware.InitializeInput, handler middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			traceIDCarrier := &carrier.ScalarCarrier{}
			prop.Inject(ctx, traceIDCarrier)
			switch params := input.Parameters.(type) {
			case *sqs.SendMessageInput:
				if params.MessageSystemAttributes == nil {
					params.MessageSystemAttributes = map[string]types.MessageSystemAttributeValue{}
				}
				addTraceHeader(params.MessageSystemAttributes, traceIDCarrier.Value())
			case *sqs.SendMessageBatchInput:
				copied := make([]types.SendMessageBatchRequestEntry, 0, len(params.Entries))
				for _, entry := range params.Entries {
					if entry.MessageSystemAttributes == nil {
						entry.MessageSystemAttributes = map[string]types.MessageSystemAttributeValue{}
					}
					addTraceHeader(entry.MessageSystemAttributes, traceIDCarrier.Value())
					copied = append(copied, entry)
				}
				params.Entries = copied
			}
			return handler.HandleInitialize(ctx, input)
		}), middleware.After)
	})
}

func addTraceHeader(m map[string]types.MessageSystemAttributeValue, traceID string) {
	m[string(types.MessageSystemAttributeNameAWSTraceHeader)] = types.MessageSystemAttributeValue{
		DataType:    ref("String"),
		StringValue: ref(traceID),
	}
}

func ref[V any](v V) *V { return &v }
