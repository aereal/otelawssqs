package testutils

import (
	"encoding/json"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func DiffSpans(want, got tracetest.SpanStubs) string {
	return cmp.Diff(want, got,
		optTransformAttributeKeyValue,
		optTransformTraceSpanContext,
		optIgnoreEventField,
		optIgnoreSpanStubField,
	)
}

var (
	optTransformAttributeKeyValue = cmp.Transformer("attribute.KeyValue.slice", func(attrs []attribute.KeyValue) attributeMap {
		return fromSet(attribute.NewSet(attrs...))
	})
	optTransformTraceSpanContext = cmp.Transformer("trace.SpanContext", func(sc trace.SpanContext) map[string]any {
		jv, err := json.Marshal(sc)
		if err != nil {
			panic(err)
		}
		var m map[string]any
		if err := json.Unmarshal(jv, &m); err != nil {
			panic(err)
		}
		return m
	})
	optIgnoreEventField    = cmpopts.IgnoreFields(sdktrace.Event{}, "Time")
	optIgnoreSpanStubField = cmpopts.IgnoreFields(tracetest.SpanStub{},
		"EndTime",
		"Parent",
		"SpanContext",
		"StartTime",
		"Resource", // Resource contains telemetry.sdk.version that may change if the library updated
		"InstrumentationScope",
		"InstrumentationLibrary", // InstrumentationLibrary is deprecated
	)
)

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
