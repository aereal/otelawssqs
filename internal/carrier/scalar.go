package carrier

import "go.opentelemetry.io/otel/propagation"

type ScalarCarrier struct {
	value string
}

var _ propagation.TextMapCarrier = (*ScalarCarrier)(nil)

func (*ScalarCarrier) Keys() []string { return []string{""} }

func (sc *ScalarCarrier) Get(_ string) string { return sc.value }

func (sc *ScalarCarrier) Set(_ string, value string) { sc.value = value }

func (sc *ScalarCarrier) Value() string { return sc.value }
