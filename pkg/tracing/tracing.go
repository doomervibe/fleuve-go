package tracing

import (
	"context"
	"time"
)

type Span struct {
	name      string
	startTime time.Time
	attrs     map[string]interface{}
}

type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs map[string]interface{}) (context.Context, *Span)
	EndSpan(span *Span)
}

type NoopTracer struct{}

func NewNoopTracer() *NoopTracer {
	return &NoopTracer{}
}

func (t *NoopTracer) StartSpan(ctx context.Context, name string, attrs map[string]interface{}) (context.Context, *Span) {
	return ctx, &Span{name: name, startTime: time.Now(), attrs: attrs}
}

func (t *NoopTracer) EndSpan(span *Span) {}

type FleuveTracer struct {
	tracer  Tracer
	enabled bool
}

func NewFleuveTracer(tracer Tracer, enabled bool) *FleuveTracer {
	if tracer == nil {
		tracer = NewNoopTracer()
	}
	return &FleuveTracer{tracer: tracer, enabled: enabled}
}

func (t *FleuveTracer) Span(ctx context.Context, name string, attrs map[string]interface{}) (context.Context, *Span) {
	if !t.enabled {
		return ctx, &Span{name: name, startTime: time.Now(), attrs: attrs}
	}
	return t.tracer.StartSpan(ctx, name, attrs)
}

func (t *FleuveTracer) End(span *Span) {
	if !t.enabled {
		return
	}
	t.tracer.EndSpan(span)
}

type SpanContextKey struct{}

func SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(SpanContextKey{}).(*Span); ok {
		return span
	}
	return nil
}

func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, SpanContextKey{}, span)
}

func WithSpan(ctx context.Context, name string, attrs map[string]interface{}, fn func(context.Context) error, tracer *FleuveTracer) error {
	if tracer == nil {
		tracer = NewFleuveTracer(nil, false)
	}

	ctx, span := tracer.Span(ctx, name, attrs)
	defer tracer.End(span)

	return fn(ContextWithSpan(ctx, span))
}
