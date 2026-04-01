// Package tracing provides an OpenTelemetry wrapper for Fleuve workflow tracing.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// FleuveTracer wraps an OpenTelemetry tracer with Fleuve-specific configuration.
type FleuveTracer struct {
	tracer       trace.Tracer
	workflowType string
	enabled      bool
}

// SpanWrapper provides a simplified interface for working with OpenTelemetry spans.
type SpanWrapper struct {
	span    trace.Span
	enabled bool
}

// NewFleuveTracer creates a new FleuveTracer instance.
// If enabled is true, it creates an OTel tracer named "fleuve".
// If disabled, it uses a no-op tracer.
func NewFleuveTracer(workflowType string, enabled bool) *FleuveTracer {
	var t trace.Tracer
	if enabled {
		t = otel.Tracer("fleuve")
	} else {
		t = noop.NewTracerProvider().Tracer("fleuve-noop")
	}

	return &FleuveTracer{
		tracer:       t,
		workflowType: workflowType,
		enabled:      enabled,
	}
}

// Enabled returns whether tracing is enabled for this tracer.
func (ft *FleuveTracer) Enabled() bool {
	return ft.enabled
}

// StartSpan creates a new span with the given name and attributes.
// The fleuve.workflow_type attribute is automatically set on all spans.
// Returns a context with the span embedded and a SpanWrapper for span operations.
func (ft *FleuveTracer) StartSpan(ctx context.Context, name string, attributes map[string]string) (context.Context, SpanWrapper) {
	if !ft.enabled {
		return ctx, SpanWrapper{enabled: false}
	}

	attrs := make([]attribute.KeyValue, 0, len(attributes)+1)
	attrs = append(attrs, attribute.String("fleuve.workflow_type", ft.workflowType))

	for k, v := range attributes {
		attrs = append(attrs, attribute.String(k, v))
	}

	ctx, span := ft.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, SpanWrapper{span: span, enabled: true}
}

// Span creates a span without a parent context (uses background context).
// This is a convenience method for cases where context propagation is not needed.
func (ft *FleuveTracer) Span(name string, attributes map[string]string) SpanWrapper {
	_, sw := ft.StartSpan(context.Background(), name, attributes)
	return sw
}

// End completes the span. This is a no-op if tracing is disabled.
func (sw SpanWrapper) End() {
	if sw.enabled && sw.span != nil {
		sw.span.End()
	}
}

// RecordError records an error as an exception on the span with ERROR status.
// This is a no-op if tracing is disabled.
func (sw SpanWrapper) RecordError(err error) {
	if !sw.enabled || sw.span == nil || err == nil {
		return
	}

	sw.span.RecordError(err)
	sw.span.SetStatus(codes.Error, err.Error())
}

// SetAttributes sets the provided attributes on the span.
// This is a no-op if tracing is disabled.
func (sw SpanWrapper) SetAttributes(attrs map[string]string) {
	if !sw.enabled || sw.span == nil {
		return
	}

	kvAttrs := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kvAttrs = append(kvAttrs, attribute.String(k, v))
	}

	sw.span.SetAttributes(kvAttrs...)
}

// AddEvent adds an event with the given name and attributes to the span.
// This is a no-op if tracing is disabled.
func (sw SpanWrapper) AddEvent(name string, attrs map[string]string) {
	if !sw.enabled || sw.span == nil {
		return
	}

	kvAttrs := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kvAttrs = append(kvAttrs, attribute.String(k, v))
	}

	sw.span.AddEvent(name, trace.WithAttributes(kvAttrs...))
}

// SpanFromContext extracts a SpanWrapper from the context if one exists.
// Returns a no-op SpanWrapper if no span is found in the context.
func SpanFromContext(ctx context.Context) SpanWrapper {
	span := trace.SpanFromContext(ctx)
	if span == nil || !span.IsRecording() {
		return SpanWrapper{enabled: false}
	}
	return SpanWrapper{span: span, enabled: true}
}

// =============================================================================
// Predefined span helpers
// =============================================================================

// ProcessCommandSpan creates a span for processing a command.
// Attributes: workflow_id, command_type
func (ft *FleuveTracer) ProcessCommandSpan(ctx context.Context, workflowID, commandType string) (context.Context, SpanWrapper) {
	return ft.StartSpan(ctx, "process_command", map[string]string{
		"workflow_id":  workflowID,
		"command_type": commandType,
	})
}

// LoadStateSpan creates a span for loading workflow state.
// Attributes: workflow_id, at_version
func (ft *FleuveTracer) LoadStateSpan(ctx context.Context, workflowID, atVersion string) (context.Context, SpanWrapper) {
	return ft.StartSpan(ctx, "load_state", map[string]string{
		"workflow_id": workflowID,
		"at_version":  atVersion,
	})
}

// ExecuteActionSpan creates a span for executing an action.
// Attributes: workflow_id, event_number
func (ft *FleuveTracer) ExecuteActionSpan(ctx context.Context, workflowID string, eventNumber int) (context.Context, SpanWrapper) {
	return ft.StartSpan(ctx, "execute_action", map[string]string{
		"workflow_id":  workflowID,
		"event_number": itoa(eventNumber),
	})
}

// itoa converts an int to a string without using fmt.Sprintf for performance.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}

	neg := false
	if i < 0 {
		neg = true
		i = -i
	}

	var buf [20]byte
	pos := len(buf)

	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}

	if neg {
		pos--
		buf[pos] = '-'
	}

	return string(buf[pos:])
}
