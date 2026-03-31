package tracing

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	// Span names for Fleuve operations
	SpanCommandProcess  = "fleuve.command.process"
	SpanEventEvolve     = "fleuve.event.evolve"
	SpanActivityExecute = "fleuve.activity.execute"
	SpanStreamConsume   = "fleuve.stream.consume"
	SpanDBOperation     = "fleuve.db.operation"
	SpanCacheOperation  = "fleuve.cache.operation"
	SpanNATSPublish     = "fleuve.nats.publish"
	SpanWorkflowDecide  = "fleuve.workflow.decide"
	SpanExternalAdapter = "fleuve.external.adapter"
)

// OTelConfig holds OpenTelemetry configuration
type OTelConfig struct {
	Enabled     bool
	Endpoint    string
	ServiceName string
	Environment string
	SampleRate  float64
	Insecure    bool
	Protocol    string // "grpc" or "http"
}

// OTelTracer wraps OpenTelemetry tracer with Fleuve-specific functionality
type OTelTracer struct {
	tracer      trace.Tracer
	provider    *sdktrace.TracerProvider
	enabled     bool
	serviceName string
	mu          sync.Mutex
}

var (
	globalTracer *OTelTracer
	once         sync.Once
)

// LoadOTelConfigFromEnv loads OpenTelemetry configuration from environment variables
func LoadOTelConfigFromEnv() OTelConfig {
	return OTelConfig{
		Enabled:     getEnvBool("FLEUVE_OTEL_ENABLED", false),
		Endpoint:    getEnvString("FLEUVE_OTEL_ENDPOINT", "localhost:4317"),
		ServiceName: getEnvString("FLEUVE_OTEL_SERVICE_NAME", "fleuve"),
		Environment: getEnvString("FLEUVE_ENVIRONMENT", "development"),
		SampleRate:  getEnvFloat("FLEUVE_OTEL_SAMPLE_RATE", 1.0),
		Insecure:    getEnvBool("FLEUVE_OTEL_INSECURE", true),
		Protocol:    getEnvString("FLEUVE_OTEL_PROTOCOL", "grpc"),
	}
}

// OTelConfigFromFleuve merges fleuve.toml (FLEUVE_ENABLE_OTEL) with FLEUVE_OTEL_* environment variables.
// Either enable_otel in config or FLEUVE_OTEL_ENABLED=true turns tracing on.
func OTelConfigFromFleuve(cfg *config.Config) OTelConfig {
	o := LoadOTelConfigFromEnv()
	if cfg != nil && cfg.EnableOtel {
		o.Enabled = true
	}
	return o
}

// NewOTelTracer creates a new OpenTelemetry tracer
func NewOTelTracer(cfg OTelConfig) (*OTelTracer, error) {
	if !cfg.Enabled {
		return &OTelTracer{
			tracer:      noop.NewTracerProvider().Tracer("noop"),
			enabled:     false,
			serviceName: cfg.ServiceName,
		}, nil
	}

	// Create OTLP exporter
	var exporter sdktrace.SpanExporter
	var err error

	ctx := context.Background()

	if cfg.Protocol == "http" {
		// HTTP exporter
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(cfg.Endpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exporter, err = otlptrace.New(ctx, otlptracehttp.NewClient(opts...))
	} else {
		// gRPC exporter (default)
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptrace.New(ctx, otlptracegrpc.NewClient(opts...))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service information
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"),
			semconv.DeploymentEnvironment(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider with sampling
	var sampler sdktrace.Sampler
	if cfg.SampleRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if cfg.SampleRate <= 0.0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRate)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set global tracer provider
	otel.SetTracerProvider(provider)

	// Set global propagator for trace context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := provider.Tracer(
		cfg.ServiceName,
		trace.WithInstrumentationVersion("1.0.0"),
	)

	return &OTelTracer{
		tracer:      tracer,
		provider:    provider,
		enabled:     true,
		serviceName: cfg.ServiceName,
	}, nil
}

// InitializeGlobalTracer initializes the global tracer (singleton)
func InitializeGlobalTracer(cfg OTelConfig) (*OTelTracer, error) {
	var initErr error
	once.Do(func() {
		globalTracer, initErr = NewOTelTracer(cfg)
		if initErr != nil {
			log.Printf("Failed to initialize OpenTelemetry tracer: %v", initErr)
		}
	})
	return globalTracer, initErr
}

// GetGlobalTracer returns the global tracer
func GetGlobalTracer() *OTelTracer {
	if globalTracer == nil {
		// Return noop tracer if not initialized
		return &OTelTracer{
			tracer:  noop.NewTracerProvider().Tracer("noop"),
			enabled: false,
		}
	}
	return globalTracer
}

// StartSpan starts a new span with the given name and attributes
func (t *OTelTracer) StartSpan(ctx context.Context, name string, attrs map[string]interface{}) (context.Context, *OtelSpan) {
	if !t.enabled {
		return ctx, &OtelSpan{enabled: false}
	}

	// Convert attributes to OpenTelemetry format
	otelAttrs := attrsToOtel(attrs)

	ctx, span := t.tracer.Start(
		ctx,
		name,
		trace.WithAttributes(otelAttrs...),
	)

	return ctx, &OtelSpan{
		span:    span,
		enabled: true,
	}
}

// Shutdown gracefully shuts down the tracer provider
func (t *OTelTracer) Shutdown(ctx context.Context) error {
	if !t.enabled || t.provider == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return t.provider.Shutdown(ctx)
}

// IsEnabled returns whether tracing is enabled
func (t *OTelTracer) IsEnabled() bool {
	return t.enabled
}

// OtelSpan wraps an OpenTelemetry span
type OtelSpan struct {
	span    trace.Span
	enabled bool
}

// End ends the span
func (s *OtelSpan) End() {
	if s.enabled && s.span != nil {
		s.span.End()
	}
}

// SetAttributes sets attributes on the span
func (s *OtelSpan) SetAttributes(attrs map[string]interface{}) {
	if s.enabled && s.span != nil {
		s.span.SetAttributes(attrsToOtel(attrs)...)
	}
}

// SetError marks the span as failed with an error
func (s *OtelSpan) SetError(err error) {
	if s.enabled && s.span != nil {
		s.span.RecordError(err)
		s.span.SetAttributes(
			attribute.Bool("error", true),
		)
	}
}

// SetStatus sets the span status
func (s *OtelSpan) SetStatus(code int32, description string) {
	if s.enabled && s.span != nil {
		var statusCode codes.Code
		switch code {
		case 0:
			statusCode = codes.Unset
		case 1:
			statusCode = codes.Ok
		default:
			statusCode = codes.Error
		}
		s.span.SetStatus(statusCode, description)
	}
}

// AddEvent adds an event to the span
func (s *OtelSpan) AddEvent(name string, attrs map[string]interface{}) {
	if s.enabled && s.span != nil {
		s.span.AddEvent(name, trace.WithAttributes(attrsToOtel(attrs)...))
	}
}

// IsRecording returns whether the span is recording
func (s *OtelSpan) IsRecording() bool {
	if !s.enabled || s.span == nil {
		return false
	}
	return s.span.IsRecording()
}

// SpanContext returns the span context
func (s *OtelSpan) SpanContext() trace.SpanContext {
	if !s.enabled || s.span == nil {
		return trace.SpanContext{}
	}
	return s.span.SpanContext()
}

// Helper functions

// attrsToOtel converts a map to OpenTelemetry attributes
func attrsToOtel(attrs map[string]interface{}) []attribute.KeyValue {
	if attrs == nil {
		return nil
	}

	result := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		key := attribute.Key(k)
		var attr attribute.KeyValue

		switch val := v.(type) {
		case string:
			attr = key.String(val)
		case int:
			attr = key.Int(val)
		case int64:
			attr = key.Int64(val)
		case float64:
			attr = key.Float64(val)
		case bool:
			attr = key.Bool(val)
		case []string:
			attr = key.StringSlice(val)
		case []int:
			attr = key.IntSlice(val)
		case []int64:
			attr = key.Int64Slice(val)
		case []float64:
			attr = key.Float64Slice(val)
		case []bool:
			attr = key.BoolSlice(val)
		default:
			attr = key.String(fmt.Sprintf("%v", val))
		}

		result = append(result, attr)
	}

	return result
}

// getEnvString gets an environment variable with a default value
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool gets a boolean environment variable with a default value
func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return strings.ToLower(value) == "true" || value == "1"
}

// getEnvFloat gets a float environment variable with a default value
func getEnvFloat(key string, defaultValue float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	var result float64
	_, err := fmt.Sscanf(value, "%f", &result)
	if err != nil {
		return defaultValue
	}
	return result
}

// Convenience functions for creating spans with common attributes

// StartCommandSpan starts a span for command processing
func StartCommandSpan(ctx context.Context, workflowType, commandType, workflowID string) (context.Context, *OtelSpan) {
	tracer := GetGlobalTracer()
	return tracer.StartSpan(ctx, SpanCommandProcess, map[string]interface{}{
		"workflow.type": workflowType,
		"command.type":  commandType,
		"workflow.id":   workflowID,
		"component":     "gateway",
	})
}

// StartEventSpan starts a span for event evolution
func StartEventSpan(ctx context.Context, workflowType, eventType, workflowID string, version int64) (context.Context, *OtelSpan) {
	tracer := GetGlobalTracer()
	return tracer.StartSpan(ctx, SpanEventEvolve, map[string]interface{}{
		"workflow.type":    workflowType,
		"event.type":       eventType,
		"workflow.id":      workflowID,
		"workflow.version": version,
		"component":        "repo",
	})
}

// StartActivitySpan starts a span for activity execution
func StartActivitySpan(ctx context.Context, activityType, workflowID string, attempt int) (context.Context, *OtelSpan) {
	tracer := GetGlobalTracer()
	return tracer.StartSpan(ctx, SpanActivityExecute, map[string]interface{}{
		"activity.type": activityType,
		"workflow.id":   workflowID,
		"attempt":       attempt,
		"component":     "actions",
	})
}

// StartStreamSpan starts a span for stream consumption
func StartStreamSpan(ctx context.Context, readerName, source string) (context.Context, *OtelSpan) {
	tracer := GetGlobalTracer()
	return tracer.StartSpan(ctx, SpanStreamConsume, map[string]interface{}{
		"reader.name": readerName,
		"source":      source,
		"component":   "stream",
	})
}

// StartDBSpan starts a span for database operations
func StartDBSpan(ctx context.Context, operation, table string) (context.Context, *OtelSpan) {
	tracer := GetGlobalTracer()
	return tracer.StartSpan(ctx, SpanDBOperation, map[string]interface{}{
		"db.operation": operation,
		"db.table":     table,
		"db.system":    "postgresql",
		"component":    "repo",
	})
}

// StartCacheSpan starts a span for cache operations
func StartCacheSpan(ctx context.Context, operation string, hit bool) (context.Context, *OtelSpan) {
	tracer := GetGlobalTracer()
	return tracer.StartSpan(ctx, SpanCacheOperation, map[string]interface{}{
		"cache.operation": operation,
		"cache.hit":       hit,
		"component":       "storage",
	})
}

// StartNATSSpan starts a span for NATS operations
func StartNATSSpan(ctx context.Context, subject string) (context.Context, *OtelSpan) {
	tracer := GetGlobalTracer()
	return tracer.StartSpan(ctx, SpanNATSPublish, map[string]interface{}{
		"messaging.system":      "nats",
		"messaging.destination": subject,
		"component":             "stream",
	})
}

// WithSpan executes a function within a span
func WithOTelSpan(ctx context.Context, name string, attrs map[string]interface{}, fn func(context.Context) error) error {
	tracer := GetGlobalTracer()
	ctx, span := tracer.StartSpan(ctx, name, attrs)
	defer span.End()

	if err := fn(ctx); err != nil {
		span.SetError(err)
		return err
	}

	return nil
}
