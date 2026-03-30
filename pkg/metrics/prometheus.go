package metrics

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// FleuveMetrics holds all Prometheus metrics for Fleuve
type FleuveMetrics struct {
	// Counters
	EventsProcessedTotal      *prometheus.CounterVec
	CommandsProcessedTotal    *prometheus.CounterVec
	ActivitiesCompletedTotal  *prometheus.CounterVec
	ActivitiesFailedTotal     *prometheus.CounterVec
	CacheHitsTotal            *prometheus.CounterVec
	CacheMissesTotal          *prometheus.CounterVec
	CommandLatencySeconds     *prometheus.HistogramVec
	ActivityExecutionDuration *prometheus.HistogramVec

	// Gauges
	ActivitiesActive      *prometheus.GaugeVec
	WorkflowsActive       *prometheus.GaugeVec
	DBConnectionsActive   prometheus.Gauge
	NATSConnectionsActive prometheus.Gauge
	DelaysPending         *prometheus.GaugeVec
	ReaderLagEvents       *prometheus.GaugeVec
	InflightEvents        *prometheus.GaugeVec

	// Registry
	registry *prometheus.Registry
	mu       sync.Mutex
}

var (
	globalMetrics *FleuveMetrics
	once          sync.Once
)

// NewFleuveMetrics creates a new FleuveMetrics instance with all metrics registered
func NewFleuveMetrics() *FleuveMetrics {
	registry := prometheus.NewRegistry()

	metrics := &FleuveMetrics{
		registry: registry,

		// Events processed counter
		EventsProcessedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fleuve_events_processed_total",
				Help: "Total number of events processed",
			},
			[]string{"workflow_type", "event_type", "status"},
		),

		// Commands processed counter
		CommandsProcessedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fleuve_commands_processed_total",
				Help: "Total number of commands processed",
			},
			[]string{"workflow_type", "command_type", "status"},
		),

		// Activities completed counter
		ActivitiesCompletedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fleuve_activities_completed_total",
				Help: "Total number of activities completed successfully",
			},
			[]string{"workflow_type", "activity_type"},
		),

		// Activities failed counter
		ActivitiesFailedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fleuve_activities_failed_total",
				Help: "Total number of activities that failed",
			},
			[]string{"workflow_type", "activity_type", "error_type"},
		),

		// Cache hits counter
		CacheHitsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fleuve_cache_hits_total",
				Help: "Total number of cache hits",
			},
			[]string{"cache_type"},
		),

		// Cache misses counter
		CacheMissesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fleuve_cache_misses_total",
				Help: "Total number of cache misses",
			},
			[]string{"cache_type"},
		),

		// Command latency histogram
		CommandLatencySeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "fleuve_command_latency_seconds",
				Help:    "Latency of command processing in seconds",
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"workflow_type", "command_type"},
		),

		// Activity execution duration histogram
		ActivityExecutionDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "fleuve_activity_execution_duration_seconds",
				Help:    "Duration of activity execution in seconds",
				Buckets: []float64{.01, .05, .1, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
			},
			[]string{"workflow_type", "activity_type", "status"},
		),

		// Activities active gauge
		ActivitiesActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "fleuve_activities_active",
				Help: "Number of currently active activities",
			},
			[]string{"workflow_type", "activity_type", "status"},
		),

		// Workflows active gauge
		WorkflowsActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "fleuve_workflows_active",
				Help: "Number of currently active workflows",
			},
			[]string{"workflow_type", "status"},
		),

		// DB connections active gauge
		DBConnectionsActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "fleuve_db_connections_active",
				Help: "Number of active database connections",
			},
		),

		// NATS connections active gauge
		NATSConnectionsActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "fleuve_nats_connections_active",
				Help: "Number of active NATS connections",
			},
		),

		// Delays pending gauge
		DelaysPending: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "fleuve_delays_pending",
				Help: "Number of pending delays",
			},
			[]string{"workflow_type"},
		),

		// Reader lag events gauge
		ReaderLagEvents: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "fleuve_reader_lag_events",
				Help: "Number of events the reader is lagging behind",
			},
			[]string{"reader_name", "workflow_type"},
		),

		// Inflight events gauge
		InflightEvents: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "fleuve_inflight_events",
				Help: "Number of events currently in flight",
			},
			[]string{"workflow_type", "reader_name"},
		),
	}

	// Register all metrics
	registry.MustRegister(
		metrics.EventsProcessedTotal,
		metrics.CommandsProcessedTotal,
		metrics.ActivitiesCompletedTotal,
		metrics.ActivitiesFailedTotal,
		metrics.CacheHitsTotal,
		metrics.CacheMissesTotal,
		metrics.CommandLatencySeconds,
		metrics.ActivityExecutionDuration,
		metrics.ActivitiesActive,
		metrics.WorkflowsActive,
		metrics.DBConnectionsActive,
		metrics.NATSConnectionsActive,
		metrics.DelaysPending,
		metrics.ReaderLagEvents,
		metrics.InflightEvents,
	)

	return metrics
}

// InitializeGlobalMetrics initializes the global metrics instance (singleton)
func InitializeGlobalMetrics() *FleuveMetrics {
	once.Do(func() {
		globalMetrics = NewFleuveMetrics()
	})
	return globalMetrics
}

// GetGlobalMetrics returns the global metrics instance
func GetGlobalMetrics() *FleuveMetrics {
	if globalMetrics == nil {
		return InitializeGlobalMetrics()
	}
	return globalMetrics
}

// Handler returns an HTTP handler for the /metrics endpoint
func (m *FleuveMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RegisterMetricsEndpoint registers the /metrics endpoint on the given mux
func (m *FleuveMetrics) RegisterMetricsEndpoint(mux *http.ServeMux) {
	mux.Handle("/metrics", m.Handler())
}

// RecordEventProcessed increments the events processed counter
func (m *FleuveMetrics) RecordEventProcessed(workflowType, eventType, status string) {
	m.EventsProcessedTotal.WithLabelValues(workflowType, eventType, status).Inc()
}

// RecordCommandProcessed increments the commands processed counter
func (m *FleuveMetrics) RecordCommandProcessed(workflowType, commandType, status string) {
	m.CommandsProcessedTotal.WithLabelValues(workflowType, commandType, status).Inc()
}

// RecordCommandLatency records command processing latency
func (m *FleuveMetrics) RecordCommandLatency(workflowType, commandType string, duration time.Duration) {
	m.CommandLatencySeconds.WithLabelValues(workflowType, commandType).Observe(duration.Seconds())
}

// ObserveCommandLatency is a convenience method that records latency from a start time
func (m *FleuveMetrics) ObserveCommandLatency(workflowType, commandType string, startTime time.Time) {
	duration := time.Since(startTime)
	m.RecordCommandLatency(workflowType, commandType, duration)
}

// RecordActivityCompleted increments the activities completed counter
func (m *FleuveMetrics) RecordActivityCompleted(workflowType, activityType string) {
	m.ActivitiesCompletedTotal.WithLabelValues(workflowType, activityType).Inc()
}

// RecordActivityFailed increments the activities failed counter
func (m *FleuveMetrics) RecordActivityFailed(workflowType, activityType, errorType string) {
	m.ActivitiesFailedTotal.WithLabelValues(workflowType, activityType, errorType).Inc()
}

// RecordActivityDuration records activity execution duration
func (m *FleuveMetrics) RecordActivityDuration(workflowType, activityType, status string, duration time.Duration) {
	m.ActivityExecutionDuration.WithLabelValues(workflowType, activityType, status).Observe(duration.Seconds())
}

// SetActivitiesActive sets the number of active activities
func (m *FleuveMetrics) SetActivitiesActive(workflowType, activityType, status string, count float64) {
	m.ActivitiesActive.WithLabelValues(workflowType, activityType, status).Set(count)
}

// IncActivitiesActive increments the number of active activities
func (m *FleuveMetrics) IncActivitiesActive(workflowType, activityType, status string) {
	m.ActivitiesActive.WithLabelValues(workflowType, activityType, status).Inc()
}

// DecActivitiesActive decrements the number of active activities
func (m *FleuveMetrics) DecActivitiesActive(workflowType, activityType, status string) {
	m.ActivitiesActive.WithLabelValues(workflowType, activityType, status).Dec()
}

// SetWorkflowsActive sets the number of active workflows
func (m *FleuveMetrics) SetWorkflowsActive(workflowType, status string, count float64) {
	m.WorkflowsActive.WithLabelValues(workflowType, status).Set(count)
}

// IncWorkflowsActive increments the number of active workflows
func (m *FleuveMetrics) IncWorkflowsActive(workflowType, status string) {
	m.WorkflowsActive.WithLabelValues(workflowType, status).Inc()
}

// DecWorkflowsActive decrements the number of active workflows
func (m *FleuveMetrics) DecWorkflowsActive(workflowType, status string) {
	m.WorkflowsActive.WithLabelValues(workflowType, status).Dec()
}

// RecordCacheHit increments the cache hits counter
func (m *FleuveMetrics) RecordCacheHit(cacheType string) {
	m.CacheHitsTotal.WithLabelValues(cacheType).Inc()
}

// RecordCacheMiss increments the cache misses counter
func (m *FleuveMetrics) RecordCacheMiss(cacheType string) {
	m.CacheMissesTotal.WithLabelValues(cacheType).Inc()
}

// SetDBConnectionsActive sets the number of active database connections
func (m *FleuveMetrics) SetDBConnectionsActive(count float64) {
	m.DBConnectionsActive.Set(count)
}

// SetNATSConnectionsActive sets the number of active NATS connections
func (m *FleuveMetrics) SetNATSConnectionsActive(count float64) {
	m.NATSConnectionsActive.Set(count)
}

// SetDelaysPending sets the number of pending delays
func (m *FleuveMetrics) SetDelaysPending(workflowType string, count float64) {
	m.DelaysPending.WithLabelValues(workflowType).Set(count)
}

// IncDelaysPending increments the number of pending delays
func (m *FleuveMetrics) IncDelaysPending(workflowType string) {
	m.DelaysPending.WithLabelValues(workflowType).Inc()
}

// DecDelaysPending decrements the number of pending delays
func (m *FleuveMetrics) DecDelaysPending(workflowType string) {
	m.DelaysPending.WithLabelValues(workflowType).Dec()
}

// SetReaderLag sets the reader lag in events
func (m *FleuveMetrics) SetReaderLag(readerName, workflowType string, lag float64) {
	m.ReaderLagEvents.WithLabelValues(readerName, workflowType).Set(lag)
}

// SetInflightEvents sets the number of in-flight events
func (m *FleuveMetrics) SetInflightEvents(workflowType, readerName string, count float64) {
	m.InflightEvents.WithLabelValues(workflowType, readerName).Set(count)
}

// IncInflightEvents increments the number of in-flight events
func (m *FleuveMetrics) IncInflightEvents(workflowType, readerName string) {
	m.InflightEvents.WithLabelValues(workflowType, readerName).Inc()
}

// DecInflightEvents decrements the number of in-flight events
func (m *FleuveMetrics) DecInflightEvents(workflowType, readerName string) {
	m.InflightEvents.WithLabelValues(workflowType, readerName).Dec()
}

// WithCommandMetrics wraps a command processing function with metrics collection
func (m *FleuveMetrics) WithCommandMetrics(workflowType, commandType string, fn func() error) error {
	startTime := time.Now()
	err := fn()
	duration := time.Since(startTime)

	status := "success"
	if err != nil {
		status = "error"
	}

	m.RecordCommandProcessed(workflowType, commandType, status)
	m.RecordCommandLatency(workflowType, commandType, duration)

	return err
}

// WithActivityMetrics wraps an activity execution function with metrics collection
func (m *FleuveMetrics) WithActivityMetrics(workflowType, activityType string, fn func() error) error {
	startTime := time.Now()

	m.IncActivitiesActive(workflowType, activityType, "running")
	defer m.DecActivitiesActive(workflowType, activityType, "running")

	err := fn()
	duration := time.Since(startTime)

	if err != nil {
		m.RecordActivityFailed(workflowType, activityType, getErrorType(err))
		m.RecordActivityDuration(workflowType, activityType, "failed", duration)
		return err
	}

	m.RecordActivityCompleted(workflowType, activityType)
	m.RecordActivityDuration(workflowType, activityType, "success", duration)
	return nil
}

// getErrorType extracts error type from an error
func getErrorType(err error) string {
	if err == nil {
		return "none"
	}

	// Simple error classification
	errStr := err.Error()
	switch {
	case contains(errStr, "timeout"):
		return "timeout"
	case contains(errStr, "connection"):
		return "connection_error"
	case contains(errStr, "not found"):
		return "not_found"
	case contains(errStr, "unauthorized"):
		return "unauthorized"
	case contains(errStr, "validation"):
		return "validation_error"
	default:
		return "unknown"
	}
}

// contains checks if a string contains a substring (case-insensitive)
func contains(str, substr string) bool {
	return len(str) >= len(substr) &&
		(str == substr || len(str) > 0 && containsHelper(str, substr))
}

func containsHelper(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// StartMetricsServer starts an HTTP server with metrics endpoint
func StartMetricsServer(addr string) error {
	metrics := GetGlobalMetrics()

	mux := http.NewServeMux()
	metrics.RegisterMetricsEndpoint(mux)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Printf("Starting metrics server on %s", addr)
	return server.ListenAndServe()
}

// MetricsTimer helps track duration of operations
type MetricsTimer struct {
	startTime time.Time
	metrics   *FleuveMetrics
}

// NewMetricsTimer creates a new metrics timer
func NewMetricsTimer() *MetricsTimer {
	return &MetricsTimer{
		startTime: time.Now(),
		metrics:   GetGlobalMetrics(),
	}
}

// Duration returns the elapsed time since the timer was created
func (t *MetricsTimer) Duration() time.Duration {
	return time.Since(t.startTime)
}

// RecordCommand records command metrics with the timer
func (t *MetricsTimer) RecordCommand(workflowType, commandType, status string) {
	t.metrics.RecordCommandProcessed(workflowType, commandType, status)
	t.metrics.RecordCommandLatency(workflowType, commandType, t.Duration())
}

// ContextKey for metrics in context
type ContextKey struct{}

// TimerFromContext retrieves a metrics timer from context
func TimerFromContext(ctx context.Context) *MetricsTimer {
	if timer, ok := ctx.Value(ContextKey{}).(*MetricsTimer); ok {
		return timer
	}
	return nil
}

// ContextWithTimer adds a metrics timer to context
func ContextWithTimer(ctx context.Context, timer *MetricsTimer) context.Context {
	return context.WithValue(ctx, ContextKey{}, timer)
}
