package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// =============================================================================
// No-op Metrics (fallback when metrics are not enabled)
// =============================================================================

// noopCounter is a no-op counter that does nothing.
type noopCounter struct{}

func (noopCounter) Inc()        {}
func (noopCounter) Add(float64) {}

// noopGauge is a no-op gauge that does nothing.
type noopGauge struct{}

func (noopGauge) Inc()              {}
func (noopGauge) Dec()              {}
func (noopGauge) Add(float64)       {}
func (noopGauge) Set(float64)       {}
func (noopGauge) Sub(float64)       {}
func (noopGauge) SetToCurrentTime() {}

// noopHistogram is a no-op histogram that does nothing.
type noopHistogram struct{}

func (noopHistogram) Observe(float64) {}

// noopMetrics returns a Metrics instance where all operations are no-ops.
func noopMetrics() *Metrics {
	return &Metrics{
		// Counters
		EventsProcessedTotal:   noopCounter{},
		ActionsExecutedTotal:   noopCounter{},
		CacheHitsTotal:         noopCounter{},
		CacheMissesTotal:       noopCounter{},
		OutboxPublishedTotal:   noopCounter{},
		OutboxFailuresTotal:    noopCounter{},
		JetStreamConsumedTotal: noopCounter{},
		JetStreamErrorsTotal:   noopCounter{},
		ReaderFallbacksTotal:   noopCounter{},

		// Gauges
		ActiveWorkflows:  noopGauge{},
		PendingDelays:    noopGauge{},
		OutboxQueueDepth: noopGauge{},
		ConsumerLag:      noopGauge{},
		StuckEvents:      noopGauge{},

		// Histograms
		CommandLatencySeconds: noopHistogram{},
		StateLoadTimeSeconds:  noopHistogram{},
		OutboxLatencySeconds:  noopHistogram{},
		OutboxBatchSize:       noopHistogram{},
		ReaderEventLagSeconds: noopHistogram{},
	}
}

// =============================================================================
// Interfaces
// =============================================================================

// Counter wraps Prometheus counter operations for use in the Metrics struct.
type Counter interface {
	Inc()
	Add(float64)
}

// Gauge wraps Prometheus gauge operations for use in the Metrics struct.
type Gauge interface {
	Inc()
	Dec()
	Add(float64)
	Set(float64)
	Sub(float64)
	SetToCurrentTime()
}

// Histogram wraps Prometheus histogram operations for use in the Metrics struct.
type Histogram interface {
	Observe(float64)
}

// =============================================================================
// Metrics
// =============================================================================

// Metrics holds all Prometheus metrics for a single workflow type.
// Simple metrics (only workflow_type label) are accessed directly via fields.
// Multi-label metrics use accessor methods: CommandsProcessed(status), ActionsFailed(errorType).
//
// Use NewMetrics to create a fully-wired instance, or noopMetrics for
// a zero-allocation fallback when metrics collection is disabled.
type Metrics struct {
	// Counters (workflow_type label only)
	EventsProcessedTotal   Counter
	ActionsExecutedTotal   Counter
	CacheHitsTotal         Counter
	CacheMissesTotal       Counter
	OutboxPublishedTotal   Counter
	OutboxFailuresTotal    Counter
	JetStreamConsumedTotal Counter
	JetStreamErrorsTotal   Counter
	ReaderFallbacksTotal   Counter

	// Gauges (workflow_type label only)
	ActiveWorkflows  Gauge
	PendingDelays    Gauge
	OutboxQueueDepth Gauge
	ConsumerLag      Gauge
	StuckEvents      Gauge

	// Histograms (workflow_type label only)
	CommandLatencySeconds Histogram
	StateLoadTimeSeconds  Histogram
	OutboxLatencySeconds  Histogram
	OutboxBatchSize       Histogram
	ReaderEventLagSeconds Histogram

	// Private fields for multi-label collectors.
	// Access via CommandsProcessed(status) and ActionsFailed(errorType).
	commandsProcessedVec *prometheus.CounterVec
	actionsFailedVec     *prometheus.CounterVec
}

// =============================================================================
// Constructors
// =============================================================================

const workflowTypeLabel = "workflow_type"

// NewMetrics creates a Metrics instance for the given workflow type and
// registers all collectors with the provided Prometheus registry.
// If registry is nil, returns a no-op Metrics instance.
func NewMetrics(workflowType string, registry *prometheus.Registry) *Metrics {
	if registry == nil {
		return noopMetrics()
	}

	constLabels := prometheus.Labels{workflowTypeLabel: workflowType}

	commandsProcessedVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "commands_processed_total",
		Help:        "Total number of commands processed.",
		ConstLabels: constLabels,
	}, []string{"status"})

	actionsFailedVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "actions_failed_total",
		Help:        "Total number of actions that failed.",
		ConstLabels: constLabels,
	}, []string{"error_type"})

	m := &Metrics{
		// Counters
		EventsProcessedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "events_processed_total",
			Help:        "Total number of events processed.",
			ConstLabels: constLabels,
		}),
		ActionsExecutedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "actions_executed_total",
			Help:        "Total number of actions executed.",
			ConstLabels: constLabels,
		}),
		CacheHitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "cache_hits_total",
			Help:        "Total number of cache hits.",
			ConstLabels: constLabels,
		}),
		CacheMissesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "cache_misses_total",
			Help:        "Total number of cache misses.",
			ConstLabels: constLabels,
		}),
		OutboxPublishedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "outbox_published_total",
			Help:        "Total number of messages published to the outbox.",
			ConstLabels: constLabels,
		}),
		OutboxFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "outbox_failures_total",
			Help:        "Total number of outbox publish failures.",
			ConstLabels: constLabels,
		}),
		JetStreamConsumedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "jetstream_consumed_total",
			Help:        "Total number of messages consumed from JetStream.",
			ConstLabels: constLabels,
		}),
		JetStreamErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "jetstream_errors_total",
			Help:        "Total number of JetStream consumption errors.",
			ConstLabels: constLabels,
		}),
		ReaderFallbacksTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "reader_fallbacks_total",
			Help:        "Total number of reader fallback events.",
			ConstLabels: constLabels,
		}),

		// Gauges
		ActiveWorkflows: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "active_workflows",
			Help:        "Number of currently active workflows.",
			ConstLabels: constLabels,
		}),
		PendingDelays: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "pending_delays",
			Help:        "Number of pending delayed executions.",
			ConstLabels: constLabels,
		}),
		OutboxQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "outbox_queue_depth",
			Help:        "Current depth of the outbox queue.",
			ConstLabels: constLabels,
		}),
		ConsumerLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "consumer_lag",
			Help:        "Current consumer lag.",
			ConstLabels: constLabels,
		}),
		StuckEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "stuck_events",
			Help:        "Number of events that appear stuck.",
			ConstLabels: constLabels,
		}),

		// Histograms
		CommandLatencySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "command_latency_seconds",
			Help:        "Latency of command processing in seconds.",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}),
		StateLoadTimeSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "state_load_time_seconds",
			Help:        "Time to load workflow state in seconds.",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}),
		OutboxLatencySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "outbox_latency_seconds",
			Help:        "Latency of outbox publish operations in seconds.",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}),
		OutboxBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "outbox_batch_size",
			Help:        "Size of outbox publish batches.",
			ConstLabels: constLabels,
			Buckets:     []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
		ReaderEventLagSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "reader_event_lag_seconds",
			Help:        "Lag between event creation and reader consumption in seconds.",
			ConstLabels: constLabels,
			Buckets:     []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}),

		// Multi-label counter vecs
		commandsProcessedVec: commandsProcessedVec,
		actionsFailedVec:     actionsFailedVec,
	}

	registry.MustRegister(
		// Counters
		m.EventsProcessedTotal.(prometheus.Collector),
		m.ActionsExecutedTotal.(prometheus.Collector),
		m.CacheHitsTotal.(prometheus.Collector),
		m.CacheMissesTotal.(prometheus.Collector),
		m.OutboxPublishedTotal.(prometheus.Collector),
		m.OutboxFailuresTotal.(prometheus.Collector),
		m.JetStreamConsumedTotal.(prometheus.Collector),
		m.JetStreamErrorsTotal.(prometheus.Collector),
		m.ReaderFallbacksTotal.(prometheus.Collector),

		// Multi-label counters
		commandsProcessedVec,
		actionsFailedVec,

		// Gauges
		m.ActiveWorkflows.(prometheus.Collector),
		m.PendingDelays.(prometheus.Collector),
		m.OutboxQueueDepth.(prometheus.Collector),
		m.ConsumerLag.(prometheus.Collector),
		m.StuckEvents.(prometheus.Collector),

		// Histograms
		m.CommandLatencySeconds.(prometheus.Collector),
		m.StateLoadTimeSeconds.(prometheus.Collector),
		m.OutboxLatencySeconds.(prometheus.Collector),
		m.OutboxBatchSize.(prometheus.Collector),
		m.ReaderEventLagSeconds.(prometheus.Collector),
	)

	return m
}

// =============================================================================
// Multi-label Counter Accessors
// =============================================================================

// CommandsProcessed returns a Counter bound to the given status label.
// Labels: workflow_type (const), status (dynamic).
func (m *Metrics) CommandsProcessed(status string) Counter {
	if m.commandsProcessedVec == nil {
		return noopCounter{}
	}
	return m.commandsProcessedVec.WithLabelValues(status)
}

// ActionsFailed returns a Counter bound to the given error_type label.
// Labels: workflow_type (const), error_type (dynamic).
func (m *Metrics) ActionsFailed(errorType string) Counter {
	if m.actionsFailedVec == nil {
		return noopCounter{}
	}
	return m.actionsFailedVec.WithLabelValues(errorType)
}
