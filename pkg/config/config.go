package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

// WorkflowConfig holds all configuration for the fleuve workflow engine.
type WorkflowConfig struct {
	DatabaseURL                string
	NATSURL                    string
	Namespace                  string
	EnableJetStream            bool
	EnableTruncation           bool
	EnableExternalMessaging    bool
	EnableReconciliation       bool
	CreateTables               bool
	SnapshotInterval           int
	MaxInflight                int
	MaxCacheSize               int
	MaxEventsPerSecond         float64
	OutboxBatchSize            int
	TruncationMinRetentionDays int
	OutboxPollInterval         time.Duration
	TrustCache                 bool
	EnableOTel                 bool
	OTelEndpoint               string
}

// boolKeys maps FLEUVE_* suffixes (uppercase) to their TOML key (lowercase)
// for boolean fields.
var boolKeys = map[string]string{
	"ENABLE_TRUNCATION":         "enable_truncation",
	"ENABLE_OTEL":               "enable_otel",
	"ENABLE_JETSTREAM":          "enable_jetstream",
	"ENABLE_EXTERNAL_MESSAGING": "enable_external_messaging",
	"ENABLE_RECONCILIATION":     "enable_reconciliation",
	"CREATE_TABLES":             "create_tables",
	"TRUST_CACHE":               "trust_cache",
}

// intKeys maps FLEUVE_* suffixes (uppercase) to their TOML key (lowercase)
// for integer fields.
var intKeys = map[string]string{
	"SNAPSHOT_INTERVAL":             "snapshot_interval",
	"MAX_INFLIGHT":                  "max_inflight",
	"MAX_CACHE_SIZE":                "max_cache_size",
	"OUTBOX_BATCH_SIZE":             "outbox_batch_size",
	"TRUNCATION_MIN_RETENTION_DAYS": "truncation_min_retention_days",
}

// floatKeys maps FLEUVE_* suffixes (uppercase) to their TOML key (lowercase)
// for float fields.
var floatKeys = map[string]string{
	"MAX_EVENTS_PER_SECOND": "max_events_per_second",
	"OUTBOX_POLL_INTERVAL":  "outbox_poll_interval",
}

// stringKeys maps FLEUVE_* suffixes (uppercase) to their TOML key (lowercase)
// for string fields.
var stringKeys = map[string]string{
	"DATABASE_URL":  "database_url",
	"NATS_URL":      "nats_url",
	"NAMESPACE":     "namespace",
	"OTEL_ENDPOINT": "otel_endpoint",
}

// allKeyMaps is a consolidated lookup for fast env-var-to-TOML-key resolution.
var allKeyMaps = []map[string]string{boolKeys, intKeys, floatKeys, stringKeys}

// tomlKeyType associates a TOML key with its expected type category.
var tomlKeyType = map[string]string{}

func init() {
	for _, m := range allKeyMaps {
		for suffix, tomlKey := range m {
			tomlKeyType[tomlKey] = suffix
		}
	}
}

// load_fleuve_toml locates and parses the fleuve.toml configuration file.
//
// Search order:
//  1. Explicit path argument (non-empty)
//  2. $FLEUVE_CONFIG environment variable
//  3. fleuve.toml in the current working directory
//
// It returns the contents of the [fleuve] table as a map[string]any.
func load_fleuve_toml(path string) (map[string]any, error) {
	if path == "" {
		path = os.Getenv("FLEUVE_CONFIG")
	}
	if path == "" {
		path = "fleuve.toml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %q: %w", path, err)
	}

	var raw map[string]any
	if err := toml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing TOML from %q: %w", path, err)
	}

	fleuveSection, ok := raw["fleuve"]
	if !ok {
		// No [fleuve] table is not an error; return an empty map.
		return make(map[string]any), nil
	}

	sectionMap, ok := fleuveSection.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("[fleuve] section in %q is not a table", path)
	}

	return sectionMap, nil
}

// applyEnvOverrides inspects all FLEUVE_* environment variables and
// overrides corresponding values in the provided map.
func applyEnvOverrides(m map[string]any) {
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "FLEUVE_") {
			continue
		}

		kv, _, found := strings.Cut(env, "=")
		if !found {
			continue
		}

		suffix := strings.TrimPrefix(kv, "FLEUVE_")
		if suffix == "" || suffix == "CONFIG" {
			// Skip FLEUVE_CONFIG itself (used for path resolution).
			continue
		}

		value := os.Getenv(kv)
		if value == "" {
			continue
		}

		if tomlKey, ok := boolKeys[suffix]; ok {
			m[tomlKey] = parseBool(value)
			continue
		}
		if tomlKey, ok := intKeys[suffix]; ok {
			m[tomlKey] = parseInt(value)
			continue
		}
		if tomlKey, ok := floatKeys[suffix]; ok {
			m[tomlKey] = parseFloat(value)
			continue
		}
		if tomlKey, ok := stringKeys[suffix]; ok {
			m[tomlKey] = value
			continue
		}
	}
}

// parseBool converts a string to bool. It accepts 1/t/T/true/TRUE/True
// as true and everything else as false.
func parseBool(s string) bool {
	b, _ := strconv.ParseBool(s)
	return b
}

// parseInt converts a string to int. Returns 0 on failure.
func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

// parseFloat converts a string to float64. Returns 0 on failure.
func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// get_string retrieves a string value from the map, returning the default
// if the key is missing or not a string.
func get_string(m map[string]any, key, defaultVal string) string {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	s, ok := v.(string)
	if !ok {
		return defaultVal
	}
	return s
}

// get_bool retrieves a bool value from the map, returning the default
// if the key is missing or not a bool.
func get_bool(m map[string]any, key string, defaultVal bool) bool {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	b, ok := v.(bool)
	if !ok {
		return defaultVal
	}
	return b
}

// get_int retrieves an int value from the map, returning the default
// if the key is missing. Accepts int, int64, or float64 values.
func get_int(m map[string]any, key string, defaultVal int) int {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return defaultVal
	}
}

// get_float64 retrieves a float64 value from the map, returning the default
// if the key is missing. Accepts float64 or int values.
func get_float64(m map[string]any, key string, defaultVal float64) float64 {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return defaultVal
	}
}

// LoadConfig loads the workflow configuration from a TOML file and applies
// environment variable overrides.
//
// The path argument is optional; when empty, LoadConfig falls back to
// $FLEUVE_CONFIG and then fleuve.toml in the current working directory.
func LoadConfig(path string) (*WorkflowConfig, error) {
	m, err := load_fleuve_toml(path)
	if err != nil {
		return nil, err
	}

	applyEnvOverrides(m)

	cfg := &WorkflowConfig{
		DatabaseURL:                get_string(m, "database_url", ""),
		NATSURL:                    get_string(m, "nats_url", ""),
		Namespace:                  get_string(m, "namespace", ""),
		EnableJetStream:            get_bool(m, "enable_jetstream", false),
		EnableTruncation:           get_bool(m, "enable_truncation", false),
		EnableExternalMessaging:    get_bool(m, "enable_external_messaging", false),
		EnableReconciliation:       get_bool(m, "enable_reconciliation", false),
		CreateTables:               get_bool(m, "create_tables", false),
		SnapshotInterval:           get_int(m, "snapshot_interval", 0),
		MaxInflight:                get_int(m, "max_inflight", 0),
		MaxCacheSize:               get_int(m, "max_cache_size", 0),
		MaxEventsPerSecond:         get_float64(m, "max_events_per_second", 0),
		OutboxBatchSize:            get_int(m, "outbox_batch_size", 0),
		TruncationMinRetentionDays: get_int(m, "truncation_min_retention_days", 0),
		TrustCache:                 get_bool(m, "trust_cache", false),
		EnableOTel:                 get_bool(m, "enable_otel", false),
		OTelEndpoint:               get_string(m, "otel_endpoint", ""),
	}

	// OutboxPollInterval is stored as a float representing seconds in TOML/env.
	outboxSeconds := get_float64(m, "outbox_poll_interval", 0)
	cfg.OutboxPollInterval = time.Duration(outboxSeconds * float64(time.Second))

	return cfg, nil
}
