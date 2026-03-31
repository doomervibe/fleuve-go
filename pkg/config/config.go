package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	DatabaseURL                string        `toml:"database_url" env:"FLEUVE_DATABASE_URL"`
	NatsURL                    string        `toml:"nats_url" env:"FLEUVE_NATS_URL"`
	SnapshotInterval           int           `toml:"snapshot_interval" env:"FLEUVE_SNAPSHOT_INTERVAL"`
	EnableTruncation           bool          `toml:"enable_truncation" env:"FLEUVE_ENABLE_TRUNCATION"`
	TruncationMinRetentionDays int           `toml:"truncation_min_retention_days" env:"FLEUVE_TRUNCATION_MIN_RETENTION_DAYS"`
	TruncationBatchSize        int           `toml:"truncation_batch_size" env:"FLEUVE_TRUNCATION_BATCH_SIZE"`
	MaxInflight                int           `toml:"max_inflight" env:"FLEUVE_MAX_INFLIGHT"`
	MaxEventsPerSecond         float64       `toml:"max_events_per_second" env:"FLEUVE_MAX_EVENTS_PER_SECOND"`
	EnableOtel                 bool          `toml:"enable_otel" env:"FLEUVE_ENABLE_OTEL"`
	MaxCacheSize               int           `toml:"max_cache_size" env:"FLEUVE_MAX_CACHE_SIZE"`
	EnableJetstream            bool          `toml:"enable_jetstream" env:"FLEUVE_ENABLE_JETSTREAM"`
	EnableReconciliation       bool          `toml:"enable_reconciliation" env:"FLEUVE_ENABLE_RECONCILIATION"`
	EnableExternalMessaging    bool          `toml:"enable_external_messaging" env:"FLEUVE_ENABLE_EXTERNAL_MESSAGING"`
	CreateTables               bool          `toml:"create_tables" env:"FLEUVE_CREATE_TABLES"`
	OutboxBatchSize            int           `toml:"outbox_batch_size" env:"FLEUVE_OUTBOX_BATCH_SIZE"`
	OutboxPollInterval         time.Duration `toml:"outbox_poll_interval" env:"FLEUVE_OUTBOX_POLL_INTERVAL"`
	UITitle                    string        `toml:"ui_title" env:"FLEUVE_UI_TITLE"`
}

func DefaultConfig() *Config {
	return &Config{
		DatabaseURL:                "",
		NatsURL:                    "nats://localhost:4222",
		SnapshotInterval:           0,
		EnableTruncation:           false,
		TruncationMinRetentionDays: 7,
		TruncationBatchSize:        1000,
		MaxInflight:                4,
		MaxEventsPerSecond:         500.0,
		EnableOtel:                 false,
		MaxCacheSize:               10000,
		EnableJetstream:            false,
		EnableReconciliation:       false,
		EnableExternalMessaging:    false,
		CreateTables:               true,
		OutboxBatchSize:            100,
		OutboxPollInterval:         100 * time.Millisecond,
		UITitle:                    "",
	}
}

func LoadFleuveToml(path string) (*Config, error) {
	cfg := DefaultConfig()

	candidates := []string{path, os.Getenv("FLEUVE_CONFIG"), "fleuve.toml"}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, err := os.Stat(candidate); err != nil { // #nosec G703 G304 -- operator-controlled config path (CLI, env, cwd file)
			continue
		}
		// #nosec G703 G304 -- operator-controlled config path (CLI, env, cwd file)
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		var raw map[string]interface{}
		if err := toml.Unmarshal(data, &raw); err != nil {
			continue
		}
		if fleuve, ok := raw["fleuve"]; ok {
			if fleuveMap, ok := fleuve.(map[string]interface{}); ok {
				if v, ok := fleuveMap["database_url"].(string); ok {
					cfg.DatabaseURL = v
				}
				if v, ok := fleuveMap["nats_url"].(string); ok {
					cfg.NatsURL = v
				}
				if v, ok := fleuveMap["snapshot_interval"].(int64); ok {
					cfg.SnapshotInterval = int(v)
				}
				if v, ok := fleuveMap["enable_truncation"].(bool); ok {
					cfg.EnableTruncation = v
				}
				if v, ok := fleuveMap["truncation_min_retention_days"].(int64); ok {
					cfg.TruncationMinRetentionDays = int(v)
				}
				if v, ok := fleuveMap["truncation_batch_size"].(int64); ok {
					cfg.TruncationBatchSize = int(v)
				}
				if v, ok := fleuveMap["max_inflight"].(int64); ok {
					cfg.MaxInflight = int(v)
				}
				if v, ok := fleuveMap["max_events_per_second"].(float64); ok {
					cfg.MaxEventsPerSecond = v
				}
				if v, ok := fleuveMap["enable_otel"].(bool); ok {
					cfg.EnableOtel = v
				}
				if v, ok := fleuveMap["max_cache_size"].(int64); ok {
					cfg.MaxCacheSize = int(v)
				}
				if v, ok := fleuveMap["enable_jetstream"].(bool); ok {
					cfg.EnableJetstream = v
				}
				if v, ok := fleuveMap["enable_reconciliation"].(bool); ok {
					cfg.EnableReconciliation = v
				}
				if v, ok := fleuveMap["enable_external_messaging"].(bool); ok {
					cfg.EnableExternalMessaging = v
				}
				if v, ok := fleuveMap["create_tables"].(bool); ok {
					cfg.CreateTables = v
				}
				if v, ok := fleuveMap["outbox_batch_size"].(int64); ok {
					cfg.OutboxBatchSize = int(v)
				}
				if v, ok := fleuveMap["outbox_poll_interval"].(string); ok {
					if d, err := time.ParseDuration(v); err == nil {
						cfg.OutboxPollInterval = d
					}
				} else if v, ok := fleuveMap["outbox_poll_interval"].(int64); ok {
					cfg.OutboxPollInterval = time.Duration(v) * time.Millisecond
				} else if v, ok := fleuveMap["outbox_poll_interval"].(float64); ok {
					cfg.OutboxPollInterval = time.Duration(v) * time.Millisecond
				}
				if v, ok := fleuveMap["ui_title"].(string); ok {
					cfg.UITitle = v
				}
			}
		}
		break
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("FLEUVE_DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := os.Getenv("FLEUVE_NATS_URL"); v != "" {
		cfg.NatsURL = v
	}
	if v := os.Getenv("FLEUVE_SNAPSHOT_INTERVAL"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.SnapshotInterval = i
		}
	}
	if v := os.Getenv("FLEUVE_ENABLE_TRUNCATION"); v != "" {
		cfg.EnableTruncation = parseBool(v)
	}
	if v := os.Getenv("FLEUVE_TRUNCATION_MIN_RETENTION_DAYS"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.TruncationMinRetentionDays = i
		}
	}
	if v := os.Getenv("FLEUVE_TRUNCATION_BATCH_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.TruncationBatchSize = i
		}
	}
	if v := os.Getenv("FLEUVE_MAX_INFLIGHT"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.MaxInflight = i
		}
	}
	if v := os.Getenv("FLEUVE_MAX_EVENTS_PER_SECOND"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.MaxEventsPerSecond = f
		}
	}
	if v := os.Getenv("FLEUVE_ENABLE_OTEL"); v != "" {
		cfg.EnableOtel = parseBool(v)
	}
	if v := os.Getenv("FLEUVE_MAX_CACHE_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.MaxCacheSize = i
		}
	}
	if v := os.Getenv("FLEUVE_ENABLE_JETSTREAM"); v != "" {
		cfg.EnableJetstream = parseBool(v)
	}
	if v := os.Getenv("FLEUVE_ENABLE_RECONCILIATION"); v != "" {
		cfg.EnableReconciliation = parseBool(v)
	}
	if v := os.Getenv("FLEUVE_ENABLE_EXTERNAL_MESSAGING"); v != "" {
		cfg.EnableExternalMessaging = parseBool(v)
	}
	if v := os.Getenv("FLEUVE_CREATE_TABLES"); v != "" {
		cfg.CreateTables = parseBool(v)
	}
	if v := os.Getenv("FLEUVE_OUTBOX_BATCH_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.OutboxBatchSize = i
		}
	}
	if v := os.Getenv("FLEUVE_OUTBOX_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.OutboxPollInterval = d
		}
	}
	if v := os.Getenv("FLEUVE_UI_TITLE"); v != "" {
		cfg.UITitle = v
	}
}

func parseBool(s string) bool {
	s = strings.ToLower(s)
	return s == "1" || s == "true" || s == "yes"
}
