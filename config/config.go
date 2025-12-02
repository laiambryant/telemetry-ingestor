package config

const (
	DEFAULT_OTEL_ENDPOINT         = "http://localhost:4318/v1/traces"
	DEFAULT_OTEL_LOGS_ENDPOINT    = "http://localhost:4318/v1/logs"
	DEFAULT_OTEL_METRICS_ENDPOINT = "http://localhost:4318/v1/metrics"
)

// Config holds the configuration for the telemetry ingestion
type Config struct {
	FilePath            string
	OtelEndpoint        string
	OtelLogsEndpoint    string
	OtelMetricsEndpoint string
	MaxBufferCapacity   int
	SendAll             bool
	Workers             int
}

// NewConfig creates a new Config with default values
func NewConfig() *Config {
	return &Config{
		FilePath:            "telemetry.json",
		OtelEndpoint:        DEFAULT_OTEL_ENDPOINT,
		OtelLogsEndpoint:    DEFAULT_OTEL_LOGS_ENDPOINT,
		OtelMetricsEndpoint: DEFAULT_OTEL_METRICS_ENDPOINT,
		MaxBufferCapacity:   1024 * 1024,
		SendAll:             false,
		Workers:             10,
	}
}
