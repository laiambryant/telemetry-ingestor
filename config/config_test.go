package config

import (
	"testing"
)

func TestNewConfig(t *testing.T) {
	cfg := NewConfig()
	if cfg.FilePath != "telemetry.json" {
		t.Errorf("Expected FilePath to be 'telemetry.json', got '%s'", cfg.FilePath)
	}
	if cfg.OtelEndpoint != DEFAULT_OTEL_ENDPOINT {
		t.Errorf("Expected OtelEndpoint to be '%s', got '%s'", DEFAULT_OTEL_ENDPOINT, cfg.OtelEndpoint)
	}
	if cfg.OtelLogsEndpoint != DEFAULT_OTEL_LOGS_ENDPOINT {
		t.Errorf("Expected OtelLogsEndpoint to be '%s', got '%s'", DEFAULT_OTEL_LOGS_ENDPOINT, cfg.OtelLogsEndpoint)
	}
	if cfg.OtelMetricsEndpoint != DEFAULT_OTEL_METRICS_ENDPOINT {
		t.Errorf("Expected OtelMetricsEndpoint to be '%s', got '%s'", DEFAULT_OTEL_METRICS_ENDPOINT, cfg.OtelMetricsEndpoint)
	}
	if cfg.MaxBufferCapacity != 1024*1024 {
		t.Errorf("Expected MaxBufferCapacity to be %d, got %d", 1024*1024, cfg.MaxBufferCapacity)
	}
	if cfg.SendAll != false {
		t.Errorf("Expected SendAll to be false, got %v", cfg.SendAll)
	}
	if cfg.Workers != 10 {
		t.Errorf("Expected Workers to be 10, got %d", cfg.Workers)
	}
}
