package stats

import (
	"log/slog"
	"sync"

	s "github.com/laiambryant/telemetry-ingestor/structs"
)

// SendStats tracks success and failure counts for sending telemetry
type SendStats struct {
	mu             sync.Mutex
	TracesSuccess  int
	TracesFailed   int
	LogsSuccess    int
	LogsFailed     int
	MetricsSuccess int
	MetricsFailed  int
}

// RecordSuccess increments the success counter for the given telemetry type
func (ss *SendStats) RecordSuccess(telemetryType s.TelemetryType) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	switch telemetryType {
	case s.TelemetryTraces:
		ss.TracesSuccess++
	case s.TelemetryLogs:
		ss.LogsSuccess++
	case s.TelemetryMetrics:
		ss.MetricsSuccess++
	}
}

// RecordFailure increments the failure counter for the given telemetry type
func (ss *SendStats) RecordFailure(telemetryType s.TelemetryType) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	switch telemetryType {
	case s.TelemetryTraces:
		ss.TracesFailed++
	case s.TelemetryLogs:
		ss.LogsFailed++
	case s.TelemetryMetrics:
		ss.MetricsFailed++
	}
}

// PrintSummary prints a summary of the send statistics
func (s *SendStats) PrintSummary() {
	s.mu.Lock()
	defer s.mu.Unlock()

	slog.Info("=== Telemetry Send Summary ===")
	if s.TracesSuccess > 0 || s.TracesFailed > 0 {
		slog.Info("Traces", "success", s.TracesSuccess, "failed", s.TracesFailed)
	}
	if s.LogsSuccess > 0 || s.LogsFailed > 0 {
		slog.Info("Logs", "success", s.LogsSuccess, "failed", s.LogsFailed)
	}
	if s.MetricsSuccess > 0 || s.MetricsFailed > 0 {
		slog.Info("Metrics", "success", s.MetricsSuccess, "failed", s.MetricsFailed)
	}
	totalSuccess := s.TracesSuccess + s.LogsSuccess + s.MetricsSuccess
	totalFailed := s.TracesFailed + s.LogsFailed + s.MetricsFailed
	slog.Info("Total", "success", totalSuccess, "failed", totalFailed)
}
