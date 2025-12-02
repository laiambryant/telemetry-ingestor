package structs

// TelemetryType represents the type of telemetry data
type TelemetryType int

const (
	TelemetryTraces TelemetryType = iota
	TelemetryLogs
	TelemetryMetrics
)

func (t TelemetryType) String() string {
	switch t {
	case TelemetryTraces:
		return "Traces"
	case TelemetryLogs:
		return "Logs"
	case TelemetryMetrics:
		return "Metrics"
	default:
		return "Unknown"
	}
}

// TelemetryData represents a map of telemetry data
type TelemetryData map[string]any

// TelemetryJob represents a job to send telemetry data
type TelemetryJob struct {
	Endpoint      string
	Payload       any
	TelemetryType TelemetryType
	LineNum       int
}
