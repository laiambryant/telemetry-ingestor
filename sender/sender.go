package sender

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/laiambryant/telemetry-ingestor/stats"
	"github.com/laiambryant/telemetry-ingestor/structs"
)

// sendToOTel sends telemetry data to the OpenTelemetry collector
func SendToOTel(endpoint string, payload any, telemetryType structs.TelemetryType, stats *stats.SendStats) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return &JSONMarshalError{Err: err}
	}

	resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		stats.RecordFailure(telemetryType)
		return &HTTPRequestError{Endpoint: endpoint, Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		stats.RecordSuccess(telemetryType)
	} else {
		stats.RecordFailure(telemetryType)
		body, _ := io.ReadAll(resp.Body)
		slog.Error("Failed to send telemetry", "type", telemetryType, "status", resp.StatusCode, "response", string(body))
	}

	return nil
}
