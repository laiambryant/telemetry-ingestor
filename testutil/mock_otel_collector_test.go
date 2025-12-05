package testutil

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

var (
	tracesData = map[string]any{
		"resourceSpans": []any{
			map[string]any{"resource": map[string]any{"attributes": []any{}}},
		},
	}
	logsData = map[string]any{
		"resourceLogs": []any{
			map[string]any{"resource": map[string]any{"attributes": []any{}}},
		},
	}
	metricsData = map[string]any{
		"resourceMetrics": []any{
			map[string]any{"resource": map[string]any{"attributes": []any{}}},
		},
	}
)

func TestMockOTelCollector(t *testing.T) {
	mock := NewMockOTelCollector()
	defer mock.Close()
	sendRequest(t, mock.TracesURL(), tracesData)
	sendRequest(t, mock.LogsURL(), logsData)
	sendRequest(t, mock.MetricsURL(), metricsData)
	traces, logs, metrics, total := mock.GetStats()
	if traces != 1 {
		t.Errorf("Expected 1 trace, got %d", traces)
	}
	if logs != 1 {
		t.Errorf("Expected 1 log, got %d", logs)
	}
	if metrics != 1 {
		t.Errorf("Expected 1 metric, got %d", metrics)
	}
	if total != 3 {
		t.Errorf("Expected 3 total requests, got %d", total)
	}
	mock.Reset()
	_, _, _, total = mock.GetStats()
	if total != 0 {
		t.Errorf("Expected 0 requests after reset, got %d", total)
	}
}

func TestMockOTelCollectorFailure(t *testing.T) {
	mock := NewMockOTelCollector()
	defer mock.Close()

	mock.ShouldFail = true

	tracesData := map[string]any{"resourceSpans": []any{}}
	resp := sendRequestWithResponse(t, mock.TracesURL(), tracesData)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", resp.StatusCode)
	}
}

func TestMockOTelCollectorBadJSON(t *testing.T) {
	mock := NewMockOTelCollector()
	defer mock.Close()

	resp := sendRawRequestWithResponse(t, mock.TracesURL(), "{")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 for bad JSON, got %d", resp.StatusCode)
	}
}

func sendRequest(t *testing.T, url string, data map[string]any) {
	body, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Failed to marshal JSON: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func sendRequestWithResponse(t *testing.T, url string, data map[string]any) *http.Response {
	body, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Failed to marshal JSON: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	return resp
}

func sendRawRequestWithResponse(t *testing.T, url string, raw string) *http.Response {
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(raw))
	if err != nil {
		t.Fatalf("Failed to send raw request: %v", err)
	}
	return resp
}
