package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
)

type MockOTelCollector struct {
	Server          *httptest.Server
	ReceivedTraces  []map[string]any
	ReceivedLogs    []map[string]any
	ReceivedMetrics []map[string]any
	mu              sync.Mutex
	RequestCount    int
	ShouldFail      bool
}

func NewMockOTelCollector() *MockOTelCollector {
	mock := &MockOTelCollector{
		ReceivedTraces:  make([]map[string]any, 0),
		ReceivedLogs:    make([]map[string]any, 0),
		ReceivedMetrics: make([]map[string]any, 0),
	}

	mux := http.NewServeMux()
	registerHandlers(mux, mock)

	mock.Server = httptest.NewServer(mux)
	return mock
}

func registerHandlers(mux *http.ServeMux, mock *MockOTelCollector) {
	mux.HandleFunc("/v1/traces", makeHandler(mock, func(m *MockOTelCollector, data map[string]any) {
		m.ReceivedTraces = append(m.ReceivedTraces, data)
	}))

	mux.HandleFunc("/v1/logs", makeHandler(mock, func(m *MockOTelCollector, data map[string]any) {
		m.ReceivedLogs = append(m.ReceivedLogs, data)
	}))

	mux.HandleFunc("/v1/metrics", makeHandler(mock, func(m *MockOTelCollector, data map[string]any) {
		m.ReceivedMetrics = append(m.ReceivedMetrics, data)
	}))
}

func makeHandler(mock *MockOTelCollector, appendFunc func(*MockOTelCollector, map[string]any)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		defer mock.mu.Unlock()
		mock.RequestCount++

		if mock.ShouldFail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var data map[string]any
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		appendFunc(mock, data)
		w.WriteHeader(http.StatusOK)
	}
}

func (m *MockOTelCollector) Close() {
	m.Server.Close()
}

func (m *MockOTelCollector) TracesURL() string {
	return m.Server.URL + "/v1/traces"
}

func (m *MockOTelCollector) LogsURL() string {
	return m.Server.URL + "/v1/logs"
}

func (m *MockOTelCollector) MetricsURL() string {
	return m.Server.URL + "/v1/metrics"
}

func (m *MockOTelCollector) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReceivedTraces = make([]map[string]any, 0)
	m.ReceivedLogs = make([]map[string]any, 0)
	m.ReceivedMetrics = make([]map[string]any, 0)
	m.RequestCount = 0
	m.ShouldFail = false
}

func (m *MockOTelCollector) GetStats() (traces, logs, metrics, total int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.ReceivedTraces), len(m.ReceivedLogs), len(m.ReceivedMetrics), m.RequestCount
}
