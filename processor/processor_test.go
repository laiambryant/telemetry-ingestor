package processor

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	c "github.com/laiambryant/gotestutils/ctesting"
	"github.com/laiambryant/telemetry-ingestor/config"
	"github.com/laiambryant/telemetry-ingestor/stats"
	"github.com/laiambryant/telemetry-ingestor/testutil"
)

type errReader struct{ err error }

func (e *errReader) Read(p []byte) (int, error) { return 0, e.err }

type IngestResult struct {
	TracesReceived  int
	LogsReceived    int
	MetricsReceived int
	ErrorOccurred   bool
}

func createTempTestFile(content string, prefix string) (string, error) {
	tmpFile, err := os.CreateTemp("", prefix)
	if err != nil {
		return "", err
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.WriteString(content); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", err
	}
	tmpFile.Close()
	return tmpPath, nil
}

func createIngestTest(fileContent string, sendAll bool, expected IngestResult) c.CharacterizationTest[IngestResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (IngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpPath, err := createTempTestFile(fileContent, "test-ingest-*.json")
			if err != nil {
				return IngestResult{}, err
			}
			defer os.Remove(tmpPath)
			cfg := &config.Config{
				OtelEndpoint:        mock.TracesURL(),
				OtelLogsEndpoint:    mock.LogsURL(),
				OtelMetricsEndpoint: mock.MetricsURL(),
				MaxBufferCapacity:   1048576,
				SendAll:             sendAll,
				Workers:             2,
			}
			err = IngestTelemetry(tmpPath, cfg)
			traces, logs, metrics, _ := mock.GetStats()
			return IngestResult{
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
				ErrorOccurred:   err != nil,
			}, nil
		},
	)
}

func TestIngestTelemetryLastMode(t *testing.T) {
	test1 := createIngestTest(
		`{"resourceSpans":[]}`,
		false,
		IngestResult{TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0, ErrorOccurred: false},
	)
	test2 := createIngestTest(
		`{"resourceSpans":[{"id":1}]}
{"resourceSpans":[{"id":2}]}
{"resourceLogs":[]}
{"resourceSpans":[]}.
{"resourceMetrics":[]}`,
		false,
		IngestResult{TracesReceived: 1, LogsReceived: 1, MetricsReceived: 1, ErrorOccurred: false},
	)
	test3 := createIngestTest(
		``,
		false,
		IngestResult{TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0, ErrorOccurred: false},
	)
	test4 := createIngestTest(
		`{"resourceSpans":[],"resourceLogs":[],"resourceMetrics":[]}`,
		false,
		IngestResult{TracesReceived: 1, LogsReceived: 1, MetricsReceived: 1, ErrorOccurred: false},
	)
	tests := []c.CharacterizationTest[IngestResult]{test1, test2, test3, test4}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestIngestTelemetrySendAllMode(t *testing.T) {
	test1 := createIngestTest(
		`{"resourceSpans":[]}`,
		true,
		IngestResult{TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0, ErrorOccurred: false},
	)
	test2 := createIngestTest(
		`{"resourceSpans":[{"id":1}]}
{"resourceSpans":[{"id":2}]}
{"resourceLogs":[]}
{"resourceMetrics":[]}`,
		true,
		IngestResult{TracesReceived: 2, LogsReceived: 1, MetricsReceived: 1, ErrorOccurred: false},
	)
	test3 := createIngestTest(
		`{"resourceSpans":[],"resourceLogs":[],"resourceMetrics":[]}`,
		true,
		IngestResult{TracesReceived: 1, LogsReceived: 1, MetricsReceived: 1, ErrorOccurred: false},
	)
	test4 := createIngestTest(
		`
{invalid json}
{"resourceSpans":[]}

{"resourceLogs":[]}`,
		true,
		IngestResult{TracesReceived: 1, LogsReceived: 1, MetricsReceived: 0, ErrorOccurred: false},
	)
	tests := []c.CharacterizationTest[IngestResult]{test1, test2, test3, test4}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestIngestTelemetryFileNotFound(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			cfg := &config.Config{
				OtelEndpoint:        "http://localhost:4318/v1/traces",
				OtelLogsEndpoint:    "http://localhost:4318/v1/logs",
				OtelMetricsEndpoint: "http://localhost:4318/v1/metrics",
				MaxBufferCapacity:   1048576,
				SendAll:             false,
				Workers:             2,
			}
			err := IngestTelemetry("/nonexistent/file.json", cfg)
			return err != nil, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestIngestTelemetryFileReadError(t *testing.T) {
	longLine := strings.Repeat("a", 2048)
	tmpPath, err := createTempTestFile(longLine, "test-too-long-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)
	cfg := &config.Config{
		OtelEndpoint:        "http://localhost:4318/v1/traces",
		OtelLogsEndpoint:    "http://localhost:4318/v1/logs",
		OtelMetricsEndpoint: "http://localhost:4318/v1/metrics",
		MaxBufferCapacity:   10,
		SendAll:             false,
		Workers:             1,
	}
	err = IngestTelemetry(tmpPath, cfg)
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	var fre *FileReadError
	if !errors.As(err, &fre) {
		t.Fatalf("expected FileReadError, got %T: %v", err, err)
	}
}

func TestProcessFileInSendAllModeScannerError(t *testing.T) {
	scanner := bufio.NewScanner(&errReader{err: fmt.Errorf("read failure")})
	cfg := &config.Config{Workers: 1}
	st := &stats.SendStats{}

	err := ProcessFileInSendAllMode(scanner, cfg, st)
	if err == nil {
		t.Fatalf("expected error from scanner, got nil")
	}
}

func TestFileNotFoundError(t *testing.T) {
	filePath := "/path/to/missing/file.json"
	err := &FileNotFoundError{FilePath: filePath}
	expected := fmt.Sprintf("file not found: %s", filePath)
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}
}

func TestFileOpenError(t *testing.T) {
	filePath := "/path/to/file.json"
	innerErr := errors.New("permission denied")
	err := &FileOpenError{FilePath: filePath, Err: innerErr}
	expectedMsg := fmt.Sprintf("failed to open file %s: %v", filePath, innerErr)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
	if unwrapped := err.Unwrap(); unwrapped != innerErr {
		t.Errorf("Expected Unwrap to return inner error, got %v", unwrapped)
	}
}

func TestFileReadError(t *testing.T) {
	filePath := "/path/to/file.json"
	innerErr := errors.New("unexpected EOF")
	err := &FileReadError{FilePath: filePath, Err: innerErr}
	expectedMsg := fmt.Sprintf("error reading file %s: %v", filePath, innerErr)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
	if unwrapped := err.Unwrap(); unwrapped != innerErr {
		t.Errorf("Expected Unwrap to return inner error, got %v", unwrapped)
	}
}

func TestOpenTelemetryFilePermissionDenied(t *testing.T) {
	tmpPath, err := createTempTestFile(`{"resourceMetrics":[]}`, "test-noperm-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)
	os.Chmod(tmpPath, 0000)
	defer os.Chmod(tmpPath, 0644)
	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if file != nil {
		file.Close()
	}
	if err == nil {
		t.Skip("Skipping permission test - running in environment where file permission restrictions are not enforced (e.g., root user or container)")
	}

	if file != nil || scanner != nil {
		t.Errorf("expected nil file and scanner on error")
	}
}

func TestSendErrorHandling(t *testing.T) {
	failingCfg := &config.Config{
		OtelEndpoint:        "http://127.0.0.1:0/v1/traces",
		OtelLogsEndpoint:    "http://127.0.0.1:0/v1/logs",
		OtelMetricsEndpoint: "http://127.0.0.1:0/v1/metrics",
		Workers:             2,
	}
	test1 := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			lastData := &LastTelemetryData{
				Traces:  map[string]any{"resourceSpans": []any{}},
				Logs:    map[string]any{"resourceLogs": []any{}},
				Metrics: map[string]any{"resourceMetrics": []any{}},
			}
			st := &stats.SendStats{}
			SendLastTelemetryData(lastData, failingCfg, st)
			return st.TracesFailed == 1 && st.LogsFailed == 1 && st.MetricsFailed == 1, nil
		},
	)
	test2 := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			content := `{"resourceSpans":[{"id":1}]}
{"resourceLogs":[{"id":2}]}
{"resourceMetrics":[{"id":3}]}`
			tmpPath, err := createTempTestFile(content, "test-worker-error-*.json")
			if err != nil {
				return false, err
			}
			defer os.Remove(tmpPath)

			file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
			if err != nil {
				return false, err
			}
			defer file.Close()

			st := &stats.SendStats{}
			if err := ProcessFileInSendAllMode(scanner, failingCfg, st); err != nil {
				return false, err
			}

			return st.TracesFailed == 1 && st.LogsFailed == 1 && st.MetricsFailed == 1, nil
		},
	)

	tests := []c.CharacterizationTest[bool]{test1, test2}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}
