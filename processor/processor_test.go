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
	s "github.com/laiambryant/telemetry-ingestor/structs"
	"github.com/laiambryant/telemetry-ingestor/testutil"
)

type errReader struct{ err error }

func (e *errReader) Read(p []byte) (int, error) { return 0, e.err }

type ParseResult struct {
	IsNil        bool
	HasTraces    bool
	HasLogs      bool
	HasMetrics   bool
	ErrorOccured bool
}

type UpdateResult struct {
	HasTraces  bool
	HasLogs    bool
	HasMetrics bool
}

type FileOpenResult struct {
	FileNotNil    bool
	ScannerNotNil bool
}

type LastModeResult struct {
	HasTraces  bool
	HasLogs    bool
	HasMetrics bool
	LineCount  int
	HasError   bool
}

type WorkerPoolResult struct {
	ChannelNotNil   bool
	WaitGroupNotNil bool
	ChannelClosed   bool
}

type ProcessTelemetryResult struct {
	JobsSent int
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

func testOpenFile(filePath string, bufferSize int) (FileOpenResult, error) {
	file, scanner, err := OpenTelemetryFile(filePath, bufferSize)
	if file != nil {
		defer file.Close()
	}
	return FileOpenResult{
		FileNotNil:    file != nil,
		ScannerNotNil: scanner != nil,
	}, err
}

func createSuccessfulFileOpenTest(content string, bufferSize int, prefix string) c.CharacterizationTest[FileOpenResult] {
	return c.NewCharacterizationTest(
		FileOpenResult{FileNotNil: true, ScannerNotNil: true},
		nil,
		func() (FileOpenResult, error) {
			tmpPath, err := createTempTestFile(content, prefix)
			if err != nil {
				return FileOpenResult{}, err
			}
			defer os.Remove(tmpPath)
			return testOpenFile(tmpPath, bufferSize)
		},
	)
}

func TestOpenTelemetryFile(t *testing.T) {
	test1 := c.NewCharacterizationTest(
		FileOpenResult{FileNotNil: false, ScannerNotNil: false},
		&FileNotFoundError{FilePath: "/nonexistent/file.json"},
		func() (FileOpenResult, error) {
			return testOpenFile("/nonexistent/file.json", 1024)
		},
	)
	test2 := createSuccessfulFileOpenTest(`{"resourceLogs":[]}`, 2048, "test-telemetry-*.json")
	test3 := c.NewCharacterizationTest(
		FileOpenResult{FileNotNil: false, ScannerNotNil: false},
		&FileOpenError{FilePath: "", Err: nil},
		func() (FileOpenResult, error) {
			tmpPath, err := createTempTestFile(`{"resourceMetrics":[]}`, "test-noperm-*.json")
			if err != nil {
				return FileOpenResult{}, err
			}
			defer os.Remove(tmpPath)
			os.Chmod(tmpPath, 0000)
			defer os.Chmod(tmpPath, 0644)

			return testOpenFile(tmpPath, 1048576)
		},
	)
	tests := []c.CharacterizationTest[FileOpenResult]{test1, test2, test3}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
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
	err := &FileOpenError{
		FilePath: filePath,
		Err:      innerErr,
	}
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
	err := &FileReadError{
		FilePath: filePath,
		Err:      innerErr,
	}
	expectedMsg := fmt.Sprintf("error reading file %s: %v", filePath, innerErr)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
	if unwrapped := err.Unwrap(); unwrapped != innerErr {
		t.Errorf("Expected Unwrap to return inner error, got %v", unwrapped)
	}
}

func TestErrorInterfaceImplementation(t *testing.T) {
	var _ error = (*FileNotFoundError)(nil)
	var _ error = (*FileOpenError)(nil)
	var _ error = (*FileReadError)(nil)
}

func createParseTelemetryTest(line string, lineNum int, expected ParseResult) c.CharacterizationTest[ParseResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (ParseResult, error) {
			data, err := ParseTelemetryLine(line, lineNum)
			return ParseResult{
				IsNil:        data == nil,
				HasTraces:    data != nil && data[resourceSpansField] != nil,
				HasLogs:      data != nil && data[resourceLogsField] != nil,
				HasMetrics:   data != nil && data[resourceMetricsField] != nil,
				ErrorOccured: err != nil,
			}, nil
		},
	)
}

func TestParseTelemetryLine(t *testing.T) {
	test1 := createParseTelemetryTest("", 1, ParseResult{IsNil: true, HasTraces: false, HasLogs: false, HasMetrics: false, ErrorOccured: false})
	test2 := createParseTelemetryTest(
		`{"resourceSpans":[{"resource":{"attributes":[]}}]}`,
		2,
		ParseResult{IsNil: false, HasTraces: true, HasLogs: false, HasMetrics: false, ErrorOccured: false},
	)
	test3 := createParseTelemetryTest(
		`{"resourceLogs":[{"resource":{"attributes":[]}}]}`,
		3,
		ParseResult{IsNil: false, HasTraces: false, HasLogs: true, HasMetrics: false, ErrorOccured: false},
	)
	test4 := createParseTelemetryTest(
		`{"resourceMetrics":[{"resource":{"attributes":[]}}]}`,
		4,
		ParseResult{IsNil: false, HasTraces: false, HasLogs: false, HasMetrics: true, ErrorOccured: false},
	)
	test5 := createParseTelemetryTest(
		`{invalid json}`,
		5,
		ParseResult{IsNil: true, HasTraces: false, HasLogs: false, HasMetrics: false, ErrorOccured: true},
	)
	tests := []c.CharacterizationTest[ParseResult]{test1, test2, test3, test4, test5}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func createUpdateLastDataTest(inputJSON string, expectedResult UpdateResult) c.CharacterizationTest[UpdateResult] {
	return c.NewCharacterizationTest(
		expectedResult,
		nil,
		func() (UpdateResult, error) {
			data, _ := ParseTelemetryLine(inputJSON, 1)
			lastData := &LastTelemetryData{}
			UpdateLastTelemetryData(data, lastData)
			return UpdateResult{
				HasTraces:  lastData.Traces != nil,
				HasLogs:    lastData.Logs != nil,
				HasMetrics: lastData.Metrics != nil,
			}, nil
		},
	)
}

func TestUpdateLastTelemetryData(t *testing.T) {
	test1 := createUpdateLastDataTest(
		`{"resourceSpans":[]}`,
		UpdateResult{HasTraces: true, HasLogs: false, HasMetrics: false},
	)
	test2 := createUpdateLastDataTest(
		`{"resourceLogs":[]}`,
		UpdateResult{HasTraces: false, HasLogs: true, HasMetrics: false},
	)
	test3 := createUpdateLastDataTest(
		`{"resourceMetrics":[]}`,
		UpdateResult{HasTraces: false, HasLogs: false, HasMetrics: true},
	)
	test4 := createUpdateLastDataTest(
		`{"resourceSpans":[],"resourceLogs":[],"resourceMetrics":[]}`,
		UpdateResult{HasTraces: true, HasLogs: true, HasMetrics: true},
	)
	tests := []c.CharacterizationTest[UpdateResult]{test1, test2, test3, test4}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func createProcessLastModeTest(fileContent string, expectedResult LastModeResult) c.CharacterizationTest[LastModeResult] {
	return c.NewCharacterizationTest(
		expectedResult,
		nil,
		func() (LastModeResult, error) {
			tmpPath, err := createTempTestFile(fileContent, "test-lastmode-*.json")
			if err != nil {
				return LastModeResult{}, err
			}
			defer os.Remove(tmpPath)
			file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
			if err != nil {
				return LastModeResult{}, err
			}
			defer file.Close()
			lastData, lineCount, err := ProcessFileInLastMode(scanner)
			return LastModeResult{
				HasTraces:  lastData != nil && lastData.Traces != nil,
				HasLogs:    lastData != nil && lastData.Logs != nil,
				HasMetrics: lastData != nil && lastData.Metrics != nil,
				LineCount:  lineCount,
				HasError:   err != nil,
			}, nil
		},
	)
}

func TestProcessFileInLastMode(t *testing.T) {
	test1 := createProcessLastModeTest(
		`{"resourceSpans":[]}`,
		LastModeResult{HasTraces: true, HasLogs: false, HasMetrics: false, LineCount: 1, HasError: false},
	)
	test2 := createProcessLastModeTest(
		`{"resourceSpans":[]}
{"resourceLogs":[]}
{"resourceMetrics":[]}`,
		LastModeResult{HasTraces: true, HasLogs: true, HasMetrics: true, LineCount: 3, HasError: false},
	)
	test3 := createProcessLastModeTest(
		`{"resourceSpans":[{"id":1}]}
{"resourceSpans":[{"id":2}]}`,
		LastModeResult{HasTraces: true, HasLogs: false, HasMetrics: false, LineCount: 2, HasError: false},
	)
	test4 := createProcessLastModeTest(
		``,
		LastModeResult{HasTraces: false, HasLogs: false, HasMetrics: false, LineCount: 0, HasError: false},
	)
	tests := []c.CharacterizationTest[LastModeResult]{test1, test2, test3, test4}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func createWorkerPoolTest(numWorkers int, expected WorkerPoolResult) c.CharacterizationTest[WorkerPoolResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (WorkerPoolResult, error) {
			stats := &stats.SendStats{}
			jobChan, wg := StartWorkerPool(numWorkers, stats)
			result := WorkerPoolResult{
				ChannelNotNil:   jobChan != nil,
				WaitGroupNotNil: wg != nil,
				ChannelClosed:   false,
			}
			if jobChan != nil {
				close(jobChan)
				result.ChannelClosed = true
				if wg != nil {
					wg.Wait()
				}
			}
			return result, nil
		},
	)
}

func TestStartWorkerPool(t *testing.T) {
	test1 := createWorkerPoolTest(1, WorkerPoolResult{ChannelNotNil: true, WaitGroupNotNil: true, ChannelClosed: true})
	test2 := createWorkerPoolTest(5, WorkerPoolResult{ChannelNotNil: true, WaitGroupNotNil: true, ChannelClosed: true})
	test3 := createWorkerPoolTest(20, WorkerPoolResult{ChannelNotNil: true, WaitGroupNotNil: true, ChannelClosed: true})
	tests := []c.CharacterizationTest[WorkerPoolResult]{test1, test2, test3}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func createProcessTelemetryTest(dataJSON string, lineNum int, expectedJobs int) c.CharacterizationTest[ProcessTelemetryResult] {
	return c.NewCharacterizationTest(
		ProcessTelemetryResult{JobsSent: expectedJobs},
		nil,
		func() (ProcessTelemetryResult, error) {
			data, _ := ParseTelemetryLine(dataJSON, lineNum)
			if data == nil {
				return ProcessTelemetryResult{JobsSent: 0}, nil
			}
			cfg := &config.Config{
				OtelEndpoint:        "http://localhost:4318/v1/traces",
				OtelLogsEndpoint:    "http://localhost:4318/v1/logs",
				OtelMetricsEndpoint: "http://localhost:4318/v1/metrics",
			}
			jobChan := make(chan s.TelemetryJob, 10)
			ProcessTelemetryInSendAllMode(data, lineNum, cfg, jobChan)
			close(jobChan)
			jobCount := 0
			for range jobChan {
				jobCount++
			}
			return ProcessTelemetryResult{JobsSent: jobCount}, nil
		},
	)
}

func TestProcessTelemetryInSendAllMode(t *testing.T) {
	test1 := createProcessTelemetryTest(`{"resourceSpans":[]}`, 1, 1)
	test2 := createProcessTelemetryTest(`{"resourceLogs":[]}`, 2, 1)
	test3 := createProcessTelemetryTest(`{"resourceMetrics":[]}`, 3, 1)
	test4 := createProcessTelemetryTest(`{"resourceSpans":[],"resourceLogs":[]}`, 4, 2)
	test5 := createProcessTelemetryTest(`{"resourceSpans":[],"resourceLogs":[],"resourceMetrics":[]}`, 5, 3)
	test6 := createProcessTelemetryTest(`{}`, 6, 0)
	tests := []c.CharacterizationTest[ProcessTelemetryResult]{test1, test2, test3, test4, test5, test6}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

type SendAllModeWithMockResult struct {
	TracesReceived  int
	LogsReceived    int
	MetricsReceived int
	TotalRequests   int
}

func createSendAllModeWithMockTest(fileContent string, expected SendAllModeWithMockResult) c.CharacterizationTest[SendAllModeWithMockResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (SendAllModeWithMockResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()

			tmpPath, err := createTempTestFile(fileContent, "test-sendall-*.json")
			if err != nil {
				return SendAllModeWithMockResult{}, err
			}
			defer os.Remove(tmpPath)

			file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
			if err != nil {
				return SendAllModeWithMockResult{}, err
			}
			defer file.Close()

			cfg := &config.Config{
				OtelEndpoint:        mock.TracesURL(),
				OtelLogsEndpoint:    mock.LogsURL(),
				OtelMetricsEndpoint: mock.MetricsURL(),
				Workers:             2,
			}
			testStats := &stats.SendStats{}

			ProcessFileInSendAllMode(scanner, cfg, testStats)

			traces, logs, metrics, total := mock.GetStats()
			return SendAllModeWithMockResult{
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
				TotalRequests:   total,
			}, nil
		},
	)
}

func TestProcessFileInSendAllModeWithMock(t *testing.T) {
	test1 := createSendAllModeWithMockTest(
		`{"resourceSpans":[]}`,
		SendAllModeWithMockResult{TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0, TotalRequests: 1},
	)

	test2 := createSendAllModeWithMockTest(
		`{"resourceLogs":[]}`,
		SendAllModeWithMockResult{TracesReceived: 0, LogsReceived: 1, MetricsReceived: 0, TotalRequests: 1},
	)

	test3 := createSendAllModeWithMockTest(
		`{"resourceMetrics":[]}`,
		SendAllModeWithMockResult{TracesReceived: 0, LogsReceived: 0, MetricsReceived: 1, TotalRequests: 1},
	)

	test4 := createSendAllModeWithMockTest(
		`{"resourceSpans":[],"resourceLogs":[],"resourceMetrics":[]}`,
		SendAllModeWithMockResult{TracesReceived: 1, LogsReceived: 1, MetricsReceived: 1, TotalRequests: 3},
	)

	tests := []c.CharacterizationTest[SendAllModeWithMockResult]{test1, test2, test3, test4}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestProcessFileInSendAllModeScannerError(t *testing.T) {
	// reader that returns an error immediately to force scanner.Err() != nil
	scanner := bufio.NewScanner(&errReader{err: fmt.Errorf("read failure")})
	cfg := &config.Config{Workers: 1}
	st := &stats.SendStats{}

	err := ProcessFileInSendAllMode(scanner, cfg, st)
	if err == nil {
		t.Fatalf("expected error from scanner, got nil")
	}
}

func TestProcessFileInSendAllModeWithMockDirect(t *testing.T) {
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()

	tmpPath, err := createTempTestFile(
		`{"resourceSpans":[]}
{"resourceLogs":[]}
{"resourceMetrics":[]}`,
		"test-sendall-direct-*.json",
	)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	cfg := &config.Config{
		OtelEndpoint:        mock.TracesURL(),
		OtelLogsEndpoint:    mock.LogsURL(),
		OtelMetricsEndpoint: mock.MetricsURL(),
		Workers:             2,
	}
	st := &stats.SendStats{}

	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	traces, logs, metrics, total := mock.GetStats()
	if traces != 1 || logs != 1 || metrics != 1 || total != 3 {
		t.Fatalf("unexpected mock counts: traces=%d logs=%d metrics=%d total=%d", traces, logs, metrics, total)
	}
}

func TestProcessFileInSendAllModeSkipsInvalidLines(t *testing.T) {
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()
	content := `
{invalid json}
{"resourceSpans":[]}
{not json}

{"resourceLogs":[]}`

	tmpPath, err := createTempTestFile(content, "test-sendall-skip-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	cfg := &config.Config{
		OtelEndpoint:        mock.TracesURL(),
		OtelLogsEndpoint:    mock.LogsURL(),
		OtelMetricsEndpoint: mock.MetricsURL(),
		Workers:             2,
	}
	st := &stats.SendStats{}

	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	traces, logs, metrics, total := mock.GetStats()
	if traces != 1 || logs != 1 || metrics != 0 || total != 2 {
		t.Fatalf("unexpected mock counts after skipping invalid/empty lines: traces=%d logs=%d metrics=%d total=%d", traces, logs, metrics, total)
	}
}

func TestProcessFileInSendAllModeContinueBranch(t *testing.T) {
	// ensure the loop hits the continue branch for an invalid line,
	// and that only the subsequent valid line is sent
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()

	content := `{not json}
{"resourceSpans":[]}
`
	tmpPath, err := createTempTestFile(content, "test-sendall-continue-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	cfg := &config.Config{
		OtelEndpoint:        mock.TracesURL(),
		OtelLogsEndpoint:    mock.LogsURL(),
		OtelMetricsEndpoint: mock.MetricsURL(),
		Workers:             2,
	}
	st := &stats.SendStats{}

	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	traces, logs, metrics, total := mock.GetStats()
	if traces != 1 || logs != 0 || metrics != 0 || total != 1 {
		t.Fatalf("unexpected mock counts: traces=%d logs=%d metrics=%d total=%d", traces, logs, metrics, total)
	}
}

func TestProcessFileInSendAllModeEmptyThenValid(t *testing.T) {
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()

	content := `
{"resourceSpans":[]}
`
	tmpPath, err := createTempTestFile(content, "test-sendall-empty-first-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	cfg := &config.Config{
		OtelEndpoint:        mock.TracesURL(),
		OtelLogsEndpoint:    mock.LogsURL(),
		OtelMetricsEndpoint: mock.MetricsURL(),
		Workers:             2,
	}
	st := &stats.SendStats{}

	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	traces, logs, metrics, total := mock.GetStats()
	if traces != 1 || logs != 0 || metrics != 0 || total != 1 {
		t.Fatalf("unexpected mock counts: traces=%d logs=%d metrics=%d total=%d", traces, logs, metrics, total)
	}
}

type SendLastDataWithMockResult struct {
	TracesReceived  int
	LogsReceived    int
	MetricsReceived int
}

func createSendLastDataWithMockTest(lastData *LastTelemetryData, expected SendLastDataWithMockResult) c.CharacterizationTest[SendLastDataWithMockResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (SendLastDataWithMockResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()

			cfg := &config.Config{
				OtelEndpoint:        mock.TracesURL(),
				OtelLogsEndpoint:    mock.LogsURL(),
				OtelMetricsEndpoint: mock.MetricsURL(),
			}
			testStats := &stats.SendStats{}

			dataCopy := &LastTelemetryData{
				Traces:  lastData.Traces,
				Logs:    lastData.Logs,
				Metrics: lastData.Metrics,
			}

			SendLastTelemetryData(dataCopy, cfg, testStats)

			traces, logs, metrics, _ := mock.GetStats()
			return SendLastDataWithMockResult{
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
}

func TestSendLastTelemetryDataWithMock(t *testing.T) {
	tracesData, _ := ParseTelemetryLine(`{"resourceSpans":[]}`, 1)
	test1 := createSendLastDataWithMockTest(
		&LastTelemetryData{Traces: tracesData},
		SendLastDataWithMockResult{TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0},
	)
	logsData, _ := ParseTelemetryLine(`{"resourceLogs":[]}`, 1)
	test2 := createSendLastDataWithMockTest(
		&LastTelemetryData{Logs: logsData},
		SendLastDataWithMockResult{TracesReceived: 0, LogsReceived: 1, MetricsReceived: 0},
	)
	metricsData, _ := ParseTelemetryLine(`{"resourceMetrics":[]}`, 1)
	test3 := createSendLastDataWithMockTest(
		&LastTelemetryData{Traces: tracesData, Logs: logsData, Metrics: metricsData},
		SendLastDataWithMockResult{TracesReceived: 1, LogsReceived: 1, MetricsReceived: 1},
	)
	test4 := createSendLastDataWithMockTest(
		&LastTelemetryData{},
		SendLastDataWithMockResult{TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0},
	)

	tests := []c.CharacterizationTest[SendLastDataWithMockResult]{test1, test2, test3, test4}
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

func TestIngestTelemetryReturnsFileReadError(t *testing.T) {
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

	if unwrapped := fre.Unwrap(); unwrapped != bufio.ErrTooLong {
		t.Fatalf("expected inner error bufio.ErrTooLong, got %v", unwrapped)
	}
}

type IngestWithMockResult struct {
	TracesReceived  int
	LogsReceived    int
	MetricsReceived int
	ErrorOccurred   bool
}

func createIngestWithMockTest(fileContent string, sendAll bool, expected IngestWithMockResult) c.CharacterizationTest[IngestWithMockResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (IngestWithMockResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()

			tmpPath, err := createTempTestFile(fileContent, "test-ingest-*.json")
			if err != nil {
				return IngestWithMockResult{}, err
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
			return IngestWithMockResult{
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
				ErrorOccurred:   err != nil,
			}, nil
		},
	)
}

func TestIngestTelemetryWithMock(t *testing.T) {
	test1 := createIngestWithMockTest(
		`{"resourceSpans":[]}`,
		false,
		IngestWithMockResult{TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0, ErrorOccurred: false},
	)
	test2 := createIngestWithMockTest(
		`{"resourceSpans":[{"id":1}]}
{"resourceSpans":[{"id":2}]}
{"resourceLogs":[]}
{"resourceMetrics":[]}`,
		false,
		IngestWithMockResult{TracesReceived: 1, LogsReceived: 1, MetricsReceived: 1, ErrorOccurred: false},
	)
	test3 := createIngestWithMockTest(
		``,
		false,
		IngestWithMockResult{TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0, ErrorOccurred: false},
	)

	tests := []c.CharacterizationTest[IngestWithMockResult]{test1, test2, test3}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestIngestTelemetryWithMockSendAll(t *testing.T) {
	test1 := createIngestWithMockTest(
		`{"resourceSpans":[]}`,
		true,
		IngestWithMockResult{TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0, ErrorOccurred: false},
	)
	test2 := createIngestWithMockTest(
		`{"resourceSpans":[{"id":1}]}
{"resourceSpans":[{"id":2}]}
{"resourceLogs":[]}
{"resourceMetrics":[]}`,
		true,
		IngestWithMockResult{TracesReceived: 2, LogsReceived: 1, MetricsReceived: 1, ErrorOccurred: false},
	)
	test3 := createIngestWithMockTest(
		``,
		true,
		IngestWithMockResult{TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0, ErrorOccurred: false},
	)

	tests := []c.CharacterizationTest[IngestWithMockResult]{test1, test2, test3}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendLastTelemetryDataErrors(t *testing.T) {
	tracesData, _ := ParseTelemetryLine(`{"resourceSpans":[]}`, 1)
	logsData, _ := ParseTelemetryLine(`{"resourceLogs":[]}`, 1)
	metricsData, _ := ParseTelemetryLine(`{"resourceMetrics":[]}`, 1)

	last := &LastTelemetryData{
		Traces:  tracesData,
		Logs:    logsData,
		Metrics: metricsData,
	}
	cfg := &config.Config{
		OtelEndpoint:        "http://127.0.0.1:0/v1/traces",
		OtelLogsEndpoint:    "http://127.0.0.1:0/v1/logs",
		OtelMetricsEndpoint: "http://127.0.0.1:0/v1/metrics",
	}
	st := &stats.SendStats{}
	SendLastTelemetryData(last, cfg, st)

	if st.TracesFailed != 1 {
		t.Errorf("expected 1 failed trace, got %d", st.TracesFailed)
	}
	if st.LogsFailed != 1 {
		t.Errorf("expected 1 failed log, got %d", st.LogsFailed)
	}
	if st.MetricsFailed != 1 {
		t.Errorf("expected 1 failed metric, got %d", st.MetricsFailed)
	}
}

func TestProcessFileInSendAllModeWithMockSkipped(t *testing.T) {
	t.Skip("Skipping SendAll mode tests - stats.PrintSummary() causes test hangs in worker pool scenarios")
}

func TestProcessFileInSendAllModeContinueOnParseError(t *testing.T) {
	// Tests the continue branch at lines 159-162 when ParseTelemetryLine returns an error
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()

	// Mix of invalid JSON (which causes parse error) and valid data
	content := `{invalid json line}
{"resourceSpans":[{"id":1}]}
{another bad line
{"resourceLogs":[{"id":2}]}
`
	tmpPath, err := createTempTestFile(content, "test-parse-error-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	cfg := &config.Config{
		OtelEndpoint:        mock.TracesURL(),
		OtelLogsEndpoint:    mock.LogsURL(),
		OtelMetricsEndpoint: mock.MetricsURL(),
		Workers:             2,
	}
	st := &stats.SendStats{}

	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Only the 2 valid lines should be sent
	traces, logs, _, total := mock.GetStats()
	if traces != 1 || logs != 1 || total != 2 {
		t.Errorf("expected 1 trace and 1 log (2 total), got traces=%d logs=%d total=%d", traces, logs, total)
	}
}

func TestProcessFileInSendAllModeContinueOnNilData(t *testing.T) {
	// Tests the continue branch at lines 159-162 when data is nil (empty lines)
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()

	// Mix of empty lines and valid data
	content := `

{"resourceSpans":[{"id":1}]}

{"resourceLogs":[{"id":2}]}

`
	tmpPath, err := createTempTestFile(content, "test-nil-data-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	cfg := &config.Config{
		OtelEndpoint:        mock.TracesURL(),
		OtelLogsEndpoint:    mock.LogsURL(),
		OtelMetricsEndpoint: mock.MetricsURL(),
		Workers:             2,
	}
	st := &stats.SendStats{}

	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Only the 2 valid non-empty lines should be sent
	traces, logs, _, total := mock.GetStats()
	if traces != 1 || logs != 1 || total != 2 {
		t.Errorf("expected 1 trace and 1 log (2 total), got traces=%d logs=%d total=%d", traces, logs, total)
	}
}

func TestWorkerHandlesSendToOTelError(t *testing.T) {
	// Tests the error handling in worker function at lines 240-242
	// When SendToOTel fails, worker should log the error but continue processing

	// Use an invalid endpoint to cause SendToOTel to fail
	cfg := &config.Config{
		OtelEndpoint:        "http://127.0.0.1:0/v1/traces", // Port 0 - connection refused
		OtelLogsEndpoint:    "http://127.0.0.1:0/v1/logs",
		OtelMetricsEndpoint: "http://127.0.0.1:0/v1/metrics",
		Workers:             2,
	}

	tmpPath, err := createTempTestFile(
		`{"resourceSpans":[{"id":1}]}
{"resourceLogs":[{"id":2}]}
{"resourceMetrics":[{"id":3}]}`,
		"test-worker-error-*.json",
	)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	st := &stats.SendStats{}

	// This should complete without panic, even though all sends fail
	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error from ProcessFileInSendAllMode, got %v", err)
	}

	// Verify all sends failed (recorded in stats)
	if st.TracesFailed != 1 {
		t.Errorf("expected 1 failed trace, got %d", st.TracesFailed)
	}
	if st.LogsFailed != 1 {
		t.Errorf("expected 1 failed log, got %d", st.LogsFailed)
	}
	if st.MetricsFailed != 1 {
		t.Errorf("expected 1 failed metric, got %d", st.MetricsFailed)
	}
}

func TestWorkerHandlesMultipleJobsWithErrors(t *testing.T) {
	// Tests worker continues processing after encountering errors (lines 240-242)
	// Worker should process all jobs even if some fail

	cfg := &config.Config{
		OtelEndpoint:        "http://127.0.0.1:0/v1/traces",
		OtelLogsEndpoint:    "http://127.0.0.1:0/v1/logs",
		OtelMetricsEndpoint: "http://127.0.0.1:0/v1/metrics",
		Workers:             1, // Single worker to ensure sequential processing
	}

	// Create file with multiple lines to generate multiple jobs
	content := `{"resourceSpans":[{"id":1}]}
{"resourceSpans":[{"id":2}]}
{"resourceSpans":[{"id":3}]}
{"resourceLogs":[{"id":1}]}
{"resourceLogs":[{"id":2}]}
`
	tmpPath, err := createTempTestFile(content, "test-worker-multi-error-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	file, scanner, err := OpenTelemetryFile(tmpPath, 1048576)
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	defer file.Close()

	st := &stats.SendStats{}

	if err := ProcessFileInSendAllMode(scanner, cfg, st); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// All jobs should be attempted (and fail)
	if st.TracesFailed != 3 {
		t.Errorf("expected 3 failed traces, got %d", st.TracesFailed)
	}
	if st.LogsFailed != 2 {
		t.Errorf("expected 2 failed logs, got %d", st.LogsFailed)
	}
}
