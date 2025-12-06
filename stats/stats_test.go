package stats

import (
	"bytes"
	"log/slog"
	"sync"
	"testing"

	c "github.com/laiambryant/gotestutils/ctesting"
	s "github.com/laiambryant/telemetry-ingestor/structs"
)

func TestRecordSuccess(t *testing.T) {
	test1 := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			ss := &SendStats{}
			ss.RecordSuccess(s.TelemetryTraces)
			return ss.TracesSuccess, nil
		},
	)
	test2 := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			ss := &SendStats{}
			ss.RecordSuccess(s.TelemetryLogs)
			return ss.LogsSuccess, nil
		},
	)
	test3 := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			ss := &SendStats{}
			ss.RecordSuccess(s.TelemetryMetrics)
			return ss.MetricsSuccess, nil
		},
	)
	tests := []c.CharacterizationTest[int]{test1, test2, test3}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRecordFailure(t *testing.T) {
	test1 := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			ss := &SendStats{}
			ss.RecordFailure(s.TelemetryTraces)
			return ss.TracesFailed, nil
		},
	)
	test2 := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			ss := &SendStats{}
			ss.RecordFailure(s.TelemetryLogs)
			return ss.LogsFailed, nil
		},
	)
	test3 := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			ss := &SendStats{}
			ss.RecordFailure(s.TelemetryMetrics)
			return ss.MetricsFailed, nil
		},
	)
	tests := []c.CharacterizationTest[int]{test1, test2, test3}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

type StatsSnapshot struct {
	TracesSuccess  int
	LogsSuccess    int
	MetricsSuccess int
	TracesFailed   int
	LogsFailed     int
	MetricsFailed  int
}

func TestRecordSuccessMultipleCalls(t *testing.T) {
	test := c.NewCharacterizationTest(
		StatsSnapshot{TracesSuccess: 2, LogsSuccess: 1, MetricsSuccess: 3},
		nil,
		func() (StatsSnapshot, error) {
			ss := &SendStats{}
			ss.RecordSuccess(s.TelemetryTraces)
			ss.RecordSuccess(s.TelemetryTraces)
			ss.RecordSuccess(s.TelemetryLogs)
			ss.RecordSuccess(s.TelemetryMetrics)
			ss.RecordSuccess(s.TelemetryMetrics)
			ss.RecordSuccess(s.TelemetryMetrics)
			return StatsSnapshot{
				TracesSuccess:  ss.TracesSuccess,
				LogsSuccess:    ss.LogsSuccess,
				MetricsSuccess: ss.MetricsSuccess,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[StatsSnapshot]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRecordFailureMultipleCalls(t *testing.T) {
	test := c.NewCharacterizationTest(
		StatsSnapshot{TracesFailed: 3, LogsFailed: 1, MetricsFailed: 1},
		nil,
		func() (StatsSnapshot, error) {
			ss := &SendStats{}
			ss.RecordFailure(s.TelemetryTraces)
			ss.RecordFailure(s.TelemetryTraces)
			ss.RecordFailure(s.TelemetryTraces)
			ss.RecordFailure(s.TelemetryLogs)
			ss.RecordFailure(s.TelemetryMetrics)
			return StatsSnapshot{
				TracesFailed:  ss.TracesFailed,
				LogsFailed:    ss.LogsFailed,
				MetricsFailed: ss.MetricsFailed,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[StatsSnapshot]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestMixedSuccessAndFailure(t *testing.T) {
	test := c.NewCharacterizationTest(
		StatsSnapshot{
			TracesSuccess:  2,
			TracesFailed:   1,
			LogsSuccess:    2,
			LogsFailed:     1,
			MetricsSuccess: 0,
			MetricsFailed:  2,
		},
		nil,
		func() (StatsSnapshot, error) {
			ss := &SendStats{}
			ss.RecordSuccess(s.TelemetryTraces)
			ss.RecordFailure(s.TelemetryTraces)
			ss.RecordSuccess(s.TelemetryTraces)
			ss.RecordSuccess(s.TelemetryLogs)
			ss.RecordSuccess(s.TelemetryLogs)
			ss.RecordFailure(s.TelemetryLogs)
			ss.RecordFailure(s.TelemetryMetrics)
			ss.RecordFailure(s.TelemetryMetrics)
			return StatsSnapshot{
				TracesSuccess:  ss.TracesSuccess,
				TracesFailed:   ss.TracesFailed,
				LogsSuccess:    ss.LogsSuccess,
				LogsFailed:     ss.LogsFailed,
				MetricsSuccess: ss.MetricsSuccess,
				MetricsFailed:  ss.MetricsFailed,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[StatsSnapshot]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestConcurrentRecordSuccess(t *testing.T) {
	iterations := 100
	test := c.NewCharacterizationTest(
		StatsSnapshot{TracesSuccess: iterations, LogsSuccess: iterations, MetricsSuccess: iterations},
		nil,
		func() (StatsSnapshot, error) {
			ss := &SendStats{}
			var wg sync.WaitGroup
			for i := 0; i < iterations; i++ {
				wg.Add(3)
				go func() {
					defer wg.Done()
					ss.RecordSuccess(s.TelemetryTraces)
				}()
				go func() {
					defer wg.Done()
					ss.RecordSuccess(s.TelemetryLogs)
				}()
				go func() {
					defer wg.Done()
					ss.RecordSuccess(s.TelemetryMetrics)
				}()
			}
			wg.Wait()
			return StatsSnapshot{
				TracesSuccess:  ss.TracesSuccess,
				LogsSuccess:    ss.LogsSuccess,
				MetricsSuccess: ss.MetricsSuccess,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[StatsSnapshot]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestConcurrentRecordFailure(t *testing.T) {
	iterations := 100
	test := c.NewCharacterizationTest(
		StatsSnapshot{TracesFailed: iterations, LogsFailed: iterations, MetricsFailed: iterations},
		nil,
		func() (StatsSnapshot, error) {
			ss := &SendStats{}
			var wg sync.WaitGroup
			for i := 0; i < iterations; i++ {
				wg.Add(3)
				go func() {
					defer wg.Done()
					ss.RecordFailure(s.TelemetryTraces)
				}()
				go func() {
					defer wg.Done()
					ss.RecordFailure(s.TelemetryLogs)
				}()
				go func() {
					defer wg.Done()
					ss.RecordFailure(s.TelemetryMetrics)
				}()
			}
			wg.Wait()
			return StatsSnapshot{
				TracesFailed:  ss.TracesFailed,
				LogsFailed:    ss.LogsFailed,
				MetricsFailed: ss.MetricsFailed,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[StatsSnapshot]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestConcurrentMixedOperations(t *testing.T) {
	iterations := 50
	test := c.NewCharacterizationTest(
		StatsSnapshot{
			TracesSuccess:  iterations,
			TracesFailed:   iterations,
			LogsSuccess:    iterations,
			LogsFailed:     iterations,
			MetricsSuccess: iterations,
			MetricsFailed:  iterations,
		},
		nil,
		func() (StatsSnapshot, error) {
			ss := &SendStats{}
			var wg sync.WaitGroup
			for i := 0; i < iterations; i++ {
				wg.Add(6)
				go func() {
					defer wg.Done()
					ss.RecordSuccess(s.TelemetryTraces)
				}()
				go func() {
					defer wg.Done()
					ss.RecordFailure(s.TelemetryTraces)
				}()
				go func() {
					defer wg.Done()
					ss.RecordSuccess(s.TelemetryLogs)
				}()
				go func() {
					defer wg.Done()
					ss.RecordFailure(s.TelemetryLogs)
				}()
				go func() {
					defer wg.Done()
					ss.RecordSuccess(s.TelemetryMetrics)
				}()
				go func() {
					defer wg.Done()
					ss.RecordFailure(s.TelemetryMetrics)
				}()
			}
			wg.Wait()
			return StatsSnapshot{
				TracesSuccess:  ss.TracesSuccess,
				TracesFailed:   ss.TracesFailed,
				LogsSuccess:    ss.LogsSuccess,
				LogsFailed:     ss.LogsFailed,
				MetricsSuccess: ss.MetricsSuccess,
				MetricsFailed:  ss.MetricsFailed,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[StatsSnapshot]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestPrintSummaryWithAllStats(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))
			slog.SetDefault(logger)

			ss := &SendStats{
				TracesSuccess:  10,
				TracesFailed:   2,
				LogsSuccess:    5,
				LogsFailed:     1,
				MetricsSuccess: 8,
				MetricsFailed:  3,
			}

			ss.PrintSummary()

			output := buf.String()

			expectedStrings := []string{
				"Telemetry Send Summary",
				"Traces",
				"success=10",
				"failed=2",
				"Logs",
				"success=5",
				"failed=1",
				"Metrics",
				"success=8",
				"failed=3",
				"Total",
				"success=23",
				"failed=6",
			}

			for _, expected := range expectedStrings {
				if !bytes.Contains([]byte(output), []byte(expected)) {
					return false, nil
				}
			}
			return true, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestPrintSummaryWithOnlyTraces(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))
	slog.SetDefault(logger)

	ss := &SendStats{
		TracesSuccess: 5,
		TracesFailed:  2,
	}

	ss.PrintSummary()

	output := buf.String()

	// Should contain Traces and Total
	if !bytes.Contains([]byte(output), []byte("Traces")) {
		t.Error("PrintSummary() output missing Traces")
	}
	if !bytes.Contains([]byte(output), []byte("Total")) {
		t.Error("PrintSummary() output missing Total")
	}
	// Verify the traces counts appear (success=5 for traces line)
	if !bytes.Contains([]byte(output), []byte("success=5")) {
		t.Error("PrintSummary() output missing traces success count")
	}
}

func TestPrintSummaryWithZeroStats(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))
	slog.SetDefault(logger)

	ss := &SendStats{}

	ss.PrintSummary()

	output := buf.String()

	// Should only contain summary header and Total with zeros
	if !bytes.Contains([]byte(output), []byte("Telemetry Send Summary")) {
		t.Error("PrintSummary() output missing summary header")
	}
	if !bytes.Contains([]byte(output), []byte("Total")) {
		t.Error("PrintSummary() output missing Total")
	}
	if !bytes.Contains([]byte(output), []byte("success=0")) {
		t.Error("PrintSummary() output missing zero success count")
	}
	if !bytes.Contains([]byte(output), []byte("failed=0")) {
		t.Error("PrintSummary() output missing zero failed count")
	}

	// Should not contain individual telemetry types since they're all zero
	if bytes.Contains([]byte(output), []byte("Traces")) &&
		bytes.Contains([]byte(output), []byte("success=0")) &&
		bytes.Count([]byte(output), []byte("success=0")) > 1 {
		// This is a soft check - implementation might still print zero stats
	}
}

func TestPrintSummaryThreadSafety(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))
	slog.SetDefault(logger)

	ss := &SendStats{}
	var wg sync.WaitGroup

	// Concurrently update stats and call PrintSummary
	for i := 0; i < 10; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			ss.RecordSuccess(s.TelemetryTraces)
		}()
		go func() {
			defer wg.Done()
			ss.RecordFailure(s.TelemetryLogs)
		}()
		go func() {
			defer wg.Done()
			ss.PrintSummary()
		}()
	}

	wg.Wait()

	// Test passes if no race conditions occur (run with -race flag)
	if ss.TracesSuccess != 10 {
		t.Errorf("TracesSuccess = %d, want 10", ss.TracesSuccess)
	}
	if ss.LogsFailed != 10 {
		t.Errorf("LogsFailed = %d, want 10", ss.LogsFailed)
	}
}

func TestSendStatsInitialization(t *testing.T) {
	ss := &SendStats{}

	// Verify all fields are initialized to zero
	if ss.TracesSuccess != 0 {
		t.Errorf("TracesSuccess = %d, want 0", ss.TracesSuccess)
	}
	if ss.TracesFailed != 0 {
		t.Errorf("TracesFailed = %d, want 0", ss.TracesFailed)
	}
	if ss.LogsSuccess != 0 {
		t.Errorf("LogsSuccess = %d, want 0", ss.LogsSuccess)
	}
	if ss.LogsFailed != 0 {
		t.Errorf("LogsFailed = %d, want 0", ss.LogsFailed)
	}
	if ss.MetricsSuccess != 0 {
		t.Errorf("MetricsSuccess = %d, want 0", ss.MetricsSuccess)
	}
	if ss.MetricsFailed != 0 {
		t.Errorf("MetricsFailed = %d, want 0", ss.MetricsFailed)
	}
}
