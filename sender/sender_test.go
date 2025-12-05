package sender

import (
	"errors"
	"fmt"
	"testing"

	c "github.com/laiambryant/gotestutils/ctesting"
	"github.com/laiambryant/telemetry-ingestor/stats"
	"github.com/laiambryant/telemetry-ingestor/structs"
	"github.com/laiambryant/telemetry-ingestor/testutil"
)

func TestJSONMarshalErrorInterface(t *testing.T) {
	innerErr := errors.New("json marshal failed")
	err := &JSONMarshalError{Err: innerErr}
	var _ error = err
	expectedMsg := fmt.Sprintf("failed to marshal JSON: %v", innerErr)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
	if unwrapped := err.Unwrap(); unwrapped != innerErr {
		t.Errorf("Expected Unwrap to return inner error, got %v", unwrapped)
	}
}

func TestHTTPRequestErrorInterface(t *testing.T) {
	endpoint := "http://localhost:4318/v1/traces"
	innerErr := errors.New("connection refused")
	err := &HTTPRequestError{Endpoint: endpoint, Err: innerErr}
	var _ error = err
	expectedMsg := fmt.Sprintf("failed to send request to %s: %v", endpoint, innerErr)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
	if unwrapped := err.Unwrap(); unwrapped != innerErr {
		t.Errorf("Expected Unwrap to return inner error, got %v", unwrapped)
	}
}

func TestJSONMarshalError(t *testing.T) {
	innerErr := errors.New("json error")
	err := &JSONMarshalError{Err: innerErr}
	expected := fmt.Sprintf("failed to marshal JSON: %v", innerErr)
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}
}

func TestHTTPRequestError(t *testing.T) {
	endpoint := "http://example.com"
	innerErr := errors.New("network error")
	err := &HTTPRequestError{Endpoint: endpoint, Err: innerErr}
	expected := fmt.Sprintf("failed to send request to %s: %v", endpoint, innerErr)
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}
}

type SendResult struct {
	Success bool
	Failed  bool
	Error   bool
}

func TestSendToOTelTraces(t *testing.T) {
	test1 := createSendTest(
		structs.TelemetryTraces,
		map[string]any{"resourceSpans": []any{}},
		false,
		SendResult{Success: true, Failed: false, Error: false},
	)
	test2 := createSendTest(
		structs.TelemetryTraces,
		map[string]any{"resourceSpans": []any{map[string]any{"id": 1}}},
		false,
		SendResult{Success: true, Failed: false, Error: false},
	)
	tests := []c.CharacterizationTest[SendResult]{test1, test2}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelLogs(t *testing.T) {
	test1 := createSendTest(
		structs.TelemetryLogs,
		map[string]any{"resourceLogs": []any{}},
		false,
		SendResult{Success: true, Failed: false, Error: false},
	)
	test2 := createSendTest(
		structs.TelemetryLogs,
		map[string]any{"resourceLogs": []any{map[string]any{"id": 1}}},
		false,
		SendResult{Success: true, Failed: false, Error: false},
	)
	tests := []c.CharacterizationTest[SendResult]{test1, test2}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelMetrics(t *testing.T) {
	test1 := createSendTest(
		structs.TelemetryMetrics,
		map[string]any{"resourceMetrics": []any{}},
		false,
		SendResult{Success: true, Failed: false, Error: false},
	)
	test2 := createSendTest(
		structs.TelemetryMetrics,
		map[string]any{"resourceMetrics": []any{map[string]any{"id": 1}}},
		false,
		SendResult{Success: true, Failed: false, Error: false},
	)
	tests := []c.CharacterizationTest[SendResult]{test1, test2}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelServerFailure(t *testing.T) {
	test1 := createSendTest(
		structs.TelemetryTraces,
		map[string]any{"resourceSpans": []any{}},
		true,
		SendResult{Success: false, Failed: true, Error: false},
	)
	test2 := createSendTest(
		structs.TelemetryLogs,
		map[string]any{"resourceLogs": []any{}},
		true,
		SendResult{Success: false, Failed: true, Error: false},
	)
	test3 := createSendTest(
		structs.TelemetryMetrics,
		map[string]any{"resourceMetrics": []any{}},
		true,
		SendResult{Success: false, Failed: true, Error: false},
	)
	tests := []c.CharacterizationTest[SendResult]{test1, test2, test3}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelInvalidEndpoint(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			st := &stats.SendStats{}
			payload := map[string]any{"resourceSpans": []any{}}
			err := SendToOTel("http://127.0.0.1:0/v1/traces", payload, structs.TelemetryTraces, st)
			if err == nil {
				return false, nil
			}
			var httpErr *HTTPRequestError
			if !errors.As(err, &httpErr) {
				t.Errorf("Expected HTTPRequestError, got %T: %v", err, err)
				return false, nil
			}
			return st.TracesFailed == 1 && st.TracesSuccess == 0, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelUnmarshalablePayload(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			st := &stats.SendStats{}
			payload := map[string]any{
				"unmarshalable": make(chan int),
			}
			err := SendToOTel(mock.TracesURL(), payload, structs.TelemetryTraces, st)
			if err == nil {
				return false, nil
			}
			var jsonErr *JSONMarshalError
			if !errors.As(err, &jsonErr) {
				t.Errorf("Expected JSONMarshalError, got %T: %v", err, err)
				return false, nil
			}
			return st.TracesSuccess == 0 && st.TracesFailed == 0, nil
		},
	)

	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelMultipleRequests(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()

			st := &stats.SendStats{}
			tracesPayload := map[string]any{"resourceSpans": []any{}}
			if err := SendToOTel(mock.TracesURL(), tracesPayload, structs.TelemetryTraces, st); err != nil {
				return false, err
			}
			logsPayload := map[string]any{"resourceLogs": []any{}}
			if err := SendToOTel(mock.LogsURL(), logsPayload, structs.TelemetryLogs, st); err != nil {
				return false, err
			}
			metricsPayload := map[string]any{"resourceMetrics": []any{}}
			if err := SendToOTel(mock.MetricsURL(), metricsPayload, structs.TelemetryMetrics, st); err != nil {
				return false, err
			}
			return st.TracesSuccess == 1 && st.LogsSuccess == 1 && st.MetricsSuccess == 1 &&
				st.TracesFailed == 0 && st.LogsFailed == 0 && st.MetricsFailed == 0, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelMixedSuccessFailure(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			st := &stats.SendStats{}
			payload1 := map[string]any{"resourceSpans": []any{}}
			if err := SendToOTel(mock.TracesURL(), payload1, structs.TelemetryTraces, st); err != nil {
				return false, err
			}
			mock.ShouldFail = true
			payload2 := map[string]any{"resourceLogs": []any{}}
			SendToOTel(mock.LogsURL(), payload2, structs.TelemetryLogs, st)
			return st.TracesSuccess == 1 && st.LogsFailed == 1 &&
				st.TracesFailed == 0 && st.LogsSuccess == 0, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelEmptyPayload(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			st := &stats.SendStats{}
			payload := map[string]any{}
			err := SendToOTel(mock.TracesURL(), payload, structs.TelemetryTraces, st)
			return err == nil && st.TracesSuccess == 1, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestSendToOTelComplexPayload(t *testing.T) {
	test := c.NewCharacterizationTest(
		true,
		nil,
		func() (bool, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			st := &stats.SendStats{}
			payload := map[string]any{
				"resourceSpans": []any{
					map[string]any{
						"resource": map[string]any{
							"attributes": []any{
								map[string]any{"key": "service.name", "value": "test-service"},
							},
						},
						"scopeSpans": []any{
							map[string]any{
								"spans": []any{
									map[string]any{
										"traceId":           "12345678901234567890123456789012",
										"spanId":            "1234567890123456",
										"name":              "test-span",
										"startTimeUnixNano": "1000000000",
										"endTimeUnixNano":   "2000000000",
									},
								},
							},
						},
					},
				},
			}
			err := SendToOTel(mock.TracesURL(), payload, structs.TelemetryTraces, st)
			return err == nil && st.TracesSuccess == 1 && st.TracesFailed == 0, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestErrorTypeAssertions(t *testing.T) {
	t.Run("JSONMarshalError type assertion", func(t *testing.T) {
		innerErr := errors.New("test error")
		err := &JSONMarshalError{Err: innerErr}

		var jsonErr *JSONMarshalError
		if !errors.As(err, &jsonErr) {
			t.Error("Expected errors.As to succeed for JSONMarshalError")
		}

		if jsonErr.Err != innerErr {
			t.Error("Expected error field to match inner error")
		}
	})

	t.Run("HTTPRequestError type assertion", func(t *testing.T) {
		innerErr := errors.New("test error")
		err := &HTTPRequestError{Endpoint: "http://test", Err: innerErr}

		var httpErr *HTTPRequestError
		if !errors.As(err, &httpErr) {
			t.Error("Expected errors.As to succeed for HTTPRequestError")
		}

		if httpErr.Endpoint != "http://test" {
			t.Error("Expected endpoint field to be preserved")
		}

		if httpErr.Err != innerErr {
			t.Error("Expected error field to match inner error")
		}
	})
}

func TestStatsThreadSafety(t *testing.T) {
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()
	st := &stats.SendStats{}
	payload := map[string]any{"resourceSpans": []any{}}
	done := make(chan bool)
	for range 10 {
		go func() {
			SendToOTel(mock.TracesURL(), payload, structs.TelemetryTraces, st)
			done <- true
		}()
	}
	for range 10 {
		<-done
	}
	if st.TracesSuccess != 10 {
		t.Errorf("Expected 10 successful traces, got %d", st.TracesSuccess)
	}

	if st.TracesFailed != 0 {
		t.Errorf("Expected 0 failed traces, got %d", st.TracesFailed)
	}
}

func createSendTest(telemetryType structs.TelemetryType, payload map[string]any, shouldFail bool, expected SendResult) c.CharacterizationTest[SendResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (SendResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			mock.ShouldFail = shouldFail
			st := &stats.SendStats{}
			var endpoint string
			switch telemetryType {
			case structs.TelemetryTraces:
				endpoint = mock.TracesURL()
			case structs.TelemetryLogs:
				endpoint = mock.LogsURL()
			case structs.TelemetryMetrics:
				endpoint = mock.MetricsURL()
			}
			err := SendToOTel(endpoint, payload, telemetryType, st)
			var success, failed bool
			switch telemetryType {
			case structs.TelemetryTraces:
				success = st.TracesSuccess == 1
				failed = st.TracesFailed == 1
			case structs.TelemetryLogs:
				success = st.LogsSuccess == 1
				failed = st.LogsFailed == 1
			case structs.TelemetryMetrics:
				success = st.MetricsSuccess == 1
				failed = st.MetricsFailed == 1
			}
			return SendResult{
				Success: success,
				Failed:  failed,
				Error:   err != nil,
			}, nil
		},
	)
}
