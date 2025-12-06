package structs

import (
	"testing"

	c "github.com/laiambryant/gotestutils/ctesting"
)

func TestTelemetryTypeString(t *testing.T) {
	test1 := c.NewCharacterizationTest(
		"Traces",
		nil,
		func() (string, error) {
			return TelemetryTraces.String(), nil
		},
	)
	test2 := c.NewCharacterizationTest(
		"Logs",
		nil,
		func() (string, error) {
			return TelemetryLogs.String(), nil
		},
	)
	test3 := c.NewCharacterizationTest(
		"Metrics",
		nil,
		func() (string, error) {
			return TelemetryMetrics.String(), nil
		},
	)
	test4 := c.NewCharacterizationTest(
		"Unknown",
		nil,
		func() (string, error) {
			return TelemetryType(999).String(), nil
		},
	)
	tests := []c.CharacterizationTest[string]{test1, test2, test3, test4}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}
