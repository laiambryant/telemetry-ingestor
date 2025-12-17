package main

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	c "github.com/laiambryant/gotestutils/ctesting"
	"github.com/laiambryant/telemetry-ingestor/config"
	"github.com/laiambryant/telemetry-ingestor/testutil"
	"github.com/spf13/cobra"
)

type RunIngestResult struct {
	ErrorOccurred   bool
	TracesReceived  int
	LogsReceived    int
	MetricsReceived int
}

func createRunIngestTest(fileContent string, filePath string, expected RunIngestResult) c.CharacterizationTest[RunIngestResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (RunIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			var tmpPath string
			var shouldCleanup bool
			if filePath == "" {
				tmpFile, err := os.CreateTemp("", "test-*.json")
				if err != nil {
					return RunIngestResult{}, err
				}
				tmpPath = tmpFile.Name()
				tmpFile.WriteString(fileContent)
				tmpFile.Close()
				shouldCleanup = true
			} else {
				tmpPath = filePath
			}
			if shouldCleanup {
				defer os.Remove(tmpPath)
			}
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err := runIngest(cmd, []string{tmpPath})
			traces, logs, metrics, _ := mock.GetStats()
			return RunIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
}

func TestRunIngest(t *testing.T) {
	test1 := createRunIngestTest(
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123"}]}]}]}`,
		"",
		RunIngestResult{ErrorOccurred: false, TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0},
	)
	test2 := createRunIngestTest(
		`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":"test log"}]}]}]}`,
		"",
		RunIngestResult{ErrorOccurred: false, TracesReceived: 0, LogsReceived: 1, MetricsReceived: 0},
	)
	test3 := createRunIngestTest(
		`{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"test_metric"}]}]}]}`,
		"",
		RunIngestResult{ErrorOccurred: false, TracesReceived: 0, LogsReceived: 0, MetricsReceived: 1},
	)
	test4 := createRunIngestTest(
		"",
		"",
		RunIngestResult{ErrorOccurred: false, TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0},
	)
	test5 := createRunIngestTest(
		"",
		"nonexistent.json",
		RunIngestResult{ErrorOccurred: true, TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0},
	)
	tests := []c.CharacterizationTest[RunIngestResult]{test1, test2, test3, test4, test5}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

type FolderIngestResult struct {
	ErrorOccurred   bool
	TracesReceived  int
	LogsReceived    int
	MetricsReceived int
}

func createRunIngestFolderTest(files map[string]string, pattern string, expected FolderIngestResult) c.CharacterizationTest[FolderIngestResult] {
	return c.NewCharacterizationTest(
		expected,
		nil,
		func() (FolderIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-folder-*")
			if err != nil {
				return FolderIngestResult{}, err
			}
			defer os.RemoveAll(tmpDir)
			for filename, content := range files {
				filePath := filepath.Join(tmpDir, filename)
				if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
					return FolderIngestResult{}, err
				}
			}
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = pattern
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			traces, logs, metrics, _ := mock.GetStats()
			return FolderIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
}

func TestRunIngestFolder(t *testing.T) {
	test1 := createRunIngestFolderTest(
		map[string]string{
			"file1.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123"}]}]}]}`,
			"file2.json": `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":"test log"}]}]}]}`,
			"file3.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"def456"}]}]}]}`,
		},
		"*.json",
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 2, LogsReceived: 1, MetricsReceived: 0},
	)
	test2 := createRunIngestFolderTest(
		map[string]string{
			"telemetry_1.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc"}]}]}]}`,
			"telemetry_2.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"def"}]}]}]}`,
			"other.json":       `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"ghi"}]}]}]}`,
		},
		"telemetry_*.json",
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 2, LogsReceived: 0, MetricsReceived: 0},
	)
	test3 := createRunIngestFolderTest(
		map[string]string{
			"test.txt": "not a json file",
		},
		"*.json",
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0},
	)
	test4 := createRunIngestFolderTest(
		map[string]string{
			"valid.json":   `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc"}]}]}]}`,
			"invalid.json": `{invalid json`,
			"empty.json":   ``,
		},
		"*.json",
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0},
	)
	tests := []c.CharacterizationTest[FolderIngestResult]{test1, test2, test3, test4}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderFileProcessingError(t *testing.T) {
	test := c.NewCharacterizationTest(
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 1, LogsReceived: 0, MetricsReceived: 0},
		nil,
		func() (FolderIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-folder-error-*")
			if err != nil {
				return FolderIngestResult{}, err
			}
			defer os.RemoveAll(tmpDir)
			validFile := filepath.Join(tmpDir, "aaa_valid.json")
			if err := os.WriteFile(validFile, []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc"}]}]}]}`), 0644); err != nil {
				return FolderIngestResult{}, err
			}
			errorDir := filepath.Join(tmpDir, "zzz_error.json")
			if err := os.Mkdir(errorDir, 0755); err != nil {
				return FolderIngestResult{}, err
			}
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			traces, logs, metrics, _ := mock.GetStats()
			return FolderIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[FolderIngestResult]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderWithArgs(t *testing.T) {
	test := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-folder-args-*")
			if err != nil {
				return 0, err
			}
			defer os.RemoveAll(tmpDir)
			testFile := filepath.Join(tmpDir, "test.json")
			content := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123"}]}]}]}`
			if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
				return 0, err
			}
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			if err != nil {
				return 0, err
			}
			traces, _, _, _ := mock.GetStats()
			return traces, nil
		},
	)
	tests := []c.CharacterizationTest[int]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderNonExistentDirectory(t *testing.T) {
	test := c.NewCharacterizationTest(
		false,
		nil,
		func() (bool, error) {
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err := runIngestFolder(cmd, []string{"/nonexistent/directory/path"})
			return err != nil, nil
		},
	)
	tests := []c.CharacterizationTest[bool]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestCobraCommandSetup(t *testing.T) {
	if rootCmd == nil {
		t.Fatal("rootCmd is nil")
	}
	if rootCmd.Use != "ingest_telemetry [file]" {
		t.Errorf("Expected Use to be 'ingest_telemetry [file]', got %s", rootCmd.Use)
	}
	if folderCmd == nil {
		t.Fatal("folderCmd is nil")
	}
	if folderCmd.Use != "folder [directory]" {
		t.Errorf("Expected Use to be 'folder [directory]', got %s", folderCmd.Use)
	}
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "folder" {
			found = true
			break
		}
	}
	if !found {
		t.Error("folder command not added to root command")
	}
}

func TestCobraCommandFlags(t *testing.T) {
	cfg = config.NewConfig()
	rootFlags := []string{"file", "traces-endpoint", "logs-endpoint", "metrics-endpoint", "max-buffer-capacity", "sendAll", "workers"}
	for _, flagName := range rootFlags {
		flag := rootCmd.Flags().Lookup(flagName)
		if flag == nil {
			t.Errorf("Root command missing flag: %s", flagName)
		}
	}
	folderFlags := []string{"folder", "traces-endpoint", "logs-endpoint", "metrics-endpoint", "max-buffer-capacity", "sendAll", "workers", "pattern"}
	for _, flagName := range folderFlags {
		flag := folderCmd.Flags().Lookup(flagName)
		if flag == nil {
			t.Errorf("Folder command missing flag: %s", flagName)
		}
	}
}

func TestRunIngestWithFileFlag(t *testing.T) {
	test := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpFile, err := os.CreateTemp("", "test-*.json")
			if err != nil {
				return 0, err
			}
			tmpPath := tmpFile.Name()
			tmpFile.WriteString(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123"}]}]}]}`)
			tmpFile.Close()
			defer os.Remove(tmpPath)
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.FilePath = tmpPath
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngest(cmd, []string{})
			if err != nil {
				return 0, err
			}
			traces, _, _, _ := mock.GetStats()
			return traces, nil
		},
	)
	tests := []c.CharacterizationTest[int]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderDefaultDirectory(t *testing.T) {
	test := c.NewCharacterizationTest(
		1,
		nil,
		func() (int, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-default-*")
			if err != nil {
				return 0, err
			}
			defer os.RemoveAll(tmpDir)
			testFile := filepath.Join(tmpDir, "test.json")
			content := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"test"}]}]}]}`
			if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
				return 0, err
			}
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.FilePath = tmpDir
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{})
			if err != nil {
				return 0, err
			}
			traces, _, _, _ := mock.GetStats()
			return traces, nil
		},
	)
	tests := []c.CharacterizationTest[int]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestMainFunction(t *testing.T) {
	if rootCmd == nil {
		t.Fatal("rootCmd should not be nil after init")
	}
	if folderCmd == nil {
		t.Fatal("folderCmd should not be nil after init")
	}
}

func BenchmarkRunIngest(b *testing.B) {
	mock := testutil.NewMockOTelCollector()
	defer mock.Close()
	tmpFile, _ := os.CreateTemp("", "bench-*.json")
	tmpPath := tmpFile.Name()
	tmpFile.WriteString(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc123"}]}]}]}`)
	tmpFile.Close()
	defer os.Remove(tmpPath)
	originalCfg := cfg
	cfg = config.NewConfig()
	cfg.OtelEndpoint = mock.TracesURL()
	cfg.OtelLogsEndpoint = mock.LogsURL()
	cfg.OtelMetricsEndpoint = mock.MetricsURL()
	defer func() { cfg = originalCfg }()
	cmd := &cobra.Command{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runIngest(cmd, []string{tmpPath})
	}
}

func createZipFile(t *testing.T, zipPath string, files map[string]string) {
	t.Helper()
	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("Failed to create zip file: %v", err)
	}
	defer zipFile.Close()
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()
	for name, content := range files {
		writer, err := zipWriter.Create(name)
		if err != nil {
			t.Fatalf("Failed to create file in zip: %v", err)
		}
		if _, err := writer.Write([]byte(content)); err != nil {
			t.Fatalf("Failed to write to zip file: %v", err)
		}
	}
}

func TestRunIngestFolderWithZipFiles(t *testing.T) {
	test := c.NewCharacterizationTest(
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 2, LogsReceived: 1, MetricsReceived: 0},
		nil,
		func() (FolderIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()

			tmpDir, err := os.MkdirTemp("", "test-zip-*")
			if err != nil {
				return FolderIngestResult{}, err
			}
			defer os.RemoveAll(tmpDir)
			zipPath := filepath.Join(tmpDir, "telemetry.zip")
			zipFiles := map[string]string{
				"trace1.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abc"}]}]}]}`,
				"trace2.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"def"}]}]}]}`,
				"log1.json":   `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":"test"}]}]}]}`,
			}
			createZipFile(t, zipPath, zipFiles)
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			traces, logs, metrics, _ := mock.GetStats()
			return FolderIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[FolderIngestResult]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderMixedZipAndRegularFiles(t *testing.T) {
	test := c.NewCharacterizationTest(
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 3, LogsReceived: 1, MetricsReceived: 0},
		nil,
		func() (FolderIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-mixed-*")
			if err != nil {
				return FolderIngestResult{}, err
			}
			defer os.RemoveAll(tmpDir)
			regularFile := filepath.Join(tmpDir, "regular.json")
			if err := os.WriteFile(regularFile, []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"regular"}]}]}]}`), 0644); err != nil {
				return FolderIngestResult{}, err
			}
			zipPath := filepath.Join(tmpDir, "archive.zip")
			zipFiles := map[string]string{
				"zip_trace1.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"zip1"}]}]}]}`,
				"zip_trace2.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"zip2"}]}]}]}`,
				"zip_log.json":    `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":"ziplog"}]}]}]}`,
			}
			createZipFile(t, zipPath, zipFiles)
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			traces, logs, metrics, _ := mock.GetStats()
			return FolderIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[FolderIngestResult]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderZipWithNoMatchingFiles(t *testing.T) {
	test := c.NewCharacterizationTest(
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0},
		nil,
		func() (FolderIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-zip-nomatch-*")
			if err != nil {
				return FolderIngestResult{}, err
			}
			defer os.RemoveAll(tmpDir)
			zipPath := filepath.Join(tmpDir, "data.zip")
			zipFiles := map[string]string{
				"data.txt": "some text data",
				"info.xml": "<data>xml</data>",
			}
			createZipFile(t, zipPath, zipFiles)
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			traces, logs, metrics, _ := mock.GetStats()
			return FolderIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[FolderIngestResult]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderZipWithPattern(t *testing.T) {
	test := c.NewCharacterizationTest(
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 2, LogsReceived: 0, MetricsReceived: 0},
		nil,
		func() (FolderIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-zip-pattern-*")
			if err != nil {
				return FolderIngestResult{}, err
			}
			defer os.RemoveAll(tmpDir)
			zipPath := filepath.Join(tmpDir, "telemetry.zip")
			zipFiles := map[string]string{
				"telemetry_1.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"t1"}]}]}]}`,
				"telemetry_2.json": `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"t2"}]}]}]}`,
				"other.json":       `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"other"}]}]}]}`,
			}
			createZipFile(t, zipPath, zipFiles)
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "telemetry_*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			traces, logs, metrics, _ := mock.GetStats()
			return FolderIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[FolderIngestResult]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}

func TestRunIngestFolderCorruptedZip(t *testing.T) {
	test := c.NewCharacterizationTest(
		FolderIngestResult{ErrorOccurred: false, TracesReceived: 0, LogsReceived: 0, MetricsReceived: 0},
		nil,
		func() (FolderIngestResult, error) {
			mock := testutil.NewMockOTelCollector()
			defer mock.Close()
			tmpDir, err := os.MkdirTemp("", "test-zip-corrupt-*")
			if err != nil {
				return FolderIngestResult{}, err
			}
			defer os.RemoveAll(tmpDir)
			zipPath := filepath.Join(tmpDir, "corrupted.zip")
			if err := os.WriteFile(zipPath, []byte("this is not a valid zip file"), 0644); err != nil {
				return FolderIngestResult{}, err
			}
			originalCfg := cfg
			cfg = config.NewConfig()
			cfg.OtelEndpoint = mock.TracesURL()
			cfg.OtelLogsEndpoint = mock.LogsURL()
			cfg.OtelMetricsEndpoint = mock.MetricsURL()
			cfg.FilePattern = "*.json"
			defer func() { cfg = originalCfg }()
			cmd := &cobra.Command{}
			err = runIngestFolder(cmd, []string{tmpDir})
			traces, logs, metrics, _ := mock.GetStats()
			return FolderIngestResult{
				ErrorOccurred:   err != nil,
				TracesReceived:  traces,
				LogsReceived:    logs,
				MetricsReceived: metrics,
			}, nil
		},
	)
	tests := []c.CharacterizationTest[FolderIngestResult]{test}
	c.VerifyCharacterizationTestsAndResults(t, tests, false)
}
