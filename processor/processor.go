package processor

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"os"
	"sync"

	"github.com/laiambryant/telemetry-ingestor/config"
	"github.com/laiambryant/telemetry-ingestor/sender"
	"github.com/laiambryant/telemetry-ingestor/stats"
	s "github.com/laiambryant/telemetry-ingestor/structs"
)

type LastTelemetryData struct {
	Traces  s.TelemetryData
	Logs    s.TelemetryData
	Metrics s.TelemetryData
}

const (
	resourceSpansField   = "resourceSpans"
	resourceLogsField    = "resourceLogs"
	resourceMetricsField = "resourceMetrics"
)

func OpenTelemetryFile(filePath string, maxBufferCapacity int) (*os.File, *bufio.Scanner, error) {

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil, &FileNotFoundError{FilePath: filePath}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, &FileOpenError{FilePath: filePath, Err: err}
	}

	scanner := bufio.NewScanner(file)
	buf := make([]byte, maxBufferCapacity)
	scanner.Buffer(buf, maxBufferCapacity)

	return file, scanner, nil
}

func ParseTelemetryLine(line string, lineNum int) (s.TelemetryData, error) {
	if len(line) == 0 {
		return nil, nil
	}

	var data s.TelemetryData
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		slog.Error("Error parsing line", "line", lineNum, "error", err)
		return nil, err
	}

	return data, nil
}

func ProcessTelemetryInSendAllMode(data s.TelemetryData, lineNum int, config *config.Config, jobChan chan<- s.TelemetryJob) {
	if _, hasTraces := data[resourceSpansField]; hasTraces {
		payload := map[string]any{resourceSpansField: data[resourceSpansField]}
		jobChan <- s.TelemetryJob{
			Endpoint:      config.OtelEndpoint,
			Payload:       payload,
			TelemetryType: s.TelemetryTraces,
			LineNum:       lineNum,
		}
	}

	if _, hasLogs := data[resourceLogsField]; hasLogs {
		payload := map[string]any{resourceLogsField: data[resourceLogsField]}
		jobChan <- s.TelemetryJob{
			Endpoint:      config.OtelLogsEndpoint,
			Payload:       payload,
			TelemetryType: s.TelemetryLogs,
			LineNum:       lineNum,
		}
	}

	if _, hasMetrics := data[resourceMetricsField]; hasMetrics {
		payload := map[string]any{resourceMetricsField: data[resourceMetricsField]}
		jobChan <- s.TelemetryJob{
			Endpoint:      config.OtelMetricsEndpoint,
			Payload:       payload,
			TelemetryType: s.TelemetryMetrics,
			LineNum:       lineNum,
		}
	}
}

func UpdateLastTelemetryData(data s.TelemetryData, lastData *LastTelemetryData) {
	if _, hasTraces := data[resourceSpansField]; hasTraces {
		lastData.Traces = data
	}
	if _, hasLogs := data[resourceLogsField]; hasLogs {
		lastData.Logs = data
	}
	if _, hasMetrics := data[resourceMetricsField]; hasMetrics {
		lastData.Metrics = data
	}
}

func StartWorkerPool(numWorkers int, stats *stats.SendStats) (chan s.TelemetryJob, *sync.WaitGroup) {
	jobChan := make(chan s.TelemetryJob, numWorkers*2)
	wg := &sync.WaitGroup{}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i+1, jobChan, wg, stats)
	}

	return jobChan, wg
}

func ProcessFileInSendAllMode(scanner *bufio.Scanner, config *config.Config, stats *stats.SendStats) error {
	jobChan, wg := StartWorkerPool(config.Workers, stats)

	lineNum := 0
	lineCount := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		data, err := ParseTelemetryLine(line, lineNum)
		if err != nil || data == nil {
			continue
		}

		lineCount++
		ProcessTelemetryInSendAllMode(data, lineNum, config, jobChan)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	slog.Info("Finished reading file", "total_lines", lineCount)

	close(jobChan)
	slog.Info("Waiting for workers to finish")
	wg.Wait()
	stats.PrintSummary()

	return nil
}

func ProcessFileInLastMode(scanner *bufio.Scanner) (*LastTelemetryData, int, error) {
	lastData := &LastTelemetryData{}
	lineNum := 0
	lineCount := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		data, err := ParseTelemetryLine(line, lineNum)
		if err != nil || data == nil {
			continue
		}

		lineCount++
		UpdateLastTelemetryData(data, lastData)
	}

	if err := scanner.Err(); err != nil {
		return nil, lineCount, err
	}

	slog.Info("Finished reading file", "total_lines", lineCount)
	return lastData, lineCount, nil
}

func SendLastTelemetryData(lastData *LastTelemetryData, config *config.Config, stats *stats.SendStats) {
	slog.Info("Sending last instances to OTel Collector")

	if lastData.Traces != nil {
		payload := map[string]any{
			resourceSpansField: lastData.Traces[resourceSpansField],
		}
		if err := sender.SendToOTel(config.OtelEndpoint, payload, s.TelemetryTraces, stats); err != nil {
			slog.Error("Failed to send traces", "error", err)
		}
	}

	if lastData.Logs != nil {
		payload := map[string]any{
			resourceLogsField: lastData.Logs[resourceLogsField],
		}
		if err := sender.SendToOTel(config.OtelLogsEndpoint, payload, s.TelemetryLogs, stats); err != nil {
			slog.Error("Failed to send logs", "error", err)
		}
	}

	if lastData.Metrics != nil {
		payload := map[string]any{
			resourceMetricsField: lastData.Metrics[resourceMetricsField],
		}
		if err := sender.SendToOTel(config.OtelMetricsEndpoint, payload, s.TelemetryMetrics, stats); err != nil {
			slog.Error("Failed to send metrics", "error", err)
		}
	}

	stats.PrintSummary()
}

func IngestTelemetry(filePath string, cfg *config.Config) error {
	slog.Info("Reading telemetry data", "file", filePath)
	if cfg.SendAll {
		slog.Info("Mode: Sending all telemetry lines")
	} else {
		slog.Info("Mode: Scanning file to find last instances of each telemetry type")
	}

	file, scanner, err := OpenTelemetryFile(filePath, cfg.MaxBufferCapacity)
	if err != nil {
		return err
	}
	defer file.Close()

	stats := &stats.SendStats{}

	if cfg.SendAll {
		return ProcessFileInSendAllMode(scanner, cfg, stats)
	}

	lastData, _, err := ProcessFileInLastMode(scanner)
	if err != nil {
		return &FileReadError{FilePath: filePath, Err: err}
	}

	SendLastTelemetryData(lastData, cfg, stats)
	return nil
}

func worker(id int, jobs <-chan s.TelemetryJob, wg *sync.WaitGroup, stats *stats.SendStats) {
	defer wg.Done()
	for job := range jobs {
		if err := sender.SendToOTel(job.Endpoint, job.Payload, job.TelemetryType, stats); err != nil {
			slog.Error("Worker failed to send telemetry", "worker", id, "type", job.TelemetryType, "line", job.LineNum, "error", err)
		}
	}
}
