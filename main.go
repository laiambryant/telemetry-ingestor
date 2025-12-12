package main

import (
	"archive/zip"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/laiambryant/telemetry-ingestor/config"
	"github.com/laiambryant/telemetry-ingestor/processor"
	"github.com/spf13/cobra"
)

var cfg = config.NewConfig()

var rootCmd = &cobra.Command{
	Use:   "ingest_telemetry [file]",
	Short: "Ingest telemetry data to OpenTelemetry Collector",
	Long: `Reads OTLP format telemetry data from JSON Lines files and sends it to an OpenTelemetry Collector.
Finds and sends the last instance of each telemetry type (traces, logs, metrics).`,
	Args: cobra.MaximumNArgs(1),
	RunE: runIngest,
}

var folderCmd = &cobra.Command{
	Use:   "folder [directory]",
	Short: "Ingest telemetry data from all files in a folder",
	Long: `Reads OTLP format telemetry data from all JSON Lines files in a directory and sends it to an OpenTelemetry Collector.
Processes each file in the folder the same way as the root command. Also supports reading files from zip archives.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runIngestFolder,
}

func init() {
	rootCmd.Flags().StringVarP(&cfg.FilePath, "file", "f", "telemetry.json", "Path to telemetry JSON file")
	rootCmd.Flags().StringVar(&cfg.OtelEndpoint, "traces-endpoint", config.DEFAULT_OTEL_ENDPOINT, "OpenTelemetry traces endpoint")
	rootCmd.Flags().StringVar(&cfg.OtelLogsEndpoint, "logs-endpoint", config.DEFAULT_OTEL_LOGS_ENDPOINT, "OpenTelemetry logs endpoint")
	rootCmd.Flags().StringVar(&cfg.OtelMetricsEndpoint, "metrics-endpoint", config.DEFAULT_OTEL_METRICS_ENDPOINT, "OpenTelemetry metrics endpoint")
	rootCmd.Flags().IntVar(&cfg.MaxBufferCapacity, "max-buffer-capacity", 1024*1024, "Maximum buffer capacity in bytes for reading lines (default 1MB)")
	rootCmd.Flags().BoolVar(&cfg.SendAll, "sendAll", false, "Send all telemetry lines instead of only the last instance of each type")
	rootCmd.Flags().IntVar(&cfg.Workers, "workers", 10, "Number of concurrent workers for sending telemetry (only used with --sendAll)")
	rootCmd.AddCommand(folderCmd)
	folderCmd.Flags().StringVarP(&cfg.FilePath, "folder", "d", ".", "Path to folder containing telemetry JSON files")
	folderCmd.Flags().StringVar(&cfg.OtelEndpoint, "traces-endpoint", config.DEFAULT_OTEL_ENDPOINT, "OpenTelemetry traces endpoint")
	folderCmd.Flags().StringVar(&cfg.OtelLogsEndpoint, "logs-endpoint", config.DEFAULT_OTEL_LOGS_ENDPOINT, "OpenTelemetry logs endpoint")
	folderCmd.Flags().StringVar(&cfg.OtelMetricsEndpoint, "metrics-endpoint", config.DEFAULT_OTEL_METRICS_ENDPOINT, "OpenTelemetry metrics endpoint")
	folderCmd.Flags().IntVar(&cfg.MaxBufferCapacity, "max-buffer-capacity", 1024*1024, "Maximum buffer capacity in bytes for reading lines (default 1MB)")
	folderCmd.Flags().BoolVar(&cfg.SendAll, "sendAll", false, "Send all telemetry lines instead of only the last instance of each type")
	folderCmd.Flags().IntVar(&cfg.Workers, "workers", 10, "Number of concurrent workers for sending telemetry (only used with --sendAll)")
	folderCmd.Flags().StringVar(&cfg.FilePattern, "pattern", "*.json", "File pattern to match (e.g., *.json, telemetry_*.jsonl)")
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runIngest(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		cfg.FilePath = args[0]
	}
	return processor.IngestTelemetry(cfg.FilePath, cfg)
}

func runIngestFolder(cmd *cobra.Command, args []string) error {
	folderPath := cfg.FilePath
	if len(args) > 0 {
		folderPath = args[0]
	}
	var files []string
	var zipFiles []string
	err := filepath.WalkDir(folderPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			slog.Warn("Error accessing path", "path", path, "error", err)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(path), ".zip") {
			zipFiles = append(zipFiles, path)
			return nil
		}
		matched, err := filepath.Match(cfg.FilePattern, filepath.Base(path))
		if err != nil {
			return nil
		}
		if matched {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		slog.Error("Error walking directory", "folder", folderPath, "error", err)
		return err
	}
	if len(files) == 0 && len(zipFiles) == 0 {
		slog.Warn("No files found matching pattern", "folder", folderPath, "pattern", cfg.FilePattern)
		return nil
	}
	totalFiles := len(files)
	filesProcessed := 0
	if len(files) > 0 {
		slog.Info("Processing folder", "folder", folderPath, "pattern", cfg.FilePattern, "file_count", len(files))
		for i, file := range files {
			slog.Info("Processing file", "index", i+1, "total", len(files), "file", file)
			if err := processor.IngestTelemetry(file, cfg); err != nil {
				slog.Error("Error processing file", "file", file, "error", err)
				continue
			}
			filesProcessed++
		}
	}
	if len(zipFiles) > 0 {
		slog.Info("Found zip files", "zip_count", len(zipFiles))
		for _, zipFile := range zipFiles {
			processedCount, err := processZipFile(zipFile, cfg)
			if err != nil {
				slog.Error("Error processing zip file", "file", zipFile, "error", err)
				continue
			}
			totalFiles += processedCount
			filesProcessed += processedCount
		}
	}

	slog.Info("Folder processing complete", "total_files", totalFiles, "processed", filesProcessed)
	return nil
}

func processZipFile(zipPath string, cfg *config.Config) (int, error) {
	slog.Info("Processing zip file", "file", zipPath)
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	processedCount := 0
	matchedFiles := 0
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}
		matched, err := filepath.Match(cfg.FilePattern, filepath.Base(file.Name))
		if err != nil {
			continue
		}
		if matched {
			matchedFiles++
		}
	}
	if matchedFiles == 0 {
		slog.Info("No matching files in zip", "zip", zipPath, "pattern", cfg.FilePattern)
		return 0, nil
	}
	slog.Info("Found matching files in zip", "zip", zipPath, "count", matchedFiles)
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}
		matched, err := filepath.Match(cfg.FilePattern, filepath.Base(file.Name))
		if err != nil || !matched {
			continue
		}
		slog.Info("Processing file from zip", "index", processedCount+1, "total", matchedFiles, "file", file.Name, "zip", zipPath)
		if err := processZipEntry(file, cfg); err != nil {
			slog.Error("Error processing file from zip", "file", file.Name, "zip", zipPath, "error", err)
			continue
		}
		processedCount++
	}
	return processedCount, nil
}

func processZipEntry(file *zip.File, cfg *config.Config) error {
	rc, err := file.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	tmpFile, err := os.CreateTemp("", "zip-extract-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	if _, err := io.Copy(tmpFile, rc); err != nil {
		tmpFile.Close()
		return err
	}
	slog.Info("unzipped file", "name", tmpFile.Name())
	tmpFile.Close()
	return processor.IngestTelemetry(tmpPath, cfg)
}
