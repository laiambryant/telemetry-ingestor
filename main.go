package main

import (
	"log/slog"
	"os"

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
	return processor.IngestFolder(folderPath, cfg)
}
