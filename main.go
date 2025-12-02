package main

import (
	"log/slog"
	"os"

	"github.com/laiambryant/observability-utils/ingest_telemetry_go/config"
	"github.com/laiambryant/observability-utils/ingest_telemetry_go/processor"
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

func init() {
	rootCmd.Flags().StringVarP(&cfg.FilePath, "file", "f", "telemetry.json", "Path to telemetry JSON file")
	rootCmd.Flags().StringVar(&cfg.OtelEndpoint, "traces-endpoint", config.DEFAULT_OTEL_ENDPOINT, "OpenTelemetry traces endpoint")
	rootCmd.Flags().StringVar(&cfg.OtelLogsEndpoint, "logs-endpoint", config.DEFAULT_OTEL_LOGS_ENDPOINT, "OpenTelemetry logs endpoint")
	rootCmd.Flags().StringVar(&cfg.OtelMetricsEndpoint, "metrics-endpoint", config.DEFAULT_OTEL_METRICS_ENDPOINT, "OpenTelemetry metrics endpoint")
	rootCmd.Flags().IntVar(&cfg.MaxBufferCapacity, "max-buffer-capacity", 1024*1024, "Maximum buffer capacity in bytes for reading lines (default 1MB)")
	rootCmd.Flags().BoolVar(&cfg.SendAll, "sendAll", false, "Send all telemetry lines instead of only the last instance of each type")
	rootCmd.Flags().IntVar(&cfg.Workers, "workers", 10, "Number of concurrent workers for sending telemetry (only used with --sendAll)")
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
