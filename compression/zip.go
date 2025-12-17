package compression

import (
	"archive/zip"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/laiambryant/telemetry-ingestor/config"
	"github.com/laiambryant/telemetry-ingestor/processor"
)

// ProcessZipFile reads files from a zip archive, filters by cfg.FilePattern, and ingests each matched entry.
// It returns the count of successfully processed (ingested) files.
func ProcessZipFile(zipPath string, cfg *config.Config) (int, error) {
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
		_ = tmpFile.Close()
		return err
	}
	slog.Info("unzipped file", "name", tmpFile.Name())
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return processor.IngestTelemetry(tmpPath, cfg)
}
