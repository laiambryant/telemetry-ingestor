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

const TEMP_FILE_PATTERN = "zip-extract-*.tmp"

func ProcessZipFile(zipPath string, cfg *config.Config) (int, error) {
	slog.Info("Processing zip file", "file", zipPath)
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	matchedFiles := countMatchingFiles(reader, cfg)
	if matchedFiles == 0 {
		slog.Info("No matching files in zip", "zip", zipPath, "pattern", cfg.FilePattern)
		return 0, nil
	}
	slog.Info("Found matching files in zip", "zip", zipPath, "count", matchedFiles)
	processedCount := processMatchingFiles(reader, zipPath, matchedFiles, cfg)
	return processedCount, nil
}

func countMatchingFiles(reader *zip.ReadCloser, cfg *config.Config) int {
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
	return matchedFiles
}

func processMatchingFiles(reader *zip.ReadCloser, zipPath string, matchedFiles int, cfg *config.Config) int {
	processedCount := 0
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
			slog.Warn("Error processing file from zip", "file", file.Name, "zip", zipPath, "error", err)
			continue
		}
		processedCount++
	}
	return processedCount
}

func processZipEntry(file *zip.File, cfg *config.Config) error {
	rc, err := file.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	tmpFile, err := createTempfile()
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	if _, err := io.Copy(tmpFile, rc); err != nil {
		_ = tmpFile.Close()
		return err
	}
	slog.Info("unzipped file", "name", tmpFile.Name())
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return processor.IngestTelemetry(tmpFile.Name(), cfg)
}

func createTempfile() (file *os.File, err error) {
	tmpFile, err := os.CreateTemp("", TEMP_FILE_PATTERN)
	if err != nil {
		return nil, err
	}
	return tmpFile, nil
}
