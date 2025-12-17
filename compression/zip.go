package compression

import (
	"archive/zip"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

const TEMP_FILE_PATTERN = "zip-extract-*.tmp"

type FileProcessor func(filePath string) error

func ExtractAndProcessZipFiles(zipPath string, filePattern string, processFile FileProcessor) (int, error) {
	slog.Info("Processing zip file", "file", zipPath)
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	matchedFiles := countMatchingFiles(reader, filePattern)
	if matchedFiles == 0 {
		slog.Info("No matching files in zip", "zip", zipPath, "pattern", filePattern)
		return 0, nil
	}

	slog.Info("Found matching files in zip", "zip", zipPath, "count", matchedFiles)
	processedCount := processMatchingFiles(reader, zipPath, matchedFiles, filePattern, processFile)
	return processedCount, nil
}

func countMatchingFiles(reader *zip.ReadCloser, pattern string) int {
	matchedFiles := 0
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}
		matched, err := filepath.Match(pattern, filepath.Base(file.Name))
		if err != nil {
			continue
		}
		if matched {
			matchedFiles++
		}
	}
	return matchedFiles
}

func processMatchingFiles(reader *zip.ReadCloser, zipPath string, matchedFiles int, pattern string, processFile FileProcessor) int {
	processedCount := 0
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}
		matched, err := filepath.Match(pattern, filepath.Base(file.Name))
		if err != nil || !matched {
			continue
		}
		slog.Info("Processing file from zip", "index", processedCount+1, "total", matchedFiles, "file", file.Name, "zip", zipPath)
		if err := processZipEntry(file, processFile); err != nil {
			slog.Warn("Error processing file from zip", "file", file.Name, "zip", zipPath, "error", err)
			continue
		}
		processedCount++
	}
	return processedCount
}

func processZipEntry(file *zip.File, processFile FileProcessor) error {
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

	return processFile(tmpFile.Name())
}

func createTempfile() (file *os.File, err error) {
	tmpFile, err := os.CreateTemp("", TEMP_FILE_PATTERN)
	if err != nil {
		return nil, err
	}
	return tmpFile, nil
}
