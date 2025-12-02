package processor

import "fmt"

// FileNotFoundError represents an error when a file is not found
type FileNotFoundError struct {
	FilePath string
}

func (e *FileNotFoundError) Error() string {
	return fmt.Sprintf("file not found: %s", e.FilePath)
}

// FileOpenError represents an error when opening a file fails
type FileOpenError struct {
	FilePath string
	Err      error
}

func (e *FileOpenError) Error() string {
	return fmt.Sprintf("failed to open file %s: %v", e.FilePath, e.Err)
}

func (e *FileOpenError) Unwrap() error {
	return e.Err
}

// FileReadError represents an error when reading a file fails
type FileReadError struct {
	FilePath string
	Err      error
}

func (e *FileReadError) Error() string {
	return fmt.Sprintf("error reading file %s: %v", e.FilePath, e.Err)
}

func (e *FileReadError) Unwrap() error {
	return e.Err
}
