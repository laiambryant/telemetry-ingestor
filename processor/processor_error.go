package processor

import "fmt"

type FileNotFoundError struct {
	FilePath string
}

func (e *FileNotFoundError) Error() string {
	return fmt.Sprintf("file not found: %s", e.FilePath)
}

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
