package processor

import (
	"errors"
	"fmt"
	"testing"
)

func TestFileNotFoundError(t *testing.T) {
	filePath := "/path/to/missing/file.json"
	err := &FileNotFoundError{FilePath: filePath}
	expected := fmt.Sprintf("file not found: %s", filePath)
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}
}

func TestFileOpenError(t *testing.T) {
	filePath := "/path/to/file.json"
	innerErr := errors.New("permission denied")
	err := &FileOpenError{
		FilePath: filePath,
		Err:      innerErr,
	}
	expectedMsg := fmt.Sprintf("failed to open file %s: %v", filePath, innerErr)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
	if unwrapped := err.Unwrap(); unwrapped != innerErr {
		t.Errorf("Expected Unwrap to return inner error, got %v", unwrapped)
	}
}

func TestFileReadError(t *testing.T) {
	filePath := "/path/to/file.json"
	innerErr := errors.New("unexpected EOF")
	err := &FileReadError{
		FilePath: filePath,
		Err:      innerErr,
	}
	expectedMsg := fmt.Sprintf("error reading file %s: %v", filePath, innerErr)
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
	if unwrapped := err.Unwrap(); unwrapped != innerErr {
		t.Errorf("Expected Unwrap to return inner error, got %v", unwrapped)
	}
}

func TestErrorInterfaceImplementation(t *testing.T) {
	var _ error = (*FileNotFoundError)(nil)
	var _ error = (*FileOpenError)(nil)
	var _ error = (*FileReadError)(nil)
}
