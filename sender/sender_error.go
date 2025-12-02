package sender

import "fmt"

// JSONMarshalError represents an error when marshaling JSON fails
type JSONMarshalError struct {
	Err error
}

func (e *JSONMarshalError) Error() string {
	return fmt.Sprintf("failed to marshal JSON: %v", e.Err)
}

func (e *JSONMarshalError) Unwrap() error {
	return e.Err
}

// HTTPRequestError represents an error when sending an HTTP request fails
type HTTPRequestError struct {
	Endpoint string
	Err      error
}

func (e *HTTPRequestError) Error() string {
	return fmt.Sprintf("failed to send request to %s: %v", e.Endpoint, e.Err)
}

func (e *HTTPRequestError) Unwrap() error {
	return e.Err
}
