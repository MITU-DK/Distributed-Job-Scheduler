// Package api — HTTP helpers.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

// errorResponse is the structured error body returned for 4xx errors.
// Structured errors let API clients handle specific error codes programmatically
// instead of parsing human-readable strings.
type errorResponse struct {
	Error   string `json:"error"`           // machine-readable code: "invalid_priority"
	Message string `json:"message"`         // human-readable description
	Field   string `json:"field,omitempty"` // which field caused the error, if applicable
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write_json_failed", "error", err)
	}
}

// writeError writes a structured errorResponse with the given status code.
func writeError(w http.ResponseWriter, status int, code, message, field string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message, Field: field})
}

// decodeBody decodes the JSON request body into dst.
// Returns false (and writes an appropriate error) if decoding fails.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType,
			"invalid_content_type",
			"Content-Type must be application/json", "")
		return false
	}
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error(), "")
		return false
	}
	return true
}

// isValidationError returns true if the error came from the job validation layer.
// This is a lightweight check to distinguish user errors (400) from system errors (500).
func isValidationError(err error) bool {
	// Errors from queue.validateJob are wrapped with "enqueue validation: ".
	return errors.Is(err, err) && len(err.Error()) > 0 &&
		(contains(err.Error(), "enqueue validation:") ||
			contains(err.Error(), "name is required") ||
			contains(err.Error(), "priority must be") ||
			contains(err.Error(), "payload is not valid JSON") ||
			contains(err.Error(), "max_retries must be"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && searchString(s, sub))
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
