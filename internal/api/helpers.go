package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

// structured error body return for 4xx errors.
type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`         // for user message
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

func writeError(w http.ResponseWriter, status int, code, message, field string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message, Field: field})
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "invalid_content_type", "Content-Type must be application/json", "")
		return false
	}
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error(), "")
		return false
	}
	return true
}

// distinguish user (400) from system errors (500).
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
