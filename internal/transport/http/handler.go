package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

type statusError struct {
	status int
	msg    string
}

func (e *statusError) Error() string { return e.msg }

// badRequest wraps msg as a 400 error for handle to pass to the client.
func badRequest(msg string) error { return &statusError{status: http.StatusBadRequest, msg: msg} }

// handle turns a decode(JSON) -> call use case -> encode(JSON) round trip
// into an http.HandlerFunc, so a new endpoint only supplies its request and
// response types plus a use-case function.
func handle[Req, Resp any](successStatus int, fn func(ctx context.Context, req Req) (Resp, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Req
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		resp, err := fn(r.Context(), req)
		if err != nil {
			var se *statusError
			if errors.As(err, &se) {
				writeError(w, se.status, se.msg)
				return
			}
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(successStatus)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
