package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func validEvent() TripEvent {
	return TripEvent{
		EventID:   "1",
		TripID:    "1",
		DriverID:  "1",
		Type:      "REQUEST",
		Zone:      "SING",
		Lat:       10.0,
		Lng:       11.0,
		Timestamp: "2026-01-01T12:00:00Z",
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func TestRequestPipeline(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
		wantQueued  bool
	}{
		{"valid event", "application/json", mustJSON(t, validEvent()), http.StatusOK, true},
		{"wrong content type", "text/plain", mustJSON(t, validEvent()), http.StatusUnsupportedMediaType, false},
		{"missing content type", "", mustJSON(t, validEvent()), http.StatusUnsupportedMediaType, false},
		{"malformed json", "application/json", `{"tripId":`, http.StatusBadRequest, false},
		{"empty body", "application/json", ``, http.StatusBadRequest, false},
		{"wrong field type", "application/json", `{"tripId":"abc"}`, http.StatusBadRequest, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := &Gateway{jobs: make(chan TripEvent, 1)}
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))

			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}

			rec := httptest.NewRecorder()

			g.handle_request(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %q)", rec.Code, tc.wantStatus, rec.Body.String())
			}

			select {
			case got := <-g.jobs:
				if !tc.wantQueued {
					t.Fatalf("unexpected JOb queued %+v", got)
				}
				if want := validEvent(); want != got {
					t.Errorf("wanted job  %+v queued but got %+v instead", want, got)
				}

			case <-time.After(200 * time.Millisecond):
				if tc.wantQueued {
					t.Fatal("expected a job to be queued, none arrived")
				}
			}

		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*TripEvent)
		wantErr bool
	}{
		{"valid", func(e *TripEvent) {}, false},
		{"missing trip id", func(e *TripEvent) { e.TripID = "1" }, true},
		{"missing driver id", func(e *TripEvent) { e.DriverID = "1" }, true},
		{"unknown type", func(e *TripEvent) { e.Type = "CANCEL" }, true},
		{"lowercase type", func(e *TripEvent) { e.Type = "request" }, true},
		{"lat too high", func(e *TripEvent) { e.Lat = 90.1 }, true},
		{"lat too low", func(e *TripEvent) { e.Lat = -90.1 }, true},
		{"lng too high", func(e *TripEvent) { e.Lng = 180.1 }, true},
		{"lng too low", func(e *TripEvent) { e.Lng = -180.1 }, true},
		{"lat/lng at bounds", func(e *TripEvent) { e.Lat, e.Lng = 90, -180 }, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent()
			tc.mutate(&e)
			if err := validate(e); (err != nil) != tc.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
