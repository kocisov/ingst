package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

type memoryStore struct {
	mu     sync.Mutex
	events []event
	nextID int64
}

func (s *memoryStore) insert(_ context.Context, item event) (event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if item.IdempotencyKey != "" {
		for _, existing := range s.events {
			if existing.IdempotencyKey != item.IdempotencyKey {
				continue
			}
			if existing.Source != item.Source || existing.Type != item.Type || existing.Timestamp != item.Timestamp || !bytes.Equal(existing.Payload, item.Payload) {
				return event{}, false, errIdempotencyConflict
			}
			return existing, false, nil
		}
	}
	s.nextID++
	item.ID = s.nextID
	s.events = append(s.events, item)
	slices.SortFunc(s.events, func(left, right event) int {
		if order := strings.Compare(right.Timestamp, left.Timestamp); order != 0 {
			return order
		}
		return int(right.ID - left.ID)
	})
	return item, true, nil
}

func (s *memoryStore) list(_ context.Context, options listOptions) ([]event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]event, 0)
	for _, item := range s.events {
		if options.Source != nil && item.Source != *options.Source || options.EventType != nil && item.Type != *options.EventType {
			continue
		}
		if options.Before != nil && (item.Timestamp > options.Before.Timestamp.Format(timestampLayout) ||
			item.Timestamp == options.Before.Timestamp.Format(timestampLayout) && item.ID >= options.Before.ID) {
			continue
		}
		result = append(result, item)
		if len(result) == options.Limit {
			break
		}
	}
	return result, nil
}

func (s *memoryStore) ping(context.Context) error { return nil }

func (s *memoryStore) authenticate(_ context.Context, key string) (bool, error) {
	return key == "test-api-key-123", nil
}

func setup(t *testing.T) http.Handler {
	t.Helper()
	return newApp(&memoryStore{}, defaultMaxBodyBytes, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func request(t *testing.T, handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if strings.HasPrefix(target, "/events") {
		req.Header.Set("Authorization", "Bearer test-api-key-123")
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	handler.ServeHTTP(recorder, req)
	return recorder
}

func decodeJSON(t *testing.T, response *httptest.ResponseRecorder) any {
	t.Helper()
	var value any
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestIngestsAndRetrievesEvent(t *testing.T) {
	handler := setup(t)
	response := request(t, handler, http.MethodPost, "/events", `{
		"source":"checkout",
		"type":"order.created",
		"timestamp":"2026-08-14T12:00:00+02:00",
		"payload":{"orderId":42}
	}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body.String())
	}

	wantEvent := map[string]any{
		"source": "checkout", "type": "order.created",
		"timestamp": "2026-08-14T10:00:00.000Z",
		"payload":   map[string]any{"orderId": float64(42)},
	}
	if got := decodeJSON(t, response); !equalJSON(got, wantEvent) {
		t.Fatalf("event = %#v, want %#v", got, wantEvent)
	}

	list := request(t, handler, http.MethodGet, "/events", "")
	wantList := map[string]any{"events": []any{wantEvent}}
	if got := decodeJSON(t, list); !equalJSON(got, wantList) {
		t.Fatalf("events = %#v, want %#v", got, wantList)
	}
}

func TestFiltersEventsBySourceAndType(t *testing.T) {
	handler := setup(t)
	for _, body := range []string{
		`{"source":"a","type":"created","timestamp":"2026-01-01","payload":1}`,
		`{"source":"b","type":"created","timestamp":"2026-01-02","payload":2}`,
		`{"source":"a","type":"deleted","timestamp":"2026-01-03","payload":3}`,
	} {
		response := request(t, handler, http.MethodPost, "/events", body)
		if response.Code != http.StatusCreated {
			t.Fatalf("status = %d: %s", response.Code, response.Body.String())
		}
	}

	response := request(t, handler, http.MethodGet, "/events?source=a&type=created", "")
	want := map[string]any{"events": []any{map[string]any{
		"source": "a", "type": "created", "timestamp": "2026-01-01T00:00:00.000Z", "payload": float64(1),
	}}}
	if got := decodeJSON(t, response); !equalJSON(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
}

func TestRejectsInvalidEvents(t *testing.T) {
	handler := setup(t)
	response := request(t, handler, http.MethodPost, "/events", `{"source":"api","type":"request","payload":{}}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	want := map[string]any{"error": "timestamp must be a valid timestamp string"}
	if got := decodeJSON(t, response); !equalJSON(got, want) {
		t.Fatalf("response = %#v, want %#v", got, want)
	}
}

func TestHealthAndUnknownRoutes(t *testing.T) {
	handler := setup(t)
	health := request(t, handler, http.MethodGet, "/health", "")
	if got := decodeJSON(t, health); !equalJSON(got, map[string]any{"status": "ok"}) {
		t.Fatalf("health = %#v", got)
	}
	missing := request(t, handler, http.MethodGet, "/missing", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestRequiresAuthentication(t *testing.T) {
	handler := setup(t)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/events", nil))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestRejectsLargeBodies(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := newApp(&memoryStore{}, 32, logger)
	response := request(t, handler, http.MethodPost, "/events", strings.Repeat(" ", 33))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
}

func TestIdempotentIngestion(t *testing.T) {
	handler := setup(t)
	body := `{"source":"api","type":"request","timestamp":"2026-01-01","payload":{"ok":true}}`

	first := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	first.Header.Set("Authorization", "Bearer test-api-key-123")
	first.Header.Set("Content-Type", "application/json")
	first.Header.Set("Idempotency-Key", "request-1")
	firstResponse := httptest.NewRecorder()
	handler.ServeHTTP(firstResponse, first)

	second := first.Clone(context.Background())
	second.Body = io.NopCloser(strings.NewReader(body))
	secondResponse := httptest.NewRecorder()
	handler.ServeHTTP(secondResponse, second)
	if firstResponse.Code != http.StatusCreated || secondResponse.Code != http.StatusOK {
		t.Fatalf("statuses = %d, %d; want %d, %d", firstResponse.Code, secondResponse.Code, http.StatusCreated, http.StatusOK)
	}
}

func TestCursorPagination(t *testing.T) {
	handler := setup(t)
	for _, timestamp := range []string{"2026-01-01", "2026-01-02", "2026-01-03"} {
		body := `{"source":"api","type":"request","timestamp":"` + timestamp + `","payload":{}}`
		response := request(t, handler, http.MethodPost, "/events", body)
		if response.Code != http.StatusCreated {
			t.Fatalf("status = %d: %s", response.Code, response.Body.String())
		}
	}
	first := request(t, handler, http.MethodGet, "/events?limit=2", "")
	var page struct {
		Events     []event `json:"events"`
		NextCursor string  `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.NextCursor == "" {
		t.Fatalf("first page = %#v", page)
	}
	second := request(t, handler, http.MethodGet, "/events?limit=2&cursor="+page.NextCursor, "")
	var nextPage struct {
		Events []event `json:"events"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &nextPage); err != nil {
		t.Fatal(err)
	}
	if len(nextPage.Events) != 1 || nextPage.Events[0].Timestamp != "2026-01-01T00:00:00.000Z" {
		t.Fatalf("second page = %#v", nextPage)
	}
}

func equalJSON(left, right any) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}
