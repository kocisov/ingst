package ingst

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCreateEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" || r.Method != http.MethodPost {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization = %q", got)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "order-42-created" {
			t.Fatalf("idempotency key = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"source":"checkout","type":"order.created","timestamp":"2026-08-14T10:00:00.000Z","payload":{"orderId":42}}`))
	}))
	defer server.Close()
	client := newTestClient(t, server.URL+"/v1")

	event, err := client.CreateEvent(context.Background(), EventInput{
		Source: "checkout", Type: "order.created",
		Timestamp: time.Date(2026, 8, 14, 10, 0, 0, 0, time.UTC),
		Payload:   map[string]int{"orderId": 42},
	}, &CreateEventOptions{IdempotencyKey: "order-42-created"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := DecodePayload[struct {
		OrderID int `json:"orderId"`
	}](event)
	if err != nil || payload.OrderID != 42 {
		t.Fatalf("payload = %#v, %v", payload, err)
	}
}

func TestListAndIterateEvents(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			if r.URL.Query().Get("source") != "checkout" || r.URL.Query().Get("limit") != "1" {
				t.Fatalf("query = %q", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{"events":[{"source":"checkout","type":"first","timestamp":"2026-08-14T10:00:00.000Z","payload":null}],"next_cursor":"page-2"}`))
			return
		}
		if r.URL.Query().Get("cursor") != "page-2" {
			t.Fatalf("cursor = %q", r.URL.Query().Get("cursor"))
		}
		_, _ = w.Write([]byte(`{"events":[{"source":"checkout","type":"second","timestamp":"2026-08-14T09:00:00.000Z","payload":null},{"source":"checkout","type":"third","timestamp":"2026-08-14T08:00:00.000Z","payload":null}]}`))
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)

	iterator := client.IterateEvents(context.Background(), ListEventsOptions{Source: "checkout", Limit: 1})
	var types []string
	for iterator.Next() {
		types = append(types, iterator.Event().Type)
	}
	if err := iterator.Err(); err != nil {
		t.Fatal(err)
	}
	if len(types) != 3 || types[0] != "first" || types[1] != "second" || types[2] != "third" {
		t.Fatalf("types = %#v", types)
	}
}

func TestHealthDoesNotAuthenticate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Fatal("health request included authorization")
		}
		_ = json.NewEncoder(w).Encode(Status{Status: "ok"})
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)

	status, err := client.Health(context.Background())
	if err != nil || status.Status != "ok" {
		t.Fatalf("health = %#v, %v", status, err)
	}
}

func TestAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Request-ID", "request-1")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)

	_, err := client.ListEvents(context.Background(), ListEventsOptions{})
	var apiError *APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("error = %T %v", err, err)
	}
	if apiError.StatusCode != http.StatusUnauthorized || apiError.RequestID != "request-1" || apiError.Error() != "unauthorized" {
		t.Fatalf("API error = %#v", apiError)
	}
}

func newTestClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	client, err := NewClient(baseURL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	return client
}
