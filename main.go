package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultMaxBodyBytes = 1 << 20
	timestampLayout     = "2006-01-02T15:04:05.000Z"
)

var errIdempotencyConflict = errors.New("idempotency key already used for another event")

type event struct {
	ID             int64           `json:"-"`
	IdempotencyKey string          `json:"-"`
	Source         string          `json:"source"`
	Type           string          `json:"type"`
	Timestamp      string          `json:"timestamp"`
	Payload        json.RawMessage `json:"payload"`
}

type eventCursor struct {
	Timestamp time.Time
	ID        int64
}

type listOptions struct {
	Source    *string
	EventType *string
	Before    *eventCursor
	Limit     int
}

type eventStore interface {
	insert(context.Context, event) (event, bool, error)
	list(context.Context, listOptions) ([]event, error)
	authenticate(context.Context, string) (bool, error)
	ping(context.Context) error
}

type app struct {
	store        eventStore
	maxBodyBytes int64
	logger       *slog.Logger
}

func newApp(store eventStore, maxBodyBytes int64, logger *slog.Logger) *app {
	return &app{store: store, maxBodyBytes: maxBodyBytes, logger: logger}
}

func (a *app) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodGet && r.URL.Path == "/ready":
		a.readiness(w, r)
	case r.URL.Path == "/events":
		authorized, err := a.authorized(r)
		if err != nil {
			a.logger.Error("API key lookup failed", "error", err, "request_id", requestID(r.Context()))
			writeError(w, http.StatusServiceUnavailable, "service unavailable")
			return
		}
		if !authorized {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		switch r.Method {
		case http.MethodPost:
			a.createEvent(w, r)
		case http.MethodGet:
			a.listEvents(w, r)
		default:
			w.Header().Set("Allow", "GET, POST")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (a *app) authorized(r *http.Request) (bool, error) {
	provided, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || provided == "" {
		return false, nil
	}
	return a.store.authenticate(r.Context(), provided)
}

func (a *app) readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.store.ping(ctx); err != nil {
		a.logger.Error("readiness check failed", "error", err, "request_id", requestID(r.Context()))
		writeError(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *app) createEvent(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "content type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, a.maxBodyBytes)

	var input struct {
		Source    *string         `json:"source"`
		Type      *string         `json:"type"`
		Timestamp *string         `json:"timestamp"`
		Payload   json.RawMessage `json:"payload"`
	}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&input); err != nil {
		a.writeDecodeError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		a.writeDecodeError(w, err)
		return
	}

	if input.Source == nil || strings.TrimSpace(*input.Source) == "" {
		writeError(w, http.StatusBadRequest, "source must be a non-empty string")
		return
	}
	if len(strings.TrimSpace(*input.Source)) > 255 {
		writeError(w, http.StatusBadRequest, "source must be at most 255 characters")
		return
	}
	if input.Type == nil || strings.TrimSpace(*input.Type) == "" {
		writeError(w, http.StatusBadRequest, "type must be a non-empty string")
		return
	}
	if len(strings.TrimSpace(*input.Type)) > 255 {
		writeError(w, http.StatusBadRequest, "type must be at most 255 characters")
		return
	}
	if input.Timestamp == nil {
		writeError(w, http.StatusBadRequest, "timestamp must be a valid timestamp string")
		return
	}
	timestamp, err := parseTimestamp(*input.Timestamp)
	if err != nil {
		writeError(w, http.StatusBadRequest, "timestamp must be a valid timestamp string")
		return
	}
	if input.Payload == nil {
		writeError(w, http.StatusBadRequest, "body must be a valid event")
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) > 255 {
		writeError(w, http.StatusBadRequest, "idempotency key must be at most 255 characters")
		return
	}

	created := event{
		IdempotencyKey: idempotencyKey,
		Source:         strings.TrimSpace(*input.Source),
		Type:           strings.TrimSpace(*input.Type),
		Timestamp:      timestamp.UTC().Format(timestampLayout),
		Payload:        input.Payload,
	}
	created, inserted, err := a.store.insert(r.Context(), created)
	if errors.Is(err, errIdempotencyConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		a.logger.Error("insert event failed", "error", err, "request_id", requestID(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	status := http.StatusCreated
	if !inserted {
		status = http.StatusOK
	}
	writeJSON(w, status, created)
}

func (a *app) writeDecodeError(w http.ResponseWriter, err error) {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	var typeError *json.UnmarshalTypeError
	if errors.As(err, &typeError) {
		writeError(w, http.StatusBadRequest, "body must be a valid event")
		return
	}
	writeError(w, http.StatusBadRequest, "body must be valid JSON")
}

func (a *app) listEvents(w http.ResponseWriter, r *http.Request) {
	limit := 100
	query := r.URL.Query()
	if values, present := query["limit"]; present {
		parsed, err := strconv.Atoi(values[0])
		if err != nil || parsed < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = min(parsed, 1000)
	}
	var source, eventType *string
	if values, present := query["source"]; present {
		source = &values[0]
	}
	if values, present := query["type"]; present {
		eventType = &values[0]
	}
	var before *eventCursor
	if value := query.Get("cursor"); value != "" {
		decoded, err := decodeCursor(value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "cursor must be valid")
			return
		}
		before = &decoded
	}

	events, err := a.store.list(r.Context(), listOptions{
		Source: source, EventType: eventType, Before: before, Limit: limit + 1,
	})
	if err != nil {
		a.logger.Error("query events failed", "error", err, "request_id", requestID(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	response := struct {
		Events     []event `json:"events"`
		NextCursor string  `json:"next_cursor,omitempty"`
	}{Events: events}
	if len(events) > limit {
		response.Events = events[:limit]
		last := response.Events[len(response.Events)-1]
		timestamp, _ := time.Parse(timestampLayout, last.Timestamp)
		response.NextCursor = encodeCursor(eventCursor{Timestamp: timestamp, ID: last.ID})
	}
	writeJSON(w, http.StatusOK, response)
}

func parseTimestamp(value string) (time.Time, error) {
	if timestamp, err := time.Parse(time.RFC3339, value); err == nil {
		return timestamp, nil
	}
	return time.Parse(time.DateOnly, value)
}

func encodeCursor(cursor eventCursor) string {
	value := cursor.Timestamp.UTC().Format(time.RFC3339Nano) + "|" + strconv.FormatInt(cursor.ID, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeCursor(value string) (eventCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return eventCursor{}, err
	}
	timestampValue, idValue, found := strings.Cut(string(decoded), "|")
	if !found {
		return eventCursor{}, errors.New("invalid cursor")
	}
	timestamp, err := time.Parse(time.RFC3339Nano, timestampValue)
	if err != nil {
		return eventCursor{}, err
	}
	id, err := strconv.ParseInt(idValue, 10, 64)
	if err != nil || id < 1 {
		return eventCursor{}, errors.New("invalid cursor")
	}
	return eventCursor{Timestamp: timestamp, ID: id}, nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type requestIDKey struct{}

func requestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (w *responseRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			bytes := make([]byte, 16)
			_, _ = rand.Read(bytes)
			id = hex.EncodeToString(bytes)
		}
		w.Header().Set("X-Request-ID", id)
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		logger.Info("request completed", "request_id", id, "method", r.Method, "path", r.URL.Path,
			"status", status, "duration_ms", time.Since(started).Milliseconds())
	})
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	startupTimeout := 10 * time.Second
	if len(os.Args) >= 2 && os.Args[1] == "migrate" {
		startupTimeout = 5 * time.Minute
	}
	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()
	store, err := newPostgresStore(startupCtx, databaseURL)
	if err != nil {
		return err
	}
	defer store.close()

	if len(os.Args) == 2 && os.Args[1] == "migrate" {
		return store.migrate(startupCtx)
	}
	if len(os.Args) == 3 && os.Args[1] == "create-api-key" {
		key, prefix, err := store.createAPIKey(startupCtx, os.Args[2])
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{
			"name": os.Args[2], "prefix": prefix, "key": key,
		})
	}
	if len(os.Args) == 3 && os.Args[1] == "revoke-api-key" {
		revoked, err := store.revokeAPIKey(startupCtx, os.Args[2])
		if err != nil {
			return err
		}
		if !revoked {
			return errors.New("API key not found or already revoked")
		}
		return nil
	}
	if len(os.Args) != 1 {
		return fmt.Errorf("usage: %s [migrate | create-api-key <name> | revoke-api-key <prefix>]", os.Args[0])
	}
	maxBodyBytes := int64(defaultMaxBodyBytes)
	if value := os.Getenv("MAX_BODY_BYTES"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 1 {
			return errors.New("MAX_BODY_BYTES must be a positive integer")
		}
		maxBodyBytes = parsed
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return errors.New("PORT must be an integer between 1 and 65535")
	}

	handler := loggingMiddleware(logger, newApp(store, maxBodyBytes, logger))
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("server listening", "address", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		logger.Info("server shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func main() {
	if err := run(); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
