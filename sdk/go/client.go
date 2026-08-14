package ingst

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type EventInput struct {
	Source    string
	Type      string
	Timestamp time.Time
	Payload   any
}

type Event struct {
	Source    string
	Type      string
	Timestamp time.Time
	Payload   json.RawMessage
}

func DecodePayload[T any](event Event) (T, error) {
	var payload T
	err := json.Unmarshal(event.Payload, &payload)
	return payload, err
}

type CreateEventOptions struct {
	IdempotencyKey string
}

type ListEventsOptions struct {
	Source string
	Type   string
	Limit  int
	Cursor string
}

type EventPage struct {
	Events     []Event
	NextCursor string
}

type Status struct {
	Status string `json:"status"`
}

type APIError struct {
	StatusCode int
	Body       []byte
	RequestID  string
	message    string
}

func (e *APIError) Error() string {
	if e.message != "" {
		return e.message
	}
	return fmt.Sprintf("ingst request failed with status %d", e.StatusCode)
}

type Client struct {
	baseURL    *url.URL
	apiKey     string
	httpClient *http.Client
	headers    http.Header
}

type ClientOption func(*Client)

func WithHTTPClient(client *http.Client) ClientOption {
	return func(target *Client) {
		if client != nil {
			target.httpClient = client
		}
	}
}

func WithHeader(name, value string) ClientOption {
	return func(client *Client) {
		client.headers.Set(name, value)
	}
}

func NewClient(baseURL, apiKey string, options ...ClientOption) (*Client, error) {
	if apiKey == "" {
		return nil, errors.New("api key is required")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL: %w", err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, errors.New("base URL must be an absolute HTTP or HTTPS URL")
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return nil, errors.New("base URL must use HTTP or HTTPS")
	}
	if !strings.HasSuffix(parsedURL.Path, "/") {
		parsedURL.Path += "/"
	}

	client := &Client{
		baseURL:    parsedURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		headers:    make(http.Header),
	}
	for _, option := range options {
		option(client)
	}
	return client, nil
}

func (c *Client) CreateEvent(ctx context.Context, input EventInput, options *CreateEventOptions) (Event, error) {
	body, err := json.Marshal(struct {
		Source    string `json:"source"`
		Type      string `json:"type"`
		Timestamp string `json:"timestamp"`
		Payload   any    `json:"payload"`
	}{
		Source: input.Source, Type: input.Type,
		Timestamp: input.Timestamp.Format(time.RFC3339Nano), Payload: input.Payload,
	})
	if err != nil {
		return Event{}, fmt.Errorf("encode event: %w", err)
	}
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	if options != nil && options.IdempotencyKey != "" {
		headers.Set("Idempotency-Key", options.IdempotencyKey)
	}

	var response wireEvent
	if err := c.request(ctx, http.MethodPost, "events", bytes.NewReader(body), headers, true, &response); err != nil {
		return Event{}, err
	}
	return response.event()
}

func (c *Client) ListEvents(ctx context.Context, options ListEventsOptions) (EventPage, error) {
	if options.Limit < 0 {
		return EventPage{}, errors.New("limit must be a positive integer")
	}
	query := make(url.Values)
	if options.Source != "" {
		query.Set("source", options.Source)
	}
	if options.Type != "" {
		query.Set("type", options.Type)
	}
	if options.Limit > 0 {
		query.Set("limit", strconv.Itoa(options.Limit))
	}
	if options.Cursor != "" {
		query.Set("cursor", options.Cursor)
	}
	path := "events"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var response struct {
		Events     []wireEvent `json:"events"`
		NextCursor string      `json:"next_cursor"`
	}
	if err := c.request(ctx, http.MethodGet, path, nil, nil, true, &response); err != nil {
		return EventPage{}, err
	}
	events := make([]Event, len(response.Events))
	for index, item := range response.Events {
		event, err := item.event()
		if err != nil {
			return EventPage{}, err
		}
		events[index] = event
	}
	return EventPage{Events: events, NextCursor: response.NextCursor}, nil
}

func (c *Client) IterateEvents(ctx context.Context, options ListEventsOptions) *EventIterator {
	return &EventIterator{ctx: ctx, client: c, options: options}
}

func (c *Client) Health(ctx context.Context) (Status, error) {
	var status Status
	err := c.request(ctx, http.MethodGet, "health", nil, nil, false, &status)
	return status, err
}

func (c *Client) Readiness(ctx context.Context) (Status, error) {
	var status Status
	err := c.request(ctx, http.MethodGet, "ready", nil, nil, false, &status)
	return status, err
}

type EventIterator struct {
	ctx     context.Context
	client  *Client
	options ListEventsOptions
	events  []Event
	index   int
	current Event
	done    bool
	err     error
}

func (i *EventIterator) Next() bool {
	if i.err != nil {
		return false
	}
	if i.index < len(i.events) {
		i.current = i.events[i.index]
		i.index++
		return true
	}
	if i.done {
		return false
	}

	page, err := i.client.ListEvents(i.ctx, i.options)
	if err != nil {
		i.err = err
		return false
	}
	i.events = page.Events
	i.index = 0
	i.options.Cursor = page.NextCursor
	if page.NextCursor == "" {
		i.done = true
	}
	if len(i.events) == 0 {
		return false
	}
	i.current = i.events[0]
	i.index = 1
	return true
}

func (i *EventIterator) Event() Event {
	return i.current
}

func (i *EventIterator) Err() error {
	return i.err
}

func (c *Client) request(
	ctx context.Context,
	method, path string,
	body io.Reader,
	headers http.Header,
	authenticated bool,
	target any,
) error {
	reference, err := url.Parse(path)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL.ResolveReference(reference).String(), body)
	if err != nil {
		return err
	}
	request.Header = c.headers.Clone()
	for name, values := range headers {
		request.Header[name] = append([]string(nil), values...)
	}
	request.Header.Set("Accept", "application/json")
	if authenticated {
		request.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return newAPIError(response, responseBody)
	}
	if err := json.Unmarshal(responseBody, target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

type wireEvent struct {
	Source    string          `json:"source"`
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

func (e wireEvent) event() (Event, error) {
	timestamp, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil {
		return Event{}, fmt.Errorf("parse event timestamp: %w", err)
	}
	return Event{Source: e.Source, Type: e.Type, Timestamp: timestamp, Payload: e.Payload}, nil
}

func newAPIError(response *http.Response, body []byte) *APIError {
	var payload struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &payload)
	return &APIError{
		StatusCode: response.StatusCode,
		Body:       body,
		RequestID:  response.Header.Get("X-Request-ID"),
		message:    payload.Error,
	}
}
