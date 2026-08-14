package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestPostgresStore(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := newPostgresStore(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := store.migrate(ctx); err != nil {
		t.Fatal(err)
	}
	apiKey, apiKeyPrefix, err := store.createAPIKey(ctx, "integration test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.pool.Exec(cleanupCtx, `DELETE FROM api_keys WHERE key_prefix = $1`, apiKeyPrefix)
	})
	active, err := store.authenticate(ctx, apiKey)
	if err != nil || !active {
		t.Fatalf("authenticate = %t, %v", active, err)
	}

	key := "integration-" + time.Now().UTC().Format("20060102150405.000000000")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.pool.Exec(cleanupCtx, `DELETE FROM events WHERE idempotency_key = $1`, key)
	})
	input := event{
		IdempotencyKey: key,
		Source:         key,
		Type:           "event.created",
		Timestamp:      "2026-08-14T10:00:00.000Z",
		Payload:        json.RawMessage(`{"ok":true}`),
	}

	created, inserted, err := store.insert(ctx, input)
	if err != nil || !inserted || created.ID == 0 {
		t.Fatalf("insert = %#v, %t, %v", created, inserted, err)
	}
	repeated, inserted, err := store.insert(ctx, input)
	if err != nil || inserted || repeated.ID != created.ID {
		t.Fatalf("repeat = %#v, %t, %v", repeated, inserted, err)
	}

	source := input.Source
	events, err := store.list(ctx, listOptions{Source: &source, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[0].ID != created.ID {
		t.Fatalf("events = %#v", events)
	}
	if err := store.ping(ctx); err != nil {
		t.Fatal(err)
	}
	revoked, err := store.revokeAPIKey(ctx, apiKeyPrefix)
	if err != nil || !revoked {
		t.Fatalf("revoke = %t, %v", revoked, err)
	}
	active, err = store.authenticate(ctx, apiKey)
	if err != nil || active {
		t.Fatalf("authenticate revoked key = %t, %v", active, err)
	}
}
