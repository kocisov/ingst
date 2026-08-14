package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresStore struct {
	pool *pgxpool.Pool
}

func newPostgresStore(ctx context.Context, databaseURL string) (*postgresStore, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("configure database: %w", err)
	}
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("configure database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &postgresStore{pool: pool}, nil
}

func (s *postgresStore) close() {
	s.pool.Close()
}

func (s *postgresStore) ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *postgresStore) migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(684252417)`); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("initialize migrations: %w", err)
	}

	migrations := []struct {
		version int
		sql     string
	}{
		{1, `
			CREATE TABLE IF NOT EXISTS events (
				id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				idempotency_key VARCHAR(255),
				source VARCHAR(255) NOT NULL,
				type VARCHAR(255) NOT NULL,
				timestamp TIMESTAMPTZ NOT NULL,
				payload JSONB NOT NULL
			);
			ALTER TABLE events ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
			CREATE UNIQUE INDEX IF NOT EXISTS events_idempotency_key_idx
				ON events(idempotency_key) WHERE idempotency_key IS NOT NULL;
			CREATE INDEX IF NOT EXISTS events_timestamp_id_idx ON events(timestamp DESC, id DESC);
			CREATE INDEX IF NOT EXISTS events_source_type_timestamp_id_idx
				ON events(source, type, timestamp DESC, id DESC);
		`},
		{2, `
			CREATE TABLE api_keys (
				id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				key_prefix VARCHAR(32) NOT NULL UNIQUE,
				key_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(key_hash) = 32),
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				revoked_at TIMESTAMPTZ
			);
		`},
	}
	for _, migration := range migrations {
		var applied bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, migration.version,
		).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %d: %w", migration.version, err)
		}
		if applied {
			continue
		}
		if _, err := tx.Exec(ctx, migration.sql); err != nil {
			return fmt.Errorf("apply migration %d: %w", migration.version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, migration.version); err != nil {
			return fmt.Errorf("record migration %d: %w", migration.version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func (s *postgresStore) authenticate(ctx context.Context, key string) (bool, error) {
	hash := sha256.Sum256([]byte(key))
	var active bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL
		)
	`, hash[:]).Scan(&active)
	return active, err
}

func (s *postgresStore) createAPIKey(ctx context.Context, name string) (string, string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 255 {
		return "", "", errors.New("API key name must contain between 1 and 255 characters")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", fmt.Errorf("generate API key: %w", err)
	}
	key := "ingst_" + base64.RawURLEncoding.EncodeToString(secret)
	prefix := key[:14]
	hash := sha256.Sum256([]byte(key))
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO api_keys (name, key_prefix, key_hash) VALUES ($1, $2, $3)
	`, name, prefix, hash[:]); err != nil {
		return "", "", fmt.Errorf("store API key: %w", err)
	}
	return key, prefix, nil
}

func (s *postgresStore) revokeAPIKey(ctx context.Context, prefix string) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE api_keys SET revoked_at = now()
		WHERE key_prefix = $1 AND revoked_at IS NULL
	`, prefix)
	if err != nil {
		return false, fmt.Errorf("revoke API key: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

func (s *postgresStore) insert(ctx context.Context, item event) (event, bool, error) {
	if item.IdempotencyKey == "" {
		err := s.pool.QueryRow(ctx, `
			INSERT INTO events (source, type, timestamp, payload)
			VALUES ($1, $2, $3::timestamptz, $4::jsonb)
			RETURNING id
		`, item.Source, item.Type, item.Timestamp, []byte(item.Payload)).Scan(&item.ID)
		return item, err == nil, err
	}

	err := s.pool.QueryRow(ctx, `
		INSERT INTO events (idempotency_key, source, type, timestamp, payload)
		VALUES ($1, $2, $3, $4::timestamptz, $5::jsonb)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id
	`, item.IdempotencyKey, item.Source, item.Type, item.Timestamp, []byte(item.Payload)).Scan(&item.ID)
	if err == nil {
		return item, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return event{}, false, err
	}

	var existing event
	var timestamp time.Time
	var payload []byte
	var matches bool
	err = s.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, source, type, timestamp, payload,
		       source = $2 AND type = $3 AND timestamp = $4::timestamptz AND payload = $5::jsonb
		FROM events
		WHERE idempotency_key = $1
	`, item.IdempotencyKey, item.Source, item.Type, item.Timestamp, []byte(item.Payload)).Scan(
		&existing.ID, &existing.IdempotencyKey, &existing.Source, &existing.Type, &timestamp, &payload, &matches,
	)
	if err != nil {
		return event{}, false, err
	}
	if !matches {
		return event{}, false, errIdempotencyConflict
	}
	existing.Timestamp = timestamp.UTC().Format(timestampLayout)
	existing.Payload = json.RawMessage(payload)
	return existing, false, nil
}

func (s *postgresStore) list(ctx context.Context, options listOptions) ([]event, error) {
	var beforeTimestamp, beforeID any
	if options.Before != nil {
		beforeTimestamp = options.Before.Timestamp
		beforeID = options.Before.ID
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, source, type, timestamp, payload
		FROM events
		WHERE ($1::text IS NULL OR source = $1)
		  AND ($2::text IS NULL OR type = $2)
		  AND ($3::timestamptz IS NULL OR (timestamp, id) < ($3::timestamptz, $4::bigint))
		ORDER BY timestamp DESC, id DESC
		LIMIT $5
	`, optionalString(options.Source), optionalString(options.EventType), beforeTimestamp, beforeID, options.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]event, 0)
	for rows.Next() {
		var item event
		var timestamp time.Time
		var payload []byte
		if err := rows.Scan(&item.ID, &item.Source, &item.Type, &timestamp, &payload); err != nil {
			return nil, err
		}
		item.Timestamp = timestamp.UTC().Format(timestampLayout)
		item.Payload = json.RawMessage(payload)
		events = append(events, item)
	}
	return events, rows.Err()
}

func optionalString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
