package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

// pgUniqueViolation is the PostgreSQL SQLSTATE code for unique_violation.
const pgUniqueViolation = "23505"

var _ storage.IndexStore = (*IndexStore)(nil)

// IndexStore implements storage.IndexStore backed by PostgreSQL.
type IndexStore struct {
	pool *pgxpool.Pool
}

// NewIndexStore creates an IndexStore wrapping the given connection pool.
func NewIndexStore(pool *pgxpool.Pool) *IndexStore {
	return &IndexStore{pool: pool}
}

// --- response metadata ---

func (s *IndexStore) Put(ctx context.Context, meta model.ResponseMeta) error {
	isCheckpoint := 0
	if meta.CheckpointKey != nil {
		isCheckpoint = 1
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO responses
			(id, previous_response_id, chain_root_id, position, is_checkpoint,
			 owner_principal, model, status, created_at, expires_at, payload_key, checkpoint_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO UPDATE SET
			previous_response_id = EXCLUDED.previous_response_id,
			chain_root_id        = EXCLUDED.chain_root_id,
			position             = EXCLUDED.position,
			is_checkpoint        = EXCLUDED.is_checkpoint,
			owner_principal      = EXCLUDED.owner_principal,
			model                = EXCLUDED.model,
			status               = EXCLUDED.status,
			created_at           = EXCLUDED.created_at,
			expires_at           = EXCLUDED.expires_at,
			payload_key          = EXCLUDED.payload_key,
			checkpoint_key       = EXCLUDED.checkpoint_key`,
		meta.ID, meta.PreviousResponseID, meta.ChainRootID, meta.Position, isCheckpoint,
		meta.OwnerPrincipal, meta.Model, string(meta.Status), meta.CreatedAt,
		meta.ExpiresAt, meta.PayloadKey, meta.CheckpointKey,
	)
	return err
}

func (s *IndexStore) Get(ctx context.Context, id string) (model.ResponseMeta, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, previous_response_id, chain_root_id, position, is_checkpoint,
		        owner_principal, model, status, created_at, expires_at, payload_key, checkpoint_key
		 FROM responses WHERE id = $1`, id)

	var r responseRow
	err := row.Scan(
		&r.ID, &r.PreviousResponseID, &r.ChainRootID, &r.Position, &r.IsCheckpoint,
		&r.OwnerPrincipal, &r.Model, &r.Status, &r.CreatedAt,
		&r.ExpiresAt, &r.PayloadKey, &r.CheckpointKey,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ResponseMeta{}, storage.ErrNotFound
	}
	if err != nil {
		return model.ResponseMeta{}, err
	}
	return rowToMeta(r), nil
}

// Delete removes a response record. Idempotent — no error if the record did not exist.
func (s *IndexStore) Delete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM responses WHERE id = $1`, id)
	return err
}

func (s *IndexStore) List(ctx context.Context, opts storage.ListOptions) ([]model.ResponseMeta, error) {
	query := `SELECT id, previous_response_id, chain_root_id, position, is_checkpoint,
	                 owner_principal, model, status, created_at, expires_at, payload_key, checkpoint_key
	          FROM responses`
	args := []any{}

	if opts.Owner != "" {
		query += ` WHERE owner_principal = $1`
		args = append(args, opts.Owner)
	}
	query += ` ORDER BY created_at ASC, id ASC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []model.ResponseMeta
	pastCursor := opts.Cursor == ""
	for rows.Next() {
		var r responseRow
		if err := rows.Scan(
			&r.ID, &r.PreviousResponseID, &r.ChainRootID, &r.Position, &r.IsCheckpoint,
			&r.OwnerPrincipal, &r.Model, &r.Status, &r.CreatedAt,
			&r.ExpiresAt, &r.PayloadKey, &r.CheckpointKey,
		); err != nil {
			return nil, err
		}
		if !pastCursor {
			if r.ID == opts.Cursor {
				pastCursor = true
			}
			continue
		}
		results = append(results, rowToMeta(r))
		if opts.Limit > 0 && len(results) >= opts.Limit {
			break
		}
	}
	return results, rows.Err()
}

func (s *IndexStore) ListExpired(ctx context.Context, before int64) ([]model.ResponseMeta, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, previous_response_id, chain_root_id, position, is_checkpoint,
		        owner_principal, model, status, created_at, expires_at, payload_key, checkpoint_key
		 FROM responses WHERE expires_at IS NOT NULL AND expires_at < $1`, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMetaRows(rows)
}

// --- write-intent tracking ---

func (s *IndexStore) InsertWriteIntent(ctx context.Context, intent model.WriteIntent) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO write_intents
			(intent_id, response_id, reservation_id, payload_key, phase, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		intent.IntentID, intent.ResponseID, intent.ReservationID,
		intent.PayloadKey, string(intent.Phase), intent.CreatedAt, intent.UpdatedAt,
	)
	if err != nil && isPgUniqueViolation(err) {
		return storage.ErrAlreadyExists
	}
	return err
}

func (s *IndexStore) UpdateWriteIntent(ctx context.Context, intentID string, phase model.WriteIntentPhase) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE write_intents SET phase = $1, updated_at = $2 WHERE intent_id = $3`,
		string(phase), time.Now().Unix(), intentID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *IndexStore) ListStaleWriteIntents(ctx context.Context, olderThan time.Duration) ([]model.WriteIntent, error) {
	threshold := time.Now().Add(-olderThan).Unix()
	rows, err := s.pool.Query(ctx, `
		SELECT intent_id, response_id, reservation_id, payload_key, phase, created_at, updated_at
		FROM write_intents
		WHERE phase NOT IN ('committed','failed') AND updated_at < $1`, threshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanIntentRows(rows)
}

func (s *IndexStore) DeleteWriteIntent(ctx context.Context, intentID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM write_intents WHERE intent_id = $1`, intentID)
	return err
}

// Count returns the total number of response records.
func (s *IndexStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM responses`).Scan(&n)
	return n, err
}

// ListOldest returns up to limit response records ordered by CreatedAt ascending.
func (s *IndexStore) ListOldest(ctx context.Context, limit int) ([]model.ResponseMeta, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, previous_response_id, chain_root_id, position, is_checkpoint,
		        owner_principal, model, status, created_at, expires_at, payload_key, checkpoint_key
		 FROM responses ORDER BY created_at ASC, id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMetaRows(rows)
}

// --- scan helpers ---

// responseRow is the on-disk shape of a responses table row.
type responseRow struct {
	ID                 string
	PreviousResponseID *string
	ChainRootID        string
	Position           int
	IsCheckpoint       int
	OwnerPrincipal     string
	Model              string
	Status             string
	CreatedAt          int64
	ExpiresAt          *int64
	PayloadKey         string
	CheckpointKey      *string
}

func rowToMeta(r responseRow) model.ResponseMeta {
	return model.ResponseMeta{
		ID:                 r.ID,
		PreviousResponseID: r.PreviousResponseID,
		ChainRootID:        r.ChainRootID,
		Position:           r.Position,
		OwnerPrincipal:     r.OwnerPrincipal,
		Model:              r.Model,
		Status:             model.ResponseStatus(r.Status),
		CreatedAt:          r.CreatedAt,
		ExpiresAt:          r.ExpiresAt,
		PayloadKey:         r.PayloadKey,
		CheckpointKey:      r.CheckpointKey,
	}
}

func scanMetaRows(rows pgx.Rows) ([]model.ResponseMeta, error) {
	var results []model.ResponseMeta
	for rows.Next() {
		var r responseRow
		if err := rows.Scan(
			&r.ID, &r.PreviousResponseID, &r.ChainRootID, &r.Position, &r.IsCheckpoint,
			&r.OwnerPrincipal, &r.Model, &r.Status, &r.CreatedAt,
			&r.ExpiresAt, &r.PayloadKey, &r.CheckpointKey,
		); err != nil {
			return nil, err
		}
		results = append(results, rowToMeta(r))
	}
	return results, rows.Err()
}

func scanIntentRows(rows pgx.Rows) ([]model.WriteIntent, error) {
	var results []model.WriteIntent
	for rows.Next() {
		var (
			intentID, responseID, reservationID, payloadKey, phase string
			createdAt, updatedAt                                   int64
		)
		if err := rows.Scan(&intentID, &responseID, &reservationID, &payloadKey, &phase, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		results = append(results, model.WriteIntent{
			IntentID:      intentID,
			ResponseID:    responseID,
			ReservationID: reservationID,
			PayloadKey:    payloadKey,
			Phase:         model.WriteIntentPhase(phase),
			CreatedAt:     createdAt,
			UpdatedAt:     updatedAt,
		})
	}
	return results, rows.Err()
}

func isPgUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}
	return false
}

// migrate runs the DDL statements to ensure all tables and indexes exist.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	return nil
}
