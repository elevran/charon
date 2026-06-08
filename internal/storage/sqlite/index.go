package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

var _ storage.IndexStore = (*IndexStore)(nil)

// IndexStore implements storage.IndexStore backed by SQLite.
type IndexStore struct{ db *sqlx.DB }

// NewIndexStore creates an IndexStore using an already-open database.
func NewIndexStore(db *sqlx.DB) *IndexStore { return &IndexStore{db: db} }

// --- response metadata ---

func (s *IndexStore) Put(_ context.Context, meta model.ResponseMeta) error {
	isCheckpoint := 0
	if meta.CheckpointKey != nil {
		isCheckpoint = 1
	}
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO responses
			(id, previous_response_id, chain_root_id, position, is_checkpoint,
			 owner_principal, model, status, created_at, expires_at, payload_key, checkpoint_key)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		meta.ID, meta.PreviousResponseID, meta.ChainRootID, meta.Position, isCheckpoint,
		meta.OwnerPrincipal, meta.Model, string(meta.Status), meta.CreatedAt,
		meta.ExpiresAt, meta.PayloadKey, meta.CheckpointKey,
	)
	return err
}

func (s *IndexStore) Get(_ context.Context, id string) (model.ResponseMeta, error) {
	var row struct {
		ID                 string  `db:"id"`
		PreviousResponseID *string `db:"previous_response_id"`
		ChainRootID        string  `db:"chain_root_id"`
		Position           int     `db:"position"`
		IsCheckpoint       int     `db:"is_checkpoint"`
		OwnerPrincipal     string  `db:"owner_principal"`
		Model              string  `db:"model"`
		Status             string  `db:"status"`
		CreatedAt          int64   `db:"created_at"`
		ExpiresAt          *int64  `db:"expires_at"`
		PayloadKey         string  `db:"payload_key"`
		CheckpointKey      *string `db:"checkpoint_key"`
	}
	err := s.db.Get(&row, `SELECT * FROM responses WHERE id = ?`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ResponseMeta{}, storage.ErrNotFound
	}
	if err != nil {
		return model.ResponseMeta{}, err
	}
	return model.ResponseMeta{
		ID:                 row.ID,
		PreviousResponseID: row.PreviousResponseID,
		ChainRootID:        row.ChainRootID,
		Position:           row.Position,
		OwnerPrincipal:     row.OwnerPrincipal,
		Model:              row.Model,
		Status:             model.ResponseStatus(row.Status),
		CreatedAt:          row.CreatedAt,
		ExpiresAt:          row.ExpiresAt,
		PayloadKey:         row.PayloadKey,
		CheckpointKey:      row.CheckpointKey,
	}, nil
}

// Delete removes a response record. Idempotent — no error if the record did not exist.
func (s *IndexStore) Delete(_ context.Context, id string) error {
	_, err := s.db.Exec(`DELETE FROM responses WHERE id = ?`, id)
	return err
}

func (s *IndexStore) List(_ context.Context, opts storage.ListOptions) ([]model.ResponseMeta, error) {
	query := `SELECT * FROM responses`
	args := []any{}

	if opts.Owner != "" {
		query += ` WHERE owner_principal = ?`
		args = append(args, opts.Owner)
	}
	query += ` ORDER BY created_at ASC, id ASC`

	rows, err := s.db.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []model.ResponseMeta
	pastCursor := opts.Cursor == ""
	for rows.Next() {
		var row struct {
			ID                 string  `db:"id"`
			PreviousResponseID *string `db:"previous_response_id"`
			ChainRootID        string  `db:"chain_root_id"`
			Position           int     `db:"position"`
			IsCheckpoint       int     `db:"is_checkpoint"`
			OwnerPrincipal     string  `db:"owner_principal"`
			Model              string  `db:"model"`
			Status             string  `db:"status"`
			CreatedAt          int64   `db:"created_at"`
			ExpiresAt          *int64  `db:"expires_at"`
			PayloadKey         string  `db:"payload_key"`
			CheckpointKey      *string `db:"checkpoint_key"`
		}
		if err := rows.StructScan(&row); err != nil {
			return nil, err
		}
		if !pastCursor {
			if row.ID == opts.Cursor {
				pastCursor = true
			}
			continue
		}
		results = append(results, model.ResponseMeta{
			ID:                 row.ID,
			PreviousResponseID: row.PreviousResponseID,
			ChainRootID:        row.ChainRootID,
			Position:           row.Position,
				OwnerPrincipal:     row.OwnerPrincipal,
			Model:              row.Model,
			Status:             model.ResponseStatus(row.Status),
			CreatedAt:          row.CreatedAt,
			ExpiresAt:          row.ExpiresAt,
			PayloadKey:         row.PayloadKey,
			CheckpointKey:      row.CheckpointKey,
		})
		if opts.Limit > 0 && len(results) >= opts.Limit {
			break
		}
	}
	return results, rows.Err()
}

func (s *IndexStore) ListExpired(_ context.Context, before int64) ([]model.ResponseMeta, error) {
	rows, err := s.db.Queryx(
		`SELECT * FROM responses WHERE expires_at IS NOT NULL AND expires_at < ?`, before,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMetaRows(rows)
}

// --- write-intent tracking ---

func (s *IndexStore) InsertWriteIntent(_ context.Context, intent model.WriteIntent) error {
	_, err := s.db.Exec(`
		INSERT INTO write_intents (intent_id, response_id, reservation_id, payload_key, phase, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?)`,
		intent.IntentID, intent.ResponseID, intent.ReservationID,
		intent.PayloadKey, string(intent.Phase), intent.CreatedAt, intent.UpdatedAt,
	)
	if err != nil && isUniqueConstraint(err) {
		return storage.ErrAlreadyExists
	}
	return err
}

func (s *IndexStore) UpdateWriteIntent(_ context.Context, intentID string, phase model.WriteIntentPhase) error {
	res, err := s.db.Exec(
		`UPDATE write_intents SET phase = ?, updated_at = ? WHERE intent_id = ?`,
		string(phase), time.Now().Unix(), intentID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *IndexStore) ListStaleWriteIntents(_ context.Context, olderThan time.Duration) ([]model.WriteIntent, error) {
	threshold := time.Now().Add(-olderThan).Unix()
	rows, err := s.db.Queryx(`
		SELECT * FROM write_intents
		WHERE phase NOT IN ('committed','failed') AND updated_at < ?`, threshold,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanIntentRows(rows)
}

func (s *IndexStore) DeleteWriteIntent(_ context.Context, intentID string) error {
	_, err := s.db.Exec(`DELETE FROM write_intents WHERE intent_id = ?`, intentID)
	return err
}

// --- scan helpers ---

func scanMetaRows(rows *sqlx.Rows) ([]model.ResponseMeta, error) {
	var results []model.ResponseMeta
	for rows.Next() {
		var row struct {
			ID                 string  `db:"id"`
			PreviousResponseID *string `db:"previous_response_id"`
			ChainRootID        string  `db:"chain_root_id"`
			Position           int     `db:"position"`
			IsCheckpoint       int     `db:"is_checkpoint"`
			OwnerPrincipal     string  `db:"owner_principal"`
			Model              string  `db:"model"`
			Status             string  `db:"status"`
			CreatedAt          int64   `db:"created_at"`
			ExpiresAt          *int64  `db:"expires_at"`
			PayloadKey         string  `db:"payload_key"`
			CheckpointKey      *string `db:"checkpoint_key"`
		}
		if err := rows.StructScan(&row); err != nil {
			return nil, err
		}
		results = append(results, model.ResponseMeta{
			ID:                 row.ID,
			PreviousResponseID: row.PreviousResponseID,
			ChainRootID:        row.ChainRootID,
			Position:           row.Position,
				OwnerPrincipal:     row.OwnerPrincipal,
			Model:              row.Model,
			Status:             model.ResponseStatus(row.Status),
			CreatedAt:          row.CreatedAt,
			ExpiresAt:          row.ExpiresAt,
			PayloadKey:         row.PayloadKey,
			CheckpointKey:      row.CheckpointKey,
		})
	}
	return results, rows.Err()
}

func scanIntentRows(rows *sqlx.Rows) ([]model.WriteIntent, error) {
	var results []model.WriteIntent
	for rows.Next() {
		var row struct {
			IntentID      string `db:"intent_id"`
			ResponseID    string `db:"response_id"`
			ReservationID string `db:"reservation_id"`
			PayloadKey    string `db:"payload_key"`
			Phase         string `db:"phase"`
			CreatedAt     int64  `db:"created_at"`
			UpdatedAt     int64  `db:"updated_at"`
		}
		if err := rows.StructScan(&row); err != nil {
			return nil, err
		}
		results = append(results, model.WriteIntent{
			IntentID:      row.IntentID,
			ResponseID:    row.ResponseID,
			ReservationID: row.ReservationID,
			PayloadKey:    row.PayloadKey,
			Phase:         model.WriteIntentPhase(row.Phase),
			CreatedAt:     row.CreatedAt,
			UpdatedAt:     row.UpdatedAt,
		})
	}
	return results, rows.Err()
}

func isUniqueConstraint(err error) bool {
	// modernc.org/sqlite wraps constraint errors; check the message.
	return err != nil && (errors.Is(err, sql.ErrNoRows) == false) &&
		containsAny(err.Error(), "UNIQUE constraint failed", "constraint failed")
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
