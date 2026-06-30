package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

// responseRow is the on-disk shape of a responses table row.
// IsCheckpoint is stored as 0/1 and mapped to CheckpointKey != nil on read.
type responseRow struct {
	ID                 string  `db:"id"`
	PreviousResponseID *string `db:"previous_response_id"`
	ChainRootID        string  `db:"chain_root_id"`
	Position           int     `db:"position"`
	IsCheckpoint       int     `db:"is_checkpoint"` // unused on read; CheckpointKey is the canonical indicator
	OwnerPrincipal     string  `db:"owner_principal"`
	Model              string  `db:"model"`
	Status             string  `db:"status"`
	CreatedAt          int64   `db:"created_at"`
	ExpiresAt          *int64  `db:"expires_at"`
	PayloadKey         string  `db:"payload_key"`
	CheckpointKey      *string `db:"checkpoint_key"`
	Background         int     `db:"background"`
}

var _ storage.IndexStore = (*IndexStore)(nil)

// IndexStore implements storage.IndexStore backed by SQLite.
// Prepared statements are cached at construction time, eliminating
// sqlite3_prepare_v2 overhead on every query.
type IndexStore struct {
	db    *sqlx.DB
	stmts indexStmts
}

type indexStmts struct {
	getResp          *sqlx.Stmt // SELECT * FROM responses WHERE id = ?
	putResp          *sql.Stmt  // INSERT OR REPLACE INTO responses
	deleteResp       *sql.Stmt  // DELETE FROM responses WHERE id = ?
	countResp        *sql.Stmt  // SELECT COUNT(*) FROM responses
	listExpired      *sqlx.Stmt // SELECT * FROM responses WHERE expires_at < ?
	insertIntent     *sql.Stmt  // INSERT INTO write_intents
	updateIntent     *sql.Stmt  // UPDATE write_intents SET phase=?, updated_at=? WHERE intent_id=?
	listStaleIntents *sqlx.Stmt // SELECT * FROM write_intents WHERE phase NOT IN ... AND updated_at < ?
	deleteIntent     *sql.Stmt  // DELETE FROM write_intents WHERE intent_id=?
}

const (
	sqlGetResp = `SELECT * FROM responses WHERE id = ?`

	sqlPutResp = `INSERT OR REPLACE INTO responses
		(id, previous_response_id, chain_root_id, position, is_checkpoint,
		 owner_principal, model, status, created_at, expires_at, payload_key, checkpoint_key, background)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`

	sqlDeleteResp = `DELETE FROM responses WHERE id = ?`

	sqlCountResp = `SELECT COUNT(*) FROM responses`

	sqlListExpired = `SELECT * FROM responses WHERE expires_at IS NOT NULL AND expires_at < ?`

	sqlInsertIntent = `INSERT INTO write_intents
		(intent_id, response_id, reservation_id, payload_key, phase, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?)`

	sqlUpdateIntent = `UPDATE write_intents SET phase = ?, updated_at = ? WHERE intent_id = ?`

	sqlListStaleIntents = `SELECT * FROM write_intents
		WHERE phase NOT IN ('committed','failed') AND updated_at < ?`

	sqlDeleteIntent = `DELETE FROM write_intents WHERE intent_id = ?`
)

// NewIndexStore creates an IndexStore and prepares all SQL statements.
func NewIndexStore(db *sqlx.DB) (*IndexStore, error) {
	s := &IndexStore{db: db}

	var err error
	prepare := func(q string) *sql.Stmt {
		if err != nil {
			return nil
		}
		st, e := db.Prepare(q)
		if e != nil {
			err = fmt.Errorf("prepare %q: %w", q[:minInt(len(q), 40)], e)
		}
		return st
	}
	preparex := func(q string) *sqlx.Stmt {
		if err != nil {
			return nil
		}
		st, e := db.Preparex(q)
		if e != nil {
			err = fmt.Errorf("preparex %q: %w", q[:minInt(len(q), 40)], e)
		}
		return st
	}

	s.stmts.getResp = preparex(sqlGetResp)
	s.stmts.putResp = prepare(sqlPutResp)
	s.stmts.deleteResp = prepare(sqlDeleteResp)
	s.stmts.countResp = prepare(sqlCountResp)
	s.stmts.listExpired = preparex(sqlListExpired)
	s.stmts.insertIntent = prepare(sqlInsertIntent)
	s.stmts.updateIntent = prepare(sqlUpdateIntent)
	s.stmts.listStaleIntents = preparex(sqlListStaleIntents)
	s.stmts.deleteIntent = prepare(sqlDeleteIntent)

	if err != nil {
		return nil, err
	}
	return s, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- response metadata ---

func (s *IndexStore) Put(_ context.Context, meta model.ResponseMeta) error {
	isCheckpoint := 0
	if meta.CheckpointKey != nil {
		isCheckpoint = 1
	}
	background := 0
	if meta.Background {
		background = 1
	}
	_, err := s.stmts.putResp.Exec(
		meta.ID, meta.PreviousResponseID, meta.ChainRootID, meta.Position, isCheckpoint,
		meta.OwnerPrincipal, meta.Model, string(meta.Status), meta.CreatedAt,
		meta.ExpiresAt, meta.PayloadKey, meta.CheckpointKey, background,
	)
	return err
}

func (s *IndexStore) Get(_ context.Context, id string) (model.ResponseMeta, error) {
	var row responseRow
	err := s.stmts.getResp.Get(&row, id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ResponseMeta{}, storage.ErrNotFound
	}
	if err != nil {
		return model.ResponseMeta{}, err
	}
	return rowToMeta(row), nil
}

// Delete removes a response record. Idempotent — no error if the record did not exist.
func (s *IndexStore) Delete(_ context.Context, id string) error {
	_, err := s.stmts.deleteResp.Exec(id)
	return err
}

func (s *IndexStore) Count(_ context.Context) (int64, error) {
	var n int64
	err := s.stmts.countResp.QueryRow().Scan(&n)
	return n, err
}

func (s *IndexStore) List(_ context.Context, opts storage.ListOptions) ([]model.ResponseMeta, error) {
	// List uses dynamic filtering so it cannot use a single prepared statement.
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
	defer func() { _ = rows.Close() }()

	var results []model.ResponseMeta
	pastCursor := opts.Cursor == ""
	for rows.Next() {
		var row responseRow
		if err := rows.StructScan(&row); err != nil {
			return nil, err
		}
		if !pastCursor {
			if row.ID == opts.Cursor {
				pastCursor = true
			}
			continue
		}
		results = append(results, rowToMeta(row))
		if opts.Limit > 0 && len(results) >= opts.Limit {
			break
		}
	}
	return results, rows.Err()
}

func (s *IndexStore) ListExpired(_ context.Context, before int64) ([]model.ResponseMeta, error) {
	rows, err := s.stmts.listExpired.Queryx(before)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanMetaRows(rows)
}

// --- write-intent tracking ---

func (s *IndexStore) InsertWriteIntent(_ context.Context, intent model.WriteIntent) error {
	_, err := s.stmts.insertIntent.Exec(
		intent.IntentID, intent.ResponseID, intent.ReservationID,
		intent.PayloadKey, string(intent.Phase), intent.CreatedAt, intent.UpdatedAt,
	)
	if err != nil && isUniqueConstraint(err) {
		return storage.ErrAlreadyExists
	}
	return err
}

func (s *IndexStore) UpdateWriteIntent(_ context.Context, intentID string, phase model.WriteIntentPhase) error {
	res, err := s.stmts.updateIntent.Exec(string(phase), time.Now().Unix(), intentID)
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
	rows, err := s.stmts.listStaleIntents.Queryx(threshold)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanIntentRows(rows)
}

func (s *IndexStore) DeleteWriteIntent(_ context.Context, intentID string) error {
	_, err := s.stmts.deleteIntent.Exec(intentID)
	return err
}

// --- scan helpers ---

func scanMetaRows(rows *sqlx.Rows) ([]model.ResponseMeta, error) {
	var results []model.ResponseMeta
	for rows.Next() {
		var row responseRow
		if err := rows.StructScan(&row); err != nil {
			return nil, err
		}
		results = append(results, rowToMeta(row))
	}
	return results, rows.Err()
}

func rowToMeta(row responseRow) model.ResponseMeta {
	return model.ResponseMeta{
		ID:                 row.ID,
		PreviousResponseID: row.PreviousResponseID,
		ChainRootID:        row.ChainRootID,
		Position:           row.Position,
		OwnerPrincipal:     row.OwnerPrincipal,
		Model:              row.Model,
		Background:         row.Background != 0,
		Status:             model.ResponseStatus(row.Status),
		CreatedAt:          row.CreatedAt,
		ExpiresAt:          row.ExpiresAt,
		PayloadKey:         row.PayloadKey,
		CheckpointKey:      row.CheckpointKey,
	}
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
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "constraint failed")
}
