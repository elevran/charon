package model

// WriteIntentPhase tracks progress through the two-phase store commit.
type WriteIntentPhase string

const (
	WriteIntentPending     WriteIntentPhase = "pending"
	WriteIntentFileWritten WriteIntentPhase = "file_written" // object written, index not yet committed
	WriteIntentCommitted   WriteIntentPhase = "committed"
	WriteIntentFailed      WriteIntentPhase = "failed"
)

type WriteIntent struct {
	IntentID   string
	ResponseID string
	PayloadKey string
	Phase      WriteIntentPhase
	CreatedAt  int64
	UpdatedAt  int64
}
