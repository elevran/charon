package model

// WriteIntentPhase tracks progress through the two-phase store commit.
type WriteIntentPhase string

const (
	WriteIntentPending     WriteIntentPhase = "pending"
	WriteIntentFileWritten WriteIntentPhase = "file_written" // object written, index not yet committed
	WriteIntentCommitted   WriteIntentPhase = "committed"
	WriteIntentFailed      WriteIntentPhase = "failed"
	// WriteIntentStreamOpen is set when the first chunk of a streaming store arrives.
	// The stream is in progress; subsequent chunks append to the in-memory stage.
	// Advances to file_written then committed when CommitStream is called.
	WriteIntentStreamOpen WriteIntentPhase = "stream_open"
)

type WriteIntent struct {
	IntentID      string
	ResponseID    string // canonical ID from the inference server
	ReservationID string // rsrv_... from the preceding resolve call; empty for new chains
	PayloadKey    string
	Phase         WriteIntentPhase
	CreatedAt     int64
	UpdatedAt     int64
}
