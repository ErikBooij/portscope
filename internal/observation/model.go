package observation

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

// Interaction is the protocol-neutral envelope consumed by storage, streaming, and the UI.
// Protocol-specific adapters retain their vocabulary in Operation, Attributes, and Payload.
type Interaction struct {
	ID         string            `json:"id"`
	UpstreamID string            `json:"upstreamId"`
	Protocol   string            `json:"protocol"`
	Connection string            `json:"connection,omitempty"`
	Operation  string            `json:"operation"`
	StartedAt  time.Time         `json:"startedAt"`
	DurationUS int64             `json:"durationUs"`
	Outcome    string            `json:"outcome"`
	Error      string            `json:"error,omitempty"`
	Request    Payload           `json:"request"`
	Response   Payload           `json:"response"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type Payload struct {
	Kind      string          `json:"kind"`
	Summary   string          `json:"summary,omitempty"`
	Text      string          `json:"text,omitempty"`
	JSON      json.RawMessage `json:"json,omitempty"`
	Size      int64           `json:"size"`
	Truncated bool            `json:"truncated,omitempty"`
	Headers   []Pair          `json:"headers,omitempty"`
}

type Pair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Query struct {
	UpstreamID string
	Protocol   string
	Outcome    string
	Search     string
	Limit      int
}

type Sink interface {
	Record(Interaction)
}

func NewID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}
