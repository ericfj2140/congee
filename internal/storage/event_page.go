package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/michmich112/congee/internal/nostr"
)

const MaxEventPageSize = 500

// EventCursor continues a structural query in created_at DESC, id ASC order.
// It does not pin a snapshot; a later sweep repairs concurrent insertions.
type EventCursor struct {
	CreatedAt  int64
	ID         string
	FilterHash string
}

type EventPage struct {
	Events []*nostr.Event
	Next   *EventCursor
}

// PagedEventStore is optional so existing Store implementations remain compatible.
type PagedEventStore interface {
	QueryEventsPage(context.Context, nostr.Filter, *EventCursor, int) (EventPage, error)
}

func ValidateEventPage(f nostr.Filter, cursor *EventCursor, size int) (string, error) {
	if size < 1 || size > MaxEventPageSize {
		return "", fmt.Errorf("page size must be between 1 and %d", MaxEventPageSize)
	}
	if f.HasSearch() || f.Limit != nil {
		return "", fmt.Errorf("paged queries require a structural filter without search or limit")
	}
	for key := range f.Tag {
		if len(key) != 2 || key[0] != '#' {
			return "", fmt.Errorf("paged tag filters require a single-letter tag name")
		}
	}
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	fingerprint := hex.EncodeToString(h[:])
	if cursor != nil {
		id, err := hex.DecodeString(cursor.ID)
		if err != nil || len(id) != 32 || cursor.FilterHash != fingerprint {
			return "", fmt.Errorf("invalid cursor or changed filter")
		}
	}
	return fingerprint, nil
}
