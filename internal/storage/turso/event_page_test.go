package turso

import (
	"context"
	"fmt"
	"github.com/michmich112/congee/internal/nostr"
	"github.com/michmich112/congee/internal/storage"
	"github.com/rs/zerolog"
	"path/filepath"
	"testing"
)

func TestEventPageSameSecond(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "events.db"), nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	author := fmt.Sprintf("%064x", 314)
	f := nostr.Filter{Authors: []string{author}, Kinds: []int{30402}}
	for i := 0; i < 451; i++ {
		ev := &nostr.Event{ID: fmt.Sprintf("%064x", i+31400), PubKey: author, Kind: 30402, CreatedAt: 10, Tags: [][]string{{"d", fmt.Sprint(i)}}, Content: "bike", Sig: fmt.Sprintf("%0128x", 1)}
		if err := st.SaveEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	var cursor *storage.EventCursor
	for {
		p, err := st.QueryEventsPage(ctx, f, cursor, 200)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Events) > 200 {
			t.Fatal("unbounded page")
		}
		for _, e := range p.Events {
			if seen[e.ID] {
				t.Fatal("duplicate", e.ID)
			}
			seen[e.ID] = true
		}
		if p.Next == nil {
			break
		}
		cursor = p.Next
		if err := st.DeleteEvent(ctx, cursor.ID); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 451 {
		t.Fatalf("visited %d", len(seen))
	}
	if _, err := st.QueryEventsPage(ctx, nostr.Filter{Kinds: []int{1}}, cursor, 200); err == nil {
		t.Fatal("changed filter accepted")
	}
	if _, err := st.QueryEventsPage(ctx, f, nil, 501); err == nil {
		t.Fatal("oversized page accepted")
	}
	for id := range seen {
		_ = st.DeleteEvent(ctx, id)
	}
}
