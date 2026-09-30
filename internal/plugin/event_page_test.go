package plugin

import (
	"context"
	"fmt"
	"github.com/michmich112/congee/internal/nostr"
	"github.com/michmich112/congee/internal/storage"
	"github.com/michmich112/congee/sdk/plugin/pluginv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"sort"
	"testing"
)

func TestEventPageGRPCSameSecondAndDeletedCursor(t *testing.T) {
	ctx := context.Background()
	store := &pageWireStore{}
	author := fmt.Sprintf("%064x", 42)
	for i := 0; i < 451; i++ {
		ev := &nostr.Event{ID: fmt.Sprintf("%064x", i+1), PubKey: author, Kind: 30402, CreatedAt: 10, Tags: [][]string{{"d", fmt.Sprint(i)}}, Content: "bike", Sig: fmt.Sprintf("%0128x", 1)}
		store.events = append(store.events, ev)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pluginv1.RegisterHostServer(server, &hostBridge{m: &Manager{store: store}})
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///memory", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pluginv1.NewHostClient(conn)
	seen := map[string]bool{}
	var cursor *pluginv1.EventCursor
	for {
		page, err := client.QueryEventsPage(ctx, &pluginv1.QueryEventsPageRequest{Filter: &pluginv1.Filter{Authors: []string{author}, Kinds: []int32{30402}}, Cursor: cursor, PageSize: 200})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range page.Events {
			if seen[ev.Id] {
				t.Fatal("duplicate", ev.Id)
			}
			seen[ev.Id] = true
		}
		if page.Next == nil {
			break
		}
		cursor = page.Next
		if err := store.DeleteEvent(ctx, cursor.Id); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 451 {
		t.Fatalf("visited %d of 451", len(seen))
	}
	_, err = client.QueryEventsPage(ctx, &pluginv1.QueryEventsPageRequest{Filter: &pluginv1.Filter{Kinds: []int32{1}}, Cursor: cursor, PageSize: 200})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed filter: %v", err)
	}
}

func TestEventPageUnavailable(t *testing.T) {
	_, err := (&hostBridge{m: &Manager{}}).QueryEventsPage(context.Background(), &pluginv1.QueryEventsPageRequest{PageSize: 200})
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	_, err = (&hostBridge{m: &Manager{}}).QueryEvents(context.Background(), &pluginv1.QueryEventsRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}

type pageWireStore struct {
	storage.Store
	events []*nostr.Event
}

func (s *pageWireStore) QueryEventsPage(_ context.Context, f nostr.Filter, c *storage.EventCursor, size int) (storage.EventPage, error) {
	hash, err := storage.ValidateEventPage(f, c, size)
	if err != nil {
		return storage.EventPage{}, err
	}
	sort.Slice(s.events, func(i, j int) bool { return s.events[i].ID < s.events[j].ID })
	p := storage.EventPage{}
	for _, e := range s.events {
		if c == nil || e.CreatedAt < c.CreatedAt || e.CreatedAt == c.CreatedAt && e.ID > c.ID {
			p.Events = append(p.Events, e)
		}
	}
	if len(p.Events) > size {
		p.Events = p.Events[:size]
		last := p.Events[size-1]
		p.Next = &storage.EventCursor{CreatedAt: last.CreatedAt, ID: last.ID, FilterHash: hash}
	}
	return p, nil
}
func (s *pageWireStore) DeleteEvent(_ context.Context, id string) error {
	for i, e := range s.events {
		if e.ID == id {
			s.events = append(s.events[:i], s.events[i+1:]...)
			break
		}
	}
	return nil
}
