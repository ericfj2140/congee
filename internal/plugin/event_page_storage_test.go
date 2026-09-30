package plugin

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/michmich112/congee/internal/db"
	"github.com/michmich112/congee/internal/nostr"
	"github.com/michmich112/congee/sdk/plugin/pluginv1"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Unlike the wire fixture, this verifies the host's NIP-01 storage winner.
func TestEventPageCanonicalStorageGRPC(t *testing.T) {
	ctx := context.Background()
	store, closeStore, err := db.OpenTestStore(ctx, filepath.Join(t.TempDir(), "events.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore()
	author := fmt.Sprintf("%064x", 42)
	event := func(id int, at int64) *nostr.Event {
		return &nostr.Event{ID: fmt.Sprintf("%064x", id), PubKey: author, Kind: 30402, CreatedAt: at, Tags: [][]string{{"d", "bike"}}, Content: "bicycle", Sig: fmt.Sprintf("%0128x", 1)}
	}
	for _, ev := range []*nostr.Event{event(3, 10), event(2, 10)} {
		if err := store.SaveEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	// A later import with a larger ID at the same timestamp cannot displace 2.
	_ = store.SaveEvent(ctx, event(4, 10))
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
	req := &pluginv1.QueryEventsPageRequest{Filter: &pluginv1.Filter{Kinds: []int32{30402}, Authors: []string{author}, TagFilters: []*pluginv1.TagFilter{{Name: "d", Values: []string{"bike"}}}}, PageSize: 1}
	p, err := client.QueryEventsPage(ctx, req)
	if err != nil || len(p.GetEvents()) != 1 || p.Events[0].Id != event(2, 10).ID || p.Next != nil {
		t.Fatalf("canonical winner %+v err=%v", p, err)
	}
	if err := store.DeleteEvent(ctx, event(2, 10).ID); err != nil {
		t.Fatal(err)
	}
	p, err = client.QueryEventsPage(ctx, req)
	if err != nil || len(p.GetEvents()) != 0 {
		t.Fatalf("deleted canonical event remains: %+v %v", p, err)
	}
}
