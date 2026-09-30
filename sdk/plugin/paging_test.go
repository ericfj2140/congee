package plugin

import (
	"context"
	"github.com/michmich112/congee/sdk/plugin/pluginv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"testing"
)

type oldHost struct{}

func (oldHost) QueryEvents(context.Context, []Filter) ([]Event, error)       { return nil, nil }
func (oldHost) GetEventsByIDs(context.Context, []string) ([]Event, error)    { return nil, nil }
func (oldHost) Log(context.Context, string, string, map[string]string) error { return nil }

type pagedFixture struct {
	oldHost
	t *testing.T
}

func (h pagedFixture) QueryEventsPage(_ context.Context, f Filter, c *EventCursor, size int) (EventPage, error) {
	if size != 200 || len(f.Kinds) != 1 || f.Kinds[0] != 30402 || c.ID != "cursor" || c.FilterHash != "filter" {
		h.t.Fatalf("request lost fields: %+v %+v %d", f, c, size)
	}
	return EventPage{Events: []Event{{ID: "winner", Kind: 30402, Tags: [][]string{{"d", "bike"}}}}, Next: &EventCursor{CreatedAt: 10, ID: "next", FilterHash: "filter"}}, nil
}
func TestPagingWireAndLegacyHost(t *testing.T) {
	for _, h := range []Host{oldHost{}, pagedFixture{t: t}} {
		listener := bufconn.Listen(1 << 20)
		server := grpc.NewServer()
		pluginv1.RegisterHostServer(server, &HostServer{H: h})
		go server.Serve(listener)
		conn, err := grpc.NewClient("passthrough:///memory", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		client := &hostClient{c: pluginv1.NewHostClient(conn)}
		p, err := client.QueryEventsPage(context.Background(), Filter{Kinds: []int{30402}}, &EventCursor{ID: "cursor", FilterHash: "filter"}, 200)
		if _, ok := h.(PagedHost); !ok {
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("old host: %v", err)
			}
		} else if err != nil || p.Next.ID != "next" || len(p.Events) != 1 || p.Events[0].Tags[0][1] != "bike" {
			t.Fatalf("page %+v err=%v", p, err)
		}
		conn.Close()
		server.Stop()
	}
}
