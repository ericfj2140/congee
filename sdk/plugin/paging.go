package plugin

import (
	"context"
	"github.com/michmich112/congee/sdk/plugin/pluginv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PagedHost is an additive extension to Host. Old hosts return Unimplemented.
type PagedHost interface {
	QueryEventsPage(context.Context, Filter, *EventCursor, int) (EventPage, error)
}

type EventCursor struct {
	CreatedAt  int64
	ID         string
	FilterHash string
}

type EventPage struct {
	Events []Event
	Next   *EventCursor
}

func cursorToProto(c *EventCursor) *pluginv1.EventCursor {
	if c == nil {
		return nil
	}
	return &pluginv1.EventCursor{CreatedAt: c.CreatedAt, Id: c.ID, FilterHash: c.FilterHash}
}

func cursorFromProto(c *pluginv1.EventCursor) *EventCursor {
	if c == nil {
		return nil
	}
	return &EventCursor{CreatedAt: c.GetCreatedAt(), ID: c.GetId(), FilterHash: c.GetFilterHash()}
}

func (h *hostClient) QueryEventsPage(ctx context.Context, f Filter, c *EventCursor, size int) (EventPage, error) {
	if h == nil || h.c == nil {
		return EventPage{}, status.Error(codes.Unavailable, "host not connected")
	}
	r, err := h.c.QueryEventsPage(ctx, &pluginv1.QueryEventsPageRequest{Filter: filterToProto(f), Cursor: cursorToProto(c), PageSize: int32(size)})
	if err != nil {
		return EventPage{}, err
	}
	return EventPage{Events: eventsFromProto(r.GetEvents()), Next: cursorFromProto(r.GetNext())}, nil
}

func (s *HostServer) QueryEventsPage(ctx context.Context, r *pluginv1.QueryEventsPageRequest) (*pluginv1.QueryEventsPageResponse, error) {
	h, ok := s.H.(PagedHost)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "host does not support event paging")
	}
	p, err := h.QueryEventsPage(ctx, filterFromProto(r.GetFilter()), cursorFromProto(r.GetCursor()), int(r.GetPageSize()))
	if err != nil {
		return nil, err
	}
	return &pluginv1.QueryEventsPageResponse{Events: eventsToProto(p.Events), Next: cursorToProto(p.Next)}, nil
}
