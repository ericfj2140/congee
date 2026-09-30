# Congee plugin SDK

Go module for Congee plugin binaries: gRPC/protobuf ABI (`congee.plugin.v1`), `Handler`, and `Serve`.

This is a **nested module**. It does **not** import the relay, Turso, admin UI, or `internal/`. Direct dependencies are gRPC and protobuf only.

```bash
go get github.com/michmich112/congee/sdk/plugin@v0.1.0
```

```go
import sdk "github.com/michmich112/congee/sdk/plugin"

func main() {
	if err := sdk.Serve(ctx, handler); err != nil {
		os.Exit(1)
	}
}
```

Handshake `api_version` must be **1**. Declare capabilities such as `sdk.CapIntercept`, `sdk.CapEventsRead`, `sdk.CapIndexOwn`, `sdk.CapAdminUI`.

The relay **fail-opens** intercept when the plugin is not ready, times out, or returns an RPC error — those paths never call `InterceptREQ`. The rolling intercept log on the plugin settings page is **host-side** (Congee admin chrome). Do not add intercept-log RPCs to this ABI or assume a plugin iframe can read that log (`connect-src 'none'`). See [docs/plugin-architecture.md](../../docs/plugin-architecture.md).

Git tags for this module are prefixed: `sdk/plugin/vX.Y.Z`. A root Congee release tag (`v1.2.3`) does **not** version this SDK.

For local ABI work against a Congee checkout, use a `go.work` overlay or `replace` pointing at `./sdk/plugin`. Do not commit a `go.work` that assumes a missing sibling repo.

## Bounded event paging

`PagedHost` adds `QueryEventsPage(ctx, filter, cursor, size)` without changing
`Host` or handshake API version 1. It requires a Congee build supporting the
new RPC; older hosts return gRPC `Unimplemented`. Plugins that need complete
source discovery must report that incompatibility rather than use a truncated
`QueryEvents` result as proof of absence.

The RPC accepts one structural filter without `search` or `limit`, with page
sizes 1–500. Events are ordered by `created_at DESC, id ASC`. Pass the returned
`Next` cursor unchanged with the same filter until it is nil. A cursor includes
both ordering keys and a filter fingerprint; it remains usable when its event
has been deleted. Invalid sizes, malformed cursors, or changed filters return
`InvalidArgument`; a disconnected source returns `Unavailable`.

Pages read the current retained store, not a pinned snapshot. Concurrent
insertions before a cursor can be missed in that sweep; repeat sweeps and
stored-event invalidations provide convergence. This is not a history export.
The storage upgrade adds indexes for global and merchant cursor traversal;
creating them can increase the first startup time on large databases.
