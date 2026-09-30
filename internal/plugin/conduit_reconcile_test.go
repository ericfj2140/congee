package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/michmich112/congee/internal/config"
	"github.com/michmich112/congee/internal/db"
	"github.com/michmich112/congee/internal/nostr"
	"github.com/michmich112/congee/sdk/plugin/pluginv1"
	"github.com/rs/zerolog"
)

// The plugin's CI supplies its separately built binary. The relay does not
// acquire a dependency on the plugin module or its implementation.
func TestConduitCanonicalReconciliation(t *testing.T) {
	binary := os.Getenv("CONDUIT_PLUGIN_TEST_BINARY")
	if binary == "" {
		t.Skip("set CONDUIT_PLUGIN_TEST_BINARY to the plugin binary")
	}
	t.Setenv("CONDUIT_EMBEDDER", "fake")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := t.TempDir()
	store, closeStore, err := db.OpenTestStore(ctx, filepath.Join(root, "events.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore()
	author := fmt.Sprintf("%064x", 314)
	event := func(id, n int, at int64, body string) *nostr.Event {
		return &nostr.Event{ID: fmt.Sprintf("%064x", id), PubKey: author, Kind: 30402, CreatedAt: at, Tags: [][]string{{"d", fmt.Sprint(n)}}, Content: body, Sig: fmt.Sprintf("%0128x", 1)}
	}
	// Entire source discovery must cross the same-second 200-event boundary.
	for n := 0; n < 205; n++ {
		if err := store.SaveEvent(ctx, event(n+100, n, 10, "catalogue")); err != nil {
			t.Fatal(err)
		}
	}
	winner := event(1, 300, 1, "needle bicycle")
	if err := store.SaveEvent(ctx, event(3, 300, 1, "old bicycle")); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveEvent(ctx, winner); err != nil {
		t.Fatal(err)
	}
	_ = store.SaveEvent(ctx, event(4, 300, 1, "losing tie"))
	plugins := filepath.Join(root, "plugins")
	pkg := filepath.Join(plugins, "conduit")
	if err := os.MkdirAll(pkg, 0755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "conduit-plugin"), data, 0755); err != nil {
		t.Fatal(err)
	}
	man, _ := json.Marshal(Manifest{ID: "conduit", Name: "Conduit", APIVersion: 1, Exec: map[string]string{runtime.GOOS + "_" + runtime.GOARCH: "conduit-plugin"}})
	if err := os.WriteFile(filepath.Join(pkg, "plugin.json"), man, 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Plugins: config.PluginsSection{Directory: plugins, Items: []config.PluginItem{{ID: "conduit", Enabled: true}}}}
	m := NewManager(cfg, filepath.Join(root, "config.json"), store, zerolog.Nop())
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	m.mu.Lock()
	in := m.inst["conduit"]
	m.mu.Unlock()
	if in == nil {
		t.Fatal("plugin did not start")
	}
	search := "needle"
	req := &nostr.ReqMessage{SubID: "test", Filters: []nostr.Filter{{Kinds: []int{30402}, Search: &search}}}
	wait := func(want int, ids []string) {
		t.Helper()
		for ctx.Err() == nil {
			in.mu.Lock()
			client := in.client
			in.mu.Unlock()
			if client != nil {
				status, err := client.Status(ctx, &pluginv1.StatusRequest{})
				var stats struct {
					Active         int `json:"active"`
					Reconciliation struct {
						Pending int `json:"pending"`
					} `json:"reconciliation"`
				}
				if err == nil {
					json.Unmarshal([]byte(status.Json), &stats)
				}
				got := m.InterceptREQ(ctx, req)
				if err == nil && status.Ready && stats.Active == want && stats.Reconciliation.Pending == 0 && got.Action == InterceptRespond && fmt.Sprint(got.EventIDs) == fmt.Sprint(ids) {
					return
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
		t.Fatal("plugin did not converge through real host/storage/gRPC")
	}
	// No stored-event notifications are sent: these are missed-callback cases.
	wait(206, []string{winner.ID})
	if err := store.DeleteEvent(ctx, winner.ID); err != nil {
		t.Fatal(err)
	}
	in.mu.Lock()
	client := in.client
	in.mu.Unlock()
	if _, err := client.AdminAction(ctx, &pluginv1.AdminActionRequest{Name: "rebuild"}); err != nil {
		t.Fatal(err)
	}
	wait(205, nil)
}
