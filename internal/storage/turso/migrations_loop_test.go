package turso

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/michmich112/congee/internal/nostr"
	"github.com/michmich112/congee/internal/storage/sqlitewriter"
	"github.com/rs/zerolog"
)

func execOnLibsqlFile(t *testing.T, ctx context.Context, path string, stmts []string) {
	t.Helper()
	sqldb, _, err := sqlitewriter.OpenLibsqlHandles(ctx, path, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sqldb.Close() }()
	for _, q := range stmts {
		if err := sqlitewriter.ExecSQL(ctx, sqldb, q); err != nil {
			t.Fatal(err)
		}
	}
}

func TestV7FTSRowidMapUpgradePreservesSearchAndFastDeletion(t *testing.T) {
	skipNoDriver(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v7.db")
	st, err := Open(ctx, path, nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	pk := strings.Repeat("b", 64)
	old := &nostr.Event{ID: strings.Repeat("a", 64), PubKey: pk, CreatedAt: 1, Kind: 3, Content: "oldkeyword", Sig: strings.Repeat("c", 128)}
	if err := st.SaveEvent(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Model the on-disk v7 schema without changing or rebuilding its FTS rows.
	execOnLibsqlFile(t, ctx, path, []string{
		`DROP TRIGGER events_ai_fts`, `DROP TRIGGER events_au_fts`, `DROP TRIGGER events_ad_fts`,
		`DROP TABLE event_fts_rowids`,
		`CREATE TRIGGER events_ai_fts AFTER INSERT ON events BEGIN
			INSERT INTO event_fts(event_id, content) VALUES(new.id, new.content); END`,
		`CREATE TRIGGER events_ad_fts AFTER DELETE ON events BEGIN
			DELETE FROM event_fts WHERE event_id = old.id; END`,
		`PRAGMA user_version = 7`,
	})
	st, err = Open(ctx, path, nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var mapped, actual int64
	if err := st.DB().QueryRowContext(ctx, `SELECT m.fts_rowid, f.rowid FROM event_fts_rowids m
		JOIN event_fts f ON f.rowid = m.fts_rowid WHERE m.event_id = ?`, old.ID).Scan(&mapped, &actual); err != nil || mapped != actual {
		t.Fatalf("v7 FTS row lost mapping: mapped=%d actual=%d err=%v", mapped, actual, err)
	}
	newer := &nostr.Event{ID: strings.Repeat("d", 64), PubKey: pk, CreatedAt: 2, Kind: 3, Content: "newkeyword", Sig: strings.Repeat("c", 128)}
	if err := st.SaveEvent(ctx, newer); err != nil {
		t.Fatal(err)
	}
	var oldRows, newRows int
	if err := st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM event_fts WHERE event_id = ?`, old.ID).Scan(&oldRows); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM event_fts WHERE event_id = ?`, newer.ID).Scan(&newRows); err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 || newRows != 1 {
		t.Fatalf("FTS replacement rows old=%d new=%d", oldRows, newRows)
	}
	oldSearch, err := st.SearchEvents(ctx, "oldkeyword", nostr.Filter{Kinds: []int{3}})
	if err != nil || len(oldSearch) != 0 {
		t.Fatalf("old revision remains searchable: count=%d err=%v", len(oldSearch), err)
	}
	newSearch, err := st.SearchEvents(ctx, "newkeyword", nostr.Filter{Kinds: []int{3}})
	if err != nil || len(newSearch) != 1 || newSearch[0].ID != newer.ID {
		t.Fatalf("new revision missing from search: count=%d err=%v", len(newSearch), err)
	}
	if err := st.DB().QueryRowContext(ctx, `SELECT m.fts_rowid, f.rowid FROM event_fts_rowids m
		JOIN event_fts f ON f.rowid = m.fts_rowid WHERE m.event_id = ?`, newer.ID).Scan(&mapped, &actual); err != nil || mapped != actual {
		t.Fatalf("new FTS row lacks mapping: mapped=%d actual=%d err=%v", mapped, actual, err)
	}
	if err := st.DeleteEvent(ctx, newer.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM event_fts`).Scan(&newRows); err != nil || newRows != 0 {
		t.Fatalf("FTS deletion failed: rows=%d err=%v", newRows, err)
	}
}

// TestRunMigrationsLoopsV6ToV7 builds a current file, re-adds ws_connection_sessions with user_version 6, and checks Open drops meta tables.
func TestRunMigrationsLoopsV6ToV7(t *testing.T) {
	skipNoDriver(t)
	ctx := context.Background()
	log := zerolog.Nop()
	dir := t.TempDir()
	path := filepath.Join(dir, "loop.db")

	s, err := Open(ctx, path, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	execOnLibsqlFile(t, ctx, path, []string{
		`CREATE TABLE IF NOT EXISTS ws_connection_sessions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			conn_id TEXT NOT NULL,
			peer_ip TEXT NOT NULL,
			remote_addr TEXT NOT NULL,
			started_unix INTEGER NOT NULL,
			ended_unix INTEGER NOT NULL,
			total_req INTEGER NOT NULL DEFAULT 0,
			total_client_event INTEGER NOT NULL DEFAULT 0,
			series_json TEXT NOT NULL DEFAULT '[]',
			subs_json TEXT NOT NULL DEFAULT '[]'
		)`,
		`CREATE TABLE IF NOT EXISTS audit_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at INTEGER NOT NULL,
			action TEXT NOT NULL,
			detail TEXT,
			pubkey TEXT NOT NULL DEFAULT ''
		)`,
		`PRAGMA user_version = 6`,
	})

	s2, err := Open(ctx, path, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	db := s2.DB()
	var uv int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&uv); err != nil {
		t.Fatal(err)
	}
	if uv != CurrentSchemaVersion() {
		t.Fatalf("user_version: got %d want %d", uv, CurrentSchemaVersion())
	}
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='ws_connection_sessions'`,
	).Scan(&n); err != nil || n != 0 {
		t.Fatalf("ws_connection_sessions should be dropped: err=%v n=%d", err, n)
	}
}

// TestRunMigrationsLoopsFakeV5ToV7 keeps a current events schema but sets user_version to 5 with legacy meta tables present.
func TestRunMigrationsLoopsFakeV5ToV7(t *testing.T) {
	skipNoDriver(t)
	ctx := context.Background()
	log := zerolog.Nop()
	dir := t.TempDir()
	path := filepath.Join(dir, "multistep.db")

	s, err := Open(ctx, path, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	execOnLibsqlFile(t, ctx, path, []string{
		`CREATE TABLE IF NOT EXISTS audit_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at INTEGER NOT NULL,
			action TEXT NOT NULL,
			detail TEXT,
			pubkey TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS config_changelog (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at INTEGER NOT NULL,
			summary TEXT NOT NULL,
			json_diff TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS relay_metric_buckets (
			bucket_start_unix INTEGER NOT NULL PRIMARY KEY,
			events_stored INTEGER NOT NULL DEFAULT 0,
			events_rejected INTEGER NOT NULL DEFAULT 0,
			req_count INTEGER NOT NULL DEFAULT 0,
			close_count INTEGER NOT NULL DEFAULT 0,
			query_ms_sum INTEGER NOT NULL DEFAULT 0,
			query_ms_count INTEGER NOT NULL DEFAULT 0,
			subscriptions_open INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS ws_connection_sessions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			conn_id TEXT NOT NULL,
			peer_ip TEXT NOT NULL,
			remote_addr TEXT NOT NULL,
			started_unix INTEGER NOT NULL,
			ended_unix INTEGER NOT NULL,
			total_req INTEGER NOT NULL DEFAULT 0,
			total_client_event INTEGER NOT NULL DEFAULT 0,
			series_json TEXT NOT NULL DEFAULT '[]',
			subs_json TEXT NOT NULL DEFAULT '[]'
		)`,
		`PRAGMA user_version = 5`,
	})

	s2, err := Open(ctx, path, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	var uv int
	if err := s2.DB().QueryRowContext(ctx, "PRAGMA user_version").Scan(&uv); err != nil {
		t.Fatal(err)
	}
	if uv != CurrentSchemaVersion() {
		t.Fatalf("user_version after multi-step migrate: got %d want %d", uv, CurrentSchemaVersion())
	}
	var metaTables int
	if err := s2.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('audit_log','config_changelog','relay_metric_buckets','ws_connection_sessions')`,
	).Scan(&metaTables); err != nil || metaTables != 0 {
		t.Fatalf("meta tables should be dropped: err=%v n=%d", err, metaTables)
	}
}

// TestPreflightMigrationTargetCurrent checks preflight on a fresh Open database.
func TestPreflightMigrationTargetCurrent(t *testing.T) {
	skipNoDriver(t)
	ctx := context.Background()
	log := zerolog.Nop()
	dir := t.TempDir()
	path := filepath.Join(dir, "pf.db")

	s, err := Open(ctx, path, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	out := PreflightMigrationTarget(ctx, path, log)
	if out.Status != "current" {
		t.Fatalf("status: got %q detail=%q", out.Status, out.Detail)
	}
	if out.ExpectedVersion != CurrentSchemaVersion() {
		t.Fatalf("expected_version: got %d", out.ExpectedVersion)
	}
	if out.ReportedVersion == nil || *out.ReportedVersion != CurrentSchemaVersion() {
		t.Fatalf("reported_version: %+v", out.ReportedVersion)
	}
}

func TestPreflightMigrationTargetEmptyPath(t *testing.T) {
	ctx := context.Background()
	out := PreflightMigrationTarget(ctx, "", zerolog.Nop())
	if out.Status != "unreadable" {
		t.Fatalf("got %q", out.Status)
	}
}
