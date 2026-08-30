//go:build mysqlmatrix

package mysqladapter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

func TestRealMySQLCompatibility(t *testing.T) {
	address := os.Getenv("MYSQL_MATRIX_ADDR")
	if address == "" {
		t.Skip("MYSQL_MATRIX_ADDR is not set")
	}
	upstreamPassword := os.Getenv("MYSQL_MATRIX_PASSWORD")
	if upstreamPassword == "" {
		upstreamPassword = "portscope-matrix-secret"
	}
	database := os.Getenv("MYSQL_MATRIX_DATABASE")
	if database == "" {
		database = "portscope_compat"
	}
	expectedVersion := os.Getenv("MYSQL_MATRIX_VERSION")
	upstreamTLS := os.Getenv("MYSQL_MATRIX_TLS") == "true"

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 128)
	upstream := config.Upstream{
		ID: "mysql-matrix", Name: "MySQL compatibility matrix", Protocol: "mysql",
		ListenAddr: "127.0.0.1:0", Target: address, Enabled: true,
		MySQL: &config.MySQLOptions{
			ListenerUsername: "matrix-app", ListenerPassword: "listener-secret",
			UpstreamUsername: "root", UpstreamPassword: upstreamPassword, Database: database,
			UpstreamTLS: config.ClientTLSOptions{Enabled: upstreamTLS, InsecureSkipVerify: upstreamTLS},
		},
	}
	adapterDone := make(chan error, 1)
	go func() {
		adapterDone <- New().Run(ctx, upstream, testSink{events: events}, func(value string) { ready <- value })
	}()

	var listener string
	select {
	case listener = <-ready:
	case err := <-adapterDone:
		t.Fatalf("start adapter: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("adapter did not become ready")
	}
	dsn := fmt.Sprintf("matrix-app:listener-secret@tcp(%s)/%s?timeout=10s&readTimeout=10s&writeTimeout=10s&multiStatements=true&interpolateParams=false", listener, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		select {
		case event := <-events:
			t.Fatalf("ping through proxy: %v (adapter: %s)", err, event.Error)
		default:
			t.Fatalf("ping through proxy: %v", err)
		}
	}

	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("query version: %v", err)
	}
	if expectedVersion != "" && !strings.HasPrefix(version, expectedVersion) {
		t.Fatalf("server version = %q, want prefix %q", version, expectedVersion)
	}

	const table = "portscope_matrix_probe"
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table) })
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, label VARCHAR(255), payload LONGBLOB, score DOUBLE, created DATETIME(6)) ENGINE=InnoDB"); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	payload := []byte(strings.Repeat("matrix-payload-", 8192))
	created := time.Date(2026, 8, 30, 14, 15, 16, 123456000, time.UTC)
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (id, label, payload, score, created) VALUES (?, ?, ?, ?, ?)", int64(42), "five-six-through-current", payload, 10.5, created); err != nil {
		t.Fatalf("prepared insert: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+table+" SET label = ? WHERE id = ?", "rolled-back", int64(42)); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var id int64
	var label string
	var roundTrip []byte
	var score float64
	if err := db.QueryRowContext(ctx, "SELECT id, label, payload, score FROM "+table+" WHERE id = ?", int64(42)).Scan(&id, &label, &roundTrip, &score); err != nil {
		t.Fatalf("prepared binary result: %v", err)
	}
	if id != 42 || label != "five-six-through-current" || score != 10.5 || string(roundTrip) != string(payload) {
		t.Fatal("prepared statement values did not survive the proxy")
	}

	rows, err := db.QueryContext(ctx, "SELECT 1 AS first_probe; SELECT 2 AS second_probe")
	if err != nil {
		t.Fatalf("multi-result query: %v", err)
	}
	defer rows.Close()
	var first, second int
	if !rows.Next() || rows.Scan(&first) != nil || !rows.NextResultSet() || !rows.Next() || rows.Scan(&second) != nil || first != 1 || second != 2 {
		t.Fatalf("multi-result values = %d, %d", first, second)
	}

	deadline := time.After(2 * time.Second)
	foundConnect, foundPrepared := false, false
	for !foundConnect || !foundPrepared {
		select {
		case event := <-events:
			foundConnect = foundConnect || event.Operation == "CONNECT"
			foundPrepared = foundPrepared || strings.HasPrefix(event.Operation, "EXECUTE")
			if event.Operation == "CONNECT" && event.Attributes["upstreamTLS"] != fmt.Sprint(upstreamTLS) {
				t.Fatalf("upstream TLS observation = %q, want %t", event.Attributes["upstreamTLS"], upstreamTLS)
			}
		case <-deadline:
			t.Fatalf("missing proxy observations: connect=%t prepared=%t", foundConnect, foundPrepared)
		}
	}
}
