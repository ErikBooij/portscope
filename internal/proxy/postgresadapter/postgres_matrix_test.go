//go:build postgresmatrix

package postgresadapter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type matrixSink struct{ events chan observation.Interaction }

func (sink matrixSink) Record(item observation.Interaction) { sink.events <- item }

func TestRealPostgresCompatibility(t *testing.T) {
	address := os.Getenv("POSTGRES_MATRIX_ADDR")
	if address == "" {
		t.Skip("POSTGRES_MATRIX_ADDR is not set")
	}
	password := os.Getenv("POSTGRES_MATRIX_PASSWORD")
	if password == "" {
		password = "portscope-matrix-secret"
	}
	database := os.Getenv("POSTGRES_MATRIX_DATABASE")
	if database == "" {
		database = "portscope_compat"
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 256)
	configured := config.Upstream{
		ID: "postgres-matrix", Name: "PostgreSQL compatibility matrix", Protocol: "postgres", Enabled: true,
		ListenAddr: "127.0.0.1:0", Target: address,
		Postgres: &config.PostgresOptions{ListenerUsername: "matrix_app", ListenerPassword: "listener-secret", UpstreamUsername: "postgres", UpstreamPassword: password, Database: database},
	}
	adapter := New()
	adapterDone := make(chan error, 1)
	go func() {
		adapterDone <- adapter.Run(ctx, configured, matrixSink{events: events}, func(value string) { ready <- value })
	}()
	var listener string
	select {
	case listener = <-ready:
	case err := <-adapterDone:
		t.Fatalf("start adapter: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("adapter did not become ready")
	}
	clientConfig, err := pgx.ParseConfig(fmt.Sprintf("postgres://matrix_app:listener-secret@%s/client_database?sslmode=disable&application_name=portscope-matrix", listener))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.ConnectConfig(ctx, clientConfig)
	if err != nil {
		t.Fatalf("connect through proxy: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })

	var version, applicationName string
	if err := connection.QueryRow(ctx, "SELECT current_setting('server_version'), current_setting('application_name')", pgx.QueryExecModeSimpleProtocol).Scan(&version, &applicationName); err != nil {
		t.Fatalf("simple query: %v", err)
	}
	if expected := os.Getenv("POSTGRES_MATRIX_VERSION"); expected != "" && !strings.HasPrefix(version, expected) {
		t.Fatalf("server version = %q, want prefix %q", version, expected)
	}
	if applicationName != "portscope-matrix" {
		t.Fatalf("startup application_name = %q", applicationName)
	}

	const table = "portscope_matrix_probe"
	_, _ = connection.Exec(ctx, "DROP TABLE IF EXISTS "+table)
	t.Cleanup(func() { _, _ = connection.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })
	if _, err := connection.Exec(ctx, "CREATE TABLE "+table+" (id bigint primary key, label text, payload bytea, score double precision, created timestamptz)"); err != nil {
		t.Fatal(err)
	}
	payload := []byte(strings.Repeat("postgres-payload-", 8192))
	created := time.Date(2026, 8, 30, 14, 15, 16, 123456000, time.UTC)
	if _, err := connection.Exec(ctx, "INSERT INTO "+table+" VALUES ($1, $2, $3, $4, $5)", int64(42), "fourteen-through-current", payload, 10.5, created); err != nil {
		t.Fatalf("extended insert: %v", err)
	}
	var id int64
	var label string
	var roundTrip []byte
	if err := connection.QueryRow(ctx, "SELECT id, label, payload FROM "+table+" WHERE id=$1", int64(42)).Scan(&id, &label, &roundTrip); err != nil {
		t.Fatalf("extended select: %v", err)
	}
	if id != 42 || label != "fourteen-through-current" || !bytesEqual(roundTrip, payload) {
		t.Fatal("extended values did not survive the proxy")
	}

	batch := &pgx.Batch{}
	batch.Queue("SELECT $1::int", 1)
	batch.Queue("SELECT $1::text", "two")
	results := connection.SendBatch(ctx, batch)
	var one int
	var two string
	if err := results.QueryRow().Scan(&one); err != nil {
		t.Fatal(err)
	}
	if err := results.QueryRow().Scan(&two); err != nil {
		t.Fatal(err)
	}
	if err := results.Close(); err != nil || one != 1 || two != "two" {
		t.Fatalf("pipeline results = %d, %q, %v", one, two, err)
	}

	queryDone := make(chan error, 1)
	go func() { _, err := connection.Exec(context.Background(), "SELECT pg_sleep(10)"); queryDone <- err }()
	time.Sleep(250 * time.Millisecond)
	if err := connection.PgConn().CancelRequest(ctx); err != nil {
		t.Fatalf("cancel request: %v", err)
	}
	select {
	case err := <-queryDone:
		if err == nil || !strings.Contains(err.Error(), "canceling statement") {
			t.Fatalf("cancelled query error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel request did not reach the upstream")
	}
	if err := connection.Ping(ctx); err != nil {
		t.Fatalf("connection unusable after cancellation: %v", err)
	}

	found := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for !found["CONNECT"] || !found["CANCEL"] || !found["QUERY SELECT"] {
		select {
		case event := <-events:
			found[event.Operation] = true
		case <-deadline:
			t.Fatalf("missing observations: %#v", found)
		}
	}
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
