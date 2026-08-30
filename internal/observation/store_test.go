package observation

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStorePersistsFiltersAndRetainsNewest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store, err := OpenStore(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"GET /old", "SET key", "GET /new"} {
		protocol := "http"
		if operation == "SET key" {
			protocol = "redis"
		}
		item := Interaction{ID: operation, UpstreamID: "one", Protocol: protocol, Operation: operation, StartedAt: time.Now(), Outcome: "ok"}
		if operation == "GET /new" {
			item.Request.JSON = json.RawMessage(`{"sku":"signal-lamp"}`)
		}
		store.Record(item)
	}
	if got := store.List(Query{Limit: 10}); len(got) != 2 || got[0].Operation != "GET /new" {
		t.Fatalf("items = %#v", got)
	}
	if got := store.List(Query{Protocol: "redis"}); len(got) != 1 || got[0].Operation != "SET key" {
		t.Fatalf("filtered = %#v", got)
	}
	reopened, err := OpenStore(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.List(Query{Search: "new"}); len(got) != 1 {
		t.Fatalf("reopened = %#v", got)
	}
	if got := reopened.List(Query{Search: "signal-lamp"}); len(got) != 1 {
		t.Fatalf("JSON search = %#v", got)
	}
}

func TestJournalIsCompactedNearRetentionLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store, err := OpenStore(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 100 {
		store.Record(Interaction{ID: string(rune(index + 1)), Protocol: "http", Operation: "GET /", StartedAt: time.Now(), Outcome: "ok"})
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	lines := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if lines > 11 {
		t.Fatalf("journal retained %d lines for a 10-event store", lines)
	}
}
