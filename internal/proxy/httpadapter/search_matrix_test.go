//go:build searchmatrix

package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

func TestRealSearchCompatibility(t *testing.T) {
	target := os.Getenv("SEARCH_MATRIX_URL")
	if target == "" {
		t.Skip("SEARCH_MATRIX_URL is not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 32)
	upstream := config.Upstream{ID: "search-matrix", Protocol: "elasticsearch", ListenAddr: "127.0.0.1:0", Target: target, HTTP: &config.HTTPOptions{}}
	go func() {
		_ = New().Run(ctx, upstream, testSink{events: events}, func(address string) { ready <- address })
	}()
	base := "http://" + <-ready

	root := matrixRequest(t, http.MethodGet, base+"/", "", "")
	var identity struct {
		Version struct {
			Number       string `json:"number"`
			Distribution string `json:"distribution"`
		} `json:"version"`
	}
	if err := json.Unmarshal(root, &identity); err != nil || identity.Version.Number == "" {
		t.Fatalf("server identity = %s, %v", root, err)
	}
	if expected := os.Getenv("SEARCH_MATRIX_VERSION"); expected != "" && !strings.HasPrefix(identity.Version.Number, expected) {
		t.Fatalf("server version = %q, want prefix %q", identity.Version.Number, expected)
	}

	index := "portscope-matrix"
	defer matrixRequest(t, http.MethodDelete, base+"/"+index, "", "")
	matrixRequest(t, http.MethodPut, base+"/"+index, "application/json", `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)
	bulk := fmt.Sprintf("{\"index\":{\"_index\":%q,\"_id\":\"1\"}}\n{\"title\":\"Brave New World\"}\n{\"index\":{\"_index\":%q,\"_id\":\"2\"}}\n{\"title\":\"Island\"}\n", index, index)
	matrixRequest(t, http.MethodPost, base+"/_bulk?refresh=true", "application/x-ndjson", bulk)
	search := matrixRequest(t, http.MethodPost, base+"/"+index+"/_search", "application/json", `{"query":{"match_all":{}},"size":1}`)
	if !bytes.Contains(search, []byte(`"value":2`)) && !bytes.Contains(search, []byte(`"value" : 2`)) {
		t.Fatalf("search response = %s", search)
	}
	msearch := fmt.Sprintf("{\"index\":%q}\n{\"query\":{\"match_all\":{}}}\n{\"index\":%q}\n{\"query\":{\"term\":{\"title.keyword\":\"Island\"}}}\n", index, index)
	matrixRequest(t, http.MethodPost, base+"/_msearch", "application/x-ndjson", msearch)

	found := map[string]observation.Interaction{}
	deadline := time.After(3 * time.Second)
	for found["BULK"].Operation == "" || found["SEARCH"].Operation == "" || found["MSEARCH"].Operation == "" {
		select {
		case item := <-events:
			found[strings.Fields(item.Operation)[0]] = item
		case <-deadline:
			t.Fatalf("missing semantic observations: %#v", found)
		}
	}
	if found["BULK"].Attributes["bulkItems"] != "2" || found["SEARCH"].Attributes["hits"] != "2" || found["MSEARCH"].Attributes["searches"] != "2" {
		t.Fatalf("semantic results: bulk=%#v search=%#v msearch=%#v", found["BULK"], found["SEARCH"], found["MSEARCH"])
	}
}

func matrixRequest(t *testing.T, method, target, contentType, body string) []byte {
	t.Helper()
	request, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode >= 300 && response.StatusCode != http.StatusNotFound {
		t.Fatalf("%s %s: %s: %s", method, target, response.Status, data)
	}
	return data
}
