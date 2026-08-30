package httpadapter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

func TestElasticsearchProfileInspectsSearchBulkAndMSearch(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "ApiKey upstream-secret" {
			t.Errorf("missing injected authorization")
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch request.URL.Path {
		case "/books/_search":
			_, _ = io.WriteString(writer, `{"took":7,"timed_out":false,"_shards":{"total":3,"successful":3,"failed":0},"hits":{"total":{"value":42,"relation":"gte"},"hits":[]}}`)
		case "/_bulk":
			_, _ = io.WriteString(writer, `{"took":11,"errors":true,"items":[{"index":{"_index":"books","_id":"1","status":201}},{"delete":{"_index":"books","_id":"2","status":404,"error":{"reason":"missing"}}}]}`)
		case "/books/_msearch":
			_, _ = io.WriteString(writer, `{"took":4,"responses":[{"hits":{"total":{"value":1,"relation":"eq"},"hits":[]}},{"status":400,"error":{"reason":"bad query"}}]}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer target.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 3)
	upstream := config.Upstream{ID: "search", Protocol: "elasticsearch", ListenAddr: "127.0.0.1:0", Target: target.URL, HTTP: &config.HTTPOptions{RequestHeaders: []config.HeaderRule{{Action: "set", Name: "Authorization", Value: "ApiKey upstream-secret", Sensitive: true}}}}
	go func() {
		_ = New().Run(ctx, upstream, testSink{events: events}, func(address string) { ready <- address })
	}()
	address := <-ready

	requests := []struct {
		path, contentType, body string
	}{
		{"/books/_search", "application/json", `{"query":{"match":{"title":"brave"}}}`},
		{"/_bulk", "application/x-ndjson", "{\"index\":{\"_index\":\"books\",\"_id\":\"1\"}}\n{\"title\":\"Brave\"}\n{\"delete\":{\"_index\":\"books\",\"_id\":\"2\"}}\n"},
		{"/books/_msearch", "application/x-ndjson", "{}\n{\"query\":{\"match_all\":{}}}\n{}\n{\"query\":{\"broken\":{}}}\n"},
	}
	for _, spec := range requests {
		request, _ := http.NewRequest(http.MethodPost, "http://"+address+spec.path, strings.NewReader(spec.body))
		request.Header.Set("Content-Type", spec.contentType)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}

	search := receiveElasticEvent(t, events)
	if search.Protocol != "elasticsearch" || search.Operation != "SEARCH books" || search.Attributes["hits"] != "42" || search.Attributes["hitRelation"] != "gte" || search.Attributes["serverTookMs"] != "7" || search.Response.Summary != "≥42 hits · 7ms server" {
		t.Fatalf("search observation = %#v", search)
	}
	if encoded, _ := json.Marshal(search.Request.Headers); strings.Contains(string(encoded), "upstream-secret") {
		t.Fatal("Elasticsearch authorization leaked")
	}

	bulk := receiveElasticEvent(t, events)
	if bulk.Operation != "BULK" || bulk.Request.Kind != "json" || bulk.Attributes["bulkItems"] != "2" || bulk.Attributes["bulkFailures"] != "1" || bulk.Outcome != "error" {
		t.Fatalf("bulk observation = %#v", bulk)
	}
	var captured struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(bulk.Request.JSON, &captured); err != nil || len(captured.Items) != 2 || captured.Items[0]["action"] != "index" {
		t.Fatalf("bulk NDJSON capture = %#v, %v", captured, err)
	}

	msearch := receiveElasticEvent(t, events)
	if msearch.Operation != "MSEARCH books" || msearch.Request.Kind != "json" || msearch.Attributes["searches"] != "2" || msearch.Attributes["searchFailures"] != "1" || msearch.Outcome != "error" {
		t.Fatalf("msearch observation = %#v", msearch)
	}
}

func TestElasticOperationClassification(t *testing.T) {
	tests := []struct{ method, path, operation, index string }{
		{"GET", "/", "INFO", ""},
		{"POST", "/logs-*/_search", "SEARCH", "logs-*"},
		{"PUT", "/books/_doc/42", "INDEX", "books"},
		{"DELETE", "/books/_doc/42", "DELETE", "books"},
		{"GET", "/_cluster/health", "CLUSTER HEALTH", ""},
		{"GET", "/_cat/indices", "CAT INDICES", ""},
	}
	for _, test := range tests {
		operation, index := elasticOperation(test.method, test.path)
		if operation != test.operation || index != test.index {
			t.Fatalf("%s %s = %s %s", test.method, test.path, operation, index)
		}
	}
}

func TestPayloadInspectsGzipWithoutChangingWireSize(t *testing.T) {
	body := &captureBuffer{limit: captureLimit}
	var encoded bytes.Buffer
	compressor := gzip.NewWriter(&encoded)
	_, _ = compressor.Write([]byte(`{"hits":{"total":{"value":2,"relation":"eq"}}}`))
	if err := compressor.Close(); err != nil {
		t.Fatal(err)
	}
	_, _ = body.Write(encoded.Bytes())

	captured := payload(http.Header{"Content-Encoding": {"gzip"}}, body, "application/json", "200 OK", nil)
	if captured.Kind != "json" || !json.Valid(captured.JSON) || captured.Size != int64(encoded.Len()) || captured.Truncated {
		t.Fatalf("gzip capture = %#v", captured)
	}
}

func receiveElasticEvent(t *testing.T, events <-chan observation.Interaction) observation.Interaction {
	t.Helper()
	select {
	case item := <-events:
		return item
	case <-time.After(2 * time.Second):
		t.Fatal("Elasticsearch observation was not recorded")
		return observation.Interaction{}
	}
}
