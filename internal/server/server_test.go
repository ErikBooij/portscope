package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy"
)

func TestManagementAPINeverReturnsStoredSecretsAndPreservesThemOnEdit(t *testing.T) {
	handler, configuration := testHandler(t)
	payload := `{"name":"cache","protocol":"redis","listenAddr":"127.0.0.1:6380","target":"127.0.0.1:6379","enabled":false,"redis":{"listenerUsername":"proxy","listenerPassword":"listener-secret","listenerPasswordSet":true,"upstreamUsername":"app","upstreamPassword":"api-secret","upstreamPasswordSet":true,"database":0,"upstreamTls":{"enabled":false}}}`
	create := httptest.NewRequest(http.MethodPost, "/api/upstreams", strings.NewReader(payload))
	create.Header.Set("Content-Type", "application/json")
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusOK || strings.Contains(created.Body.String(), "api-secret") || strings.Contains(created.Body.String(), "listener-secret") || !strings.Contains(created.Body.String(), `"listenerPasswordSet":true`) || !strings.Contains(created.Body.String(), `"upstreamPasswordSet":true`) {
		t.Fatalf("unsafe create response (%d): %s", created.Code, created.Body.String())
	}
	var public config.Upstream
	if err := json.Unmarshal(created.Body.Bytes(), &public); err != nil {
		t.Fatal(err)
	}
	public.Name = "renamed"
	encoded, _ := json.Marshal(public)
	update := httptest.NewRequest(http.MethodPut, "/api/upstreams/"+public.ID, bytes.NewReader(encoded))
	update.SetPathValue("id", public.ID)
	updated := httptest.NewRecorder()
	handler.ServeHTTP(updated, update)
	if updated.Code != http.StatusOK || strings.Contains(updated.Body.String(), "api-secret") {
		t.Fatalf("unsafe update response (%d): %s", updated.Code, updated.Body.String())
	}
	stored, ok := configuration.Get(public.ID)
	if !ok || stored.Redis == nil || stored.Redis.ListenerPassword != "listener-secret" || stored.Redis.UpstreamPassword != "api-secret" {
		t.Fatalf("stored secret was not preserved: %#v", stored)
	}
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/upstreams", nil))
	if strings.Contains(list.Body.String(), "api-secret") || strings.Contains(list.Body.String(), "listener-secret") {
		t.Fatalf("list leaked a secret: %s", list.Body.String())
	}
}

func TestManagementAPIRejectsCrossOriginMutationAndSetsDefensiveHeaders(t *testing.T) {
	handler, _ := testHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/api/demo", nil)
	request.Header.Set("Origin", "https://attacker.example")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("cross-origin status = %d", response.Code)
	}
	if response.Header().Get("Content-Security-Policy") == "" || response.Header().Get("X-Content-Type-Options") != "nosniff" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("missing defensive headers: %#v", response.Header())
	}
}

func testHandler(t *testing.T) (http.Handler, *config.Store) {
	t.Helper()
	directory := t.TempDir()
	configPath := filepath.Join(directory, "upstreams.json")
	if err := os.WriteFile(configPath, []byte("[]"), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration, err := config.OpenStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	observations, err := observation.OpenStore(filepath.Join(directory, "interactions.jsonl"), 20)
	if err != nil {
		t.Fatal(err)
	}
	manager := proxy.NewManager(observations, nil)
	t.Cleanup(manager.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(context.Background(), configuration, observations, manager, logger).Handler(), configuration
}
