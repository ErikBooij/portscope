package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateKeepsProtocolSpecificTargetsDistinct(t *testing.T) {
	cases := []struct {
		name  string
		item  Upstream
		valid bool
	}{{"http", Upstream{Name: "api", Protocol: "http", ListenAddr: "127.0.0.1:9000", Target: "http://localhost:3000"}, true}, {"redis", Upstream{Name: "cache", Protocol: "redis", ListenAddr: "127.0.0.1:6380", Target: "localhost:6379"}, true}, {"http target on redis", Upstream{Name: "cache", Protocol: "redis", ListenAddr: "127.0.0.1:6380", Target: "http://localhost:6379"}, false}, {"bare target on http", Upstream{Name: "api", Protocol: "http", ListenAddr: "127.0.0.1:9000", Target: "localhost:3000"}, false}}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := Validate(test.item)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v error=%v", test.valid, err)
			}
		})
	}
}

func TestValidateRejectsUnsafeHeaderPoliciesAndMismatchedTLS(t *testing.T) {
	base := Upstream{Name: "api", Protocol: "http", ListenAddr: "127.0.0.1:9000", Target: "https://localhost:3000", HTTP: &HTTPOptions{}}
	base.HTTP.RequestHeaders = []HeaderRule{{Action: "set", Name: "X-Test", Value: "ok\r\nevil: yes"}}
	if err := Validate(base); err == nil || !strings.Contains(err.Error(), "line break") {
		t.Fatalf("expected line-break rejection, got %v", err)
	}
	base.HTTP.RequestHeaders = []HeaderRule{{Action: "remove", Name: "Content-Length"}}
	if err := Validate(base); err == nil || !strings.Contains(err.Error(), "managed") {
		t.Fatalf("expected managed-header rejection, got %v", err)
	}
	base.HTTP.RequestHeaders = nil
	base.HTTP.UpstreamTLS = ClientTLSOptions{CertFile: "client.pem"}
	if err := Validate(base); err == nil || !strings.Contains(err.Error(), "require TLS") {
		t.Fatalf("expected disabled-TLS rejection, got %v", err)
	}
}

func TestSecretsAreWriteOnlyAndPreservedOnEdit(t *testing.T) {
	original := Upstream{ID: "cache", Name: "cache", Protocol: "redis", ListenAddr: "127.0.0.1:6380", Target: "localhost:6379", Redis: &RedisOptions{Username: "app", Password: "redis-secret"}}
	public := PublicUpstream(original)
	if public.Redis.Password != "" || !public.Redis.PasswordSet {
		t.Fatalf("public Redis credentials leaked or lost state: %#v", public.Redis)
	}
	incoming := public
	merged := MergeSecrets(incoming, original)
	if merged.Redis.Password != "redis-secret" {
		t.Fatal("blank write-only password did not preserve the existing secret")
	}

	httpOriginal := Upstream{ID: "api", Name: "api", Protocol: "http", ListenAddr: "127.0.0.1:9000", Target: "http://localhost:3000", HTTP: &HTTPOptions{RequestHeaders: []HeaderRule{{Action: "set", Name: "Authorization", Value: "Bearer secret", Sensitive: true}}}}
	httpPublic := PublicUpstream(httpOriginal)
	if httpPublic.HTTP.RequestHeaders[0].Value != "" || !httpPublic.HTTP.RequestHeaders[0].ValueSet {
		t.Fatal("sensitive injected header was not returned write-only")
	}
	if got := MergeSecrets(httpPublic, httpOriginal).HTTP.RequestHeaders[0].Value; got != "Bearer secret" {
		t.Fatalf("merged header secret = %q", got)
	}
}

func TestStoreRejectsDuplicateEnabledListeners(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upstreams.json")
	if err := os.WriteFile(path, []byte("[]"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	first := Upstream{Name: "one", Protocol: "redis", ListenAddr: "127.0.0.1:6380", Target: "localhost:6379", Enabled: true}
	if _, err := store.Put(first); err != nil {
		t.Fatal(err)
	}
	second := Upstream{Name: "two", Protocol: "http", ListenAddr: "127.0.0.1:6380", Target: "http://localhost:3000", Enabled: true}
	if _, err := store.Put(second); err == nil || !strings.Contains(err.Error(), "already used") {
		t.Fatalf("expected duplicate listener error, got %v", err)
	}
}
