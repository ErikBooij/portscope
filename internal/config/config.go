package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Upstream struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	Protocol    string              `json:"protocol"`
	ListenAddr  string              `json:"listenAddr"`
	Target      string              `json:"target"`
	Enabled     bool                `json:"enabled"`
	ListenerTLS *ListenerTLSOptions `json:"listenerTls,omitempty"`
	HTTP        *HTTPOptions        `json:"http,omitempty"`
	Redis       *RedisOptions       `json:"redis,omitempty"`
}

type ListenerTLSOptions struct {
	Enabled           bool   `json:"enabled"`
	CertFile          string `json:"certFile,omitempty"`
	KeyFile           string `json:"keyFile,omitempty"`
	ClientCAFile      string `json:"clientCaFile,omitempty"`
	RequireClientCert bool   `json:"requireClientCert,omitempty"`
}

type ClientTLSOptions struct {
	Enabled            bool   `json:"enabled"`
	ServerName         string `json:"serverName,omitempty"`
	CAFile             string `json:"caFile,omitempty"`
	CertFile           string `json:"certFile,omitempty"`
	KeyFile            string `json:"keyFile,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`
}

type HeaderRule struct {
	Action    string `json:"action"`
	Name      string `json:"name"`
	Value     string `json:"value,omitempty"`
	Sensitive bool   `json:"sensitive,omitempty"`
	ValueSet  bool   `json:"valueSet,omitempty"`
}

type HTTPOptions struct {
	PreserveHost    bool             `json:"preserveHost,omitempty"`
	RequestHeaders  []HeaderRule     `json:"requestHeaders,omitempty"`
	ResponseHeaders []HeaderRule     `json:"responseHeaders,omitempty"`
	UpstreamTLS     ClientTLSOptions `json:"upstreamTls,omitempty"`
}

type RedisOptions struct {
	Username    string           `json:"username,omitempty"`
	Password    string           `json:"password,omitempty"`
	PasswordSet bool             `json:"passwordSet,omitempty"`
	Database    int              `json:"database,omitempty"`
	TLS         ClientTLSOptions `json:"tls,omitempty"`
}

type Store struct {
	mu    sync.RWMutex
	path  string
	items []Upstream
}

func OpenStore(path string) (*Store, error) {
	store := &Store{path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		store.items = []Upstream{{ID: "demo-http", Name: "Portscope Echo Lab", Protocol: "http", ListenAddr: "127.0.0.1:9081", Target: "internal://echo", Enabled: true, HTTP: &HTTPOptions{}}}
		return store, store.persist()
	}
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &store.items); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	listeners := make(map[string]string)
	for _, item := range store.items {
		if err := Validate(item); err != nil {
			return nil, fmt.Errorf("invalid upstream %q: %w", item.Name, err)
		}
		if item.Enabled {
			if owner, exists := listeners[item.ListenAddr]; exists {
				return nil, fmt.Errorf("invalid upstream %q: listen address %s is already used by %q", item.Name, item.ListenAddr, owner)
			}
			listeners[item.ListenAddr] = item.Name
		}
	}
	return store, nil
}

func (s *Store) List() []Upstream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneUpstreams(s.items)
}

func (s *Store) Get(id string) (Upstream, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, item := range s.items {
		if item.ID == id {
			return cloneUpstream(item), true
		}
	}
	return Upstream{}, false
}

func (s *Store) Put(item Upstream) (Upstream, error) {
	if err := Validate(item); err != nil {
		return Upstream{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if item.ID == "" {
		item.ID = newID()
	}
	for _, candidate := range s.items {
		if candidate.ID != item.ID && candidate.Enabled && item.Enabled && candidate.ListenAddr == item.ListenAddr {
			return Upstream{}, fmt.Errorf("listen address %s is already used by %q", item.ListenAddr, candidate.Name)
		}
	}
	for i := range s.items {
		if s.items[i].ID == item.ID {
			s.items[i] = cloneUpstream(item)
			return cloneUpstream(item), s.persistLocked()
		}
	}
	s.items = append(s.items, cloneUpstream(item))
	return cloneUpstream(item), s.persistLocked()
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		if s.items[i].ID == id {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return s.persistLocked()
		}
	}
	return os.ErrNotExist
}

func Validate(item Upstream) error {
	if strings.TrimSpace(item.Name) == "" {
		return errors.New("name is required")
	}
	if item.Protocol != "http" && item.Protocol != "redis" {
		return errors.New("protocol must be http or redis")
	}
	host, port, err := net.SplitHostPort(item.ListenAddr)
	if err != nil || port == "" {
		return errors.New("listen address must be host:port")
	}
	if host != "localhost" && net.ParseIP(host) == nil {
		return errors.New("listen host must be an IP address or localhost")
	}
	if err := validateListenerTLS(item.ListenerTLS); err != nil {
		return err
	}
	if item.Protocol == "http" {
		return validateHTTP(item)
	}
	return validateRedis(item)
}

func validateHTTP(item Upstream) error {
	if item.Target != "internal://echo" {
		parsed, err := url.Parse(item.Target)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "h2c") || parsed.Host == "" {
			return errors.New("HTTP target must be an http://, https://, or h2c:// URL")
		}
		if parsed.User != nil {
			return errors.New("HTTP target credentials are not allowed; inject an Authorization header instead")
		}
	}
	if item.Redis != nil {
		return errors.New("Redis settings are only valid for Redis upstreams")
	}
	if item.HTTP == nil {
		return nil
	}
	if item.HTTP.UpstreamTLS.Enabled && item.Target != "internal://echo" {
		parsed, _ := url.Parse(item.Target)
		if parsed.Scheme != "https" {
			return errors.New("HTTP upstream TLS options require an https:// target")
		}
	}
	if err := validateClientTLS(item.HTTP.UpstreamTLS, "HTTP upstream TLS"); err != nil {
		return err
	}
	for _, group := range [][]HeaderRule{item.HTTP.RequestHeaders, item.HTTP.ResponseHeaders} {
		for _, rule := range group {
			if err := validateHeaderRule(rule); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRedis(item Upstream) error {
	if _, _, err := net.SplitHostPort(item.Target); err != nil {
		return errors.New("Redis target must be host:port")
	}
	if item.HTTP != nil {
		return errors.New("HTTP settings are only valid for HTTP upstreams")
	}
	if item.ListenerTLS != nil && item.ListenerTLS.Enabled {
		return errors.New("TLS listeners are currently supported for HTTP upstreams only")
	}
	if item.Redis == nil {
		return nil
	}
	if item.Redis.Database < 0 {
		return errors.New("Redis database must be zero or greater")
	}
	if item.Redis.Username != "" && item.Redis.Password == "" && !item.Redis.PasswordSet {
		return errors.New("Redis username requires a password")
	}
	return validateClientTLS(item.Redis.TLS, "Redis TLS")
}

func validateListenerTLS(options *ListenerTLSOptions) error {
	if options == nil || !options.Enabled {
		return nil
	}
	if options.CertFile == "" || options.KeyFile == "" {
		return errors.New("listener TLS requires both a certificate file and key file")
	}
	if options.RequireClientCert && options.ClientCAFile == "" {
		return errors.New("requiring client certificates also requires a client CA file")
	}
	return nil
}

func validateClientTLS(options ClientTLSOptions, label string) error {
	if !options.Enabled {
		if options.ServerName != "" || options.CAFile != "" || options.CertFile != "" || options.KeyFile != "" || options.InsecureSkipVerify {
			return fmt.Errorf("%s options require TLS to be enabled", label)
		}
		return nil
	}
	if (options.CertFile == "") != (options.KeyFile == "") {
		return fmt.Errorf("%s client certificate and key must be configured together", label)
	}
	return nil
}

var blockedHeaders = map[string]struct{}{
	"connection": {}, "proxy-connection": {}, "keep-alive": {}, "proxy-authenticate": {}, "proxy-authorization": {},
	"te": {}, "trailer": {}, "transfer-encoding": {}, "upgrade": {}, "host": {}, "content-length": {},
}

func validateHeaderRule(rule HeaderRule) error {
	if rule.Action != "set" && rule.Action != "add" && rule.Action != "remove" {
		return errors.New("header action must be set, add, or remove")
	}
	if !validHeaderName(rule.Name) {
		return fmt.Errorf("invalid header name %q", rule.Name)
	}
	if _, blocked := blockedHeaders[strings.ToLower(rule.Name)]; blocked {
		return fmt.Errorf("header %q is managed by the proxy and cannot be changed", rule.Name)
	}
	if strings.ContainsAny(rule.Value, "\r\n") {
		return fmt.Errorf("header %q contains a line break", rule.Name)
	}
	if rule.Action == "remove" && rule.Value != "" {
		return fmt.Errorf("remove rule for header %q cannot have a value", rule.Name)
	}
	return nil
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	const separators = "()<>@,;:\\\"/[]?={} \t"
	for _, char := range name {
		if char < 33 || char > 126 || strings.ContainsRune(separators, char) {
			return false
		}
	}
	return true
}

// PublicUpstreams returns an API-safe deep copy. Secrets are never serialized back to a client.
func PublicUpstreams(items []Upstream) []Upstream {
	result := cloneUpstreams(items)
	for i := range result {
		if result[i].Redis != nil {
			result[i].Redis.PasswordSet = result[i].Redis.Password != ""
			result[i].Redis.Password = ""
		}
		if result[i].HTTP != nil {
			redactRules(result[i].HTTP.RequestHeaders)
			redactRules(result[i].HTTP.ResponseHeaders)
		}
	}
	return result
}

func PublicUpstream(item Upstream) Upstream { return PublicUpstreams([]Upstream{item})[0] }

// MergeSecrets interprets an empty write-only value with ValueSet/PasswordSet as "keep existing".
func MergeSecrets(incoming, existing Upstream) Upstream {
	if incoming.Redis != nil && existing.Redis != nil && incoming.Redis.Password == "" && incoming.Redis.PasswordSet {
		incoming.Redis.Password = existing.Redis.Password
	}
	if incoming.HTTP != nil && existing.HTTP != nil {
		mergeRuleSecrets(incoming.HTTP.RequestHeaders, existing.HTTP.RequestHeaders)
		mergeRuleSecrets(incoming.HTTP.ResponseHeaders, existing.HTTP.ResponseHeaders)
	}
	return incoming
}

func redactRules(rules []HeaderRule) {
	for i := range rules {
		if rules[i].Sensitive {
			rules[i].ValueSet = rules[i].Value != ""
			rules[i].Value = ""
		}
	}
}

func mergeRuleSecrets(incoming, existing []HeaderRule) {
	used := make([]bool, len(existing))
	for i := range incoming {
		if !incoming[i].Sensitive || incoming[i].Value != "" || !incoming[i].ValueSet {
			continue
		}
		for j := range existing {
			if !used[j] && existing[j].Sensitive && strings.EqualFold(existing[j].Name, incoming[i].Name) && existing[j].Action == incoming[i].Action {
				incoming[i].Value = existing[j].Value
				used[j] = true
				break
			}
		}
	}
}

func cloneUpstreams(items []Upstream) []Upstream {
	result := make([]Upstream, len(items))
	for i := range items {
		result[i] = cloneUpstream(items[i])
	}
	return result
}

func cloneUpstream(item Upstream) Upstream {
	data, _ := json.Marshal(item)
	var result Upstream
	_ = json.Unmarshal(data, &result)
	return result
}

func (s *Store) persist() error { s.mu.Lock(); defer s.mu.Unlock(); return s.persistLocked() }
func (s *Store) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	data, err := json.MarshalIndent(s.items, "", "  ")
	if err != nil {
		return err
	}
	temporary := s.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, s.path); err != nil {
		return err
	}
	if directory, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func newID() string {
	var data [6]byte
	if _, err := rand.Read(data[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(data[:])
}
