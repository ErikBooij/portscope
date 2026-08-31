package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RuntimeList returns disposable runtime copies. Environment-backed secrets
// and repository-relative file paths are materialized without changing what
// is persisted or returned by the management interface.
func (s *Store) RuntimeList() ([]Upstream, error) {
	s.mu.RLock()
	items := cloneUpstreams(s.items)
	base := filepath.Dir(s.path)
	s.mu.RUnlock()
	for i := range items {
		if !items[i].Enabled {
			continue
		}
		if err := materializeUpstream(&items[i], base, os.LookupEnv); err != nil {
			return nil, fmt.Errorf("upstream %q: %w", items[i].Name, err)
		}
		if err := Validate(items[i]); err != nil {
			return nil, fmt.Errorf("upstream %q after runtime expansion: %w", items[i].Name, err)
		}
	}
	return items, nil
}

func materializeUpstream(item *Upstream, base string, lookup func(string) (string, bool)) error {
	expand := func(label string, value *string) error {
		resolved, err := expandEnvironment(*value, lookup)
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		*value = resolved
		return nil
	}
	resolveFile := func(label string, value *string) error {
		if err := expand(label, value); err != nil || *value == "" {
			return err
		}
		if !filepath.IsAbs(*value) {
			*value = filepath.Clean(filepath.Join(base, *value))
		}
		return nil
	}
	resolveClientTLS := func(label string, options *ClientTLSOptions) error {
		if options == nil {
			return nil
		}
		files := []struct {
			name  string
			value *string
		}{{"CA file", &options.CAFile}, {"certificate file", &options.CertFile}, {"key file", &options.KeyFile}}
		for _, file := range files {
			if err := resolveFile(label+" "+file.name, file.value); err != nil {
				return err
			}
		}
		return nil
	}
	if item.ListenerTLS != nil {
		files := []struct {
			name  string
			value *string
		}{{"certificate file", &item.ListenerTLS.CertFile}, {"key file", &item.ListenerTLS.KeyFile}, {"client CA file", &item.ListenerTLS.ClientCAFile}}
		for _, file := range files {
			if err := resolveFile("listener TLS "+file.name, file.value); err != nil {
				return err
			}
		}
	}
	if item.HTTP != nil {
		for i := range item.HTTP.RequestHeaders {
			if item.HTTP.RequestHeaders[i].Sensitive {
				if err := expand("sensitive request header "+item.HTTP.RequestHeaders[i].Name, &item.HTTP.RequestHeaders[i].Value); err != nil {
					return err
				}
			}
		}
		for i := range item.HTTP.ResponseHeaders {
			if item.HTTP.ResponseHeaders[i].Sensitive {
				if err := expand("sensitive response header "+item.HTTP.ResponseHeaders[i].Name, &item.HTTP.ResponseHeaders[i].Value); err != nil {
					return err
				}
			}
		}
		if err := resolveClientTLS("HTTP upstream TLS", &item.HTTP.UpstreamTLS); err != nil {
			return err
		}
	}
	type passwordPair struct {
		label string
		value *string
	}
	var passwords []passwordPair
	var tlsOptions *ClientTLSOptions
	switch {
	case item.Redis != nil:
		passwords = []passwordPair{{"Redis listener password", &item.Redis.ListenerPassword}, {"Redis upstream password", &item.Redis.UpstreamPassword}}
		tlsOptions = &item.Redis.UpstreamTLS
	case item.MySQL != nil:
		passwords = []passwordPair{{"MySQL listener password", &item.MySQL.ListenerPassword}, {"MySQL upstream password", &item.MySQL.UpstreamPassword}}
		tlsOptions = &item.MySQL.UpstreamTLS
	case item.Postgres != nil:
		passwords = []passwordPair{{"PostgreSQL listener password", &item.Postgres.ListenerPassword}, {"PostgreSQL upstream password", &item.Postgres.UpstreamPassword}}
		tlsOptions = &item.Postgres.UpstreamTLS
	case item.MongoDB != nil:
		passwords = []passwordPair{{"MongoDB listener password", &item.MongoDB.ListenerPassword}, {"MongoDB upstream password", &item.MongoDB.UpstreamPassword}}
		tlsOptions = &item.MongoDB.UpstreamTLS
	case item.RabbitMQ != nil:
		passwords = []passwordPair{{"RabbitMQ listener password", &item.RabbitMQ.ListenerPassword}, {"RabbitMQ upstream password", &item.RabbitMQ.UpstreamPassword}}
		tlsOptions = &item.RabbitMQ.UpstreamTLS
	}
	for _, password := range passwords {
		if err := expand(password.label, password.value); err != nil {
			return err
		}
	}
	if err := resolveClientTLS("upstream TLS", tlsOptions); err != nil {
		return err
	}
	if item.GRPC != nil {
		if err := resolveFile("gRPC descriptor set", &item.GRPC.DescriptorSetFile); err != nil {
			return err
		}
	}
	return nil
}

func expandEnvironment(value string, lookup func(string) (string, bool)) (string, error) {
	var result strings.Builder
	for {
		start := strings.Index(value, "${")
		if start < 0 {
			result.WriteString(value)
			return result.String(), nil
		}
		result.WriteString(value[:start])
		value = value[start+2:]
		end := strings.IndexByte(value, '}')
		if end < 0 {
			return "", errors.New("unterminated environment reference")
		}
		name := value[:end]
		if !validEnvironmentName(name) {
			return "", fmt.Errorf("invalid environment variable name %q", name)
		}
		replacement, exists := lookup(name)
		if !exists {
			return "", fmt.Errorf("environment variable %s is not set", name)
		}
		result.WriteString(replacement)
		value = value[end+1:]
	}
}

func validEnvironmentName(value string) bool {
	if value == "" || !((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z') || value[0] == '_') {
		return false
	}
	for _, character := range value[1:] {
		if !((character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '_') {
			return false
		}
	}
	return true
}
