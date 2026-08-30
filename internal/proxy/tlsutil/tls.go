package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/erikbooij/portscope/internal/config"
)

func Client(options config.ClientTLSOptions, defaultServerName string) (*tls.Config, error) {
	if !options.Enabled {
		return nil, nil
	}
	configuration := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         options.ServerName,
		InsecureSkipVerify: options.InsecureSkipVerify, // Explicit opt-out exposed with a dashboard warning.
	}
	if configuration.ServerName == "" {
		configuration.ServerName = defaultServerName
	}
	if options.CAFile != "" {
		pool, err := certificatePool(options.CAFile)
		if err != nil {
			return nil, fmt.Errorf("load CA file: %w", err)
		}
		configuration.RootCAs = pool
	}
	if options.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		configuration.Certificates = []tls.Certificate{certificate}
	}
	return configuration, nil
}

func Server(options config.ListenerTLSOptions) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(options.CertFile, options.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load listener certificate: %w", err)
	}
	configuration := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, NextProtos: []string{"h2", "http/1.1"}}
	if options.ClientCAFile != "" {
		pool, err := certificatePool(options.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("load client CA file: %w", err)
		}
		configuration.ClientCAs = pool
		if options.RequireClientCert {
			configuration.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			configuration.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return configuration, nil
}

func certificatePool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("%s contains no PEM certificates", path)
	}
	return pool, nil
}
