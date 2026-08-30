package postgresadapter

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

func TestListenerAcceptsClassicAndDirectTLSNegotiation(t *testing.T) {
	serverTLS, roots, _ := postgresTestTLSCertificate(t)
	for _, direct := range []bool{false, true} {
		name := "ssl-request"
		if direct {
			name = "direct"
		}
		t.Run(name, func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type result struct {
				startup []byte
				secure  *tls.Conn
				err     error
			}
			completed := make(chan result, 1)
			go func() {
				startup, secure, _, err := New().acceptStartup(ctx, server, bufio.NewReader(server), serverTLS)
				completed <- result{startup: startup, secure: secure, err: err}
			}()

			if !direct {
				if err := writeStartup(client, appendInt32(nil, sslRequestCode)); err != nil {
					t.Fatal(err)
				}
				var response [1]byte
				if _, err := client.Read(response[:]); err != nil || response[0] != 'S' {
					t.Fatalf("SSLRequest response = %q, %v", response, err)
				}
			}
			secureClient := tls.Client(client, &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1", NextProtos: []string{"postgresql"}})
			if err := secureClient.HandshakeContext(ctx); err != nil {
				t.Fatal(err)
			}
			if err := writeStartup(secureClient, buildStartup(nil, "app", "database")); err != nil {
				t.Fatal(err)
			}
			got := <-completed
			if got.err != nil || got.secure == nil {
				t.Fatalf("accept startup = secure %v, error %v", got.secure != nil, got.err)
			}
			parameters, err := parseStartup(got.startup)
			if err != nil || parameters["user"] != "app" || parameters["database"] != "database" {
				t.Fatalf("startup parameters = %#v, %v", parameters, err)
			}
		})
	}
}

func TestCancellationUsesConfiguredUpstreamTLS(t *testing.T) {
	serverTLS, _, caPath := postgresTestTLSCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan cancelKey, 1)
	serverDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		payload, err := readStartup(bufio.NewReader(connection))
		if err != nil {
			serverDone <- err
			return
		}
		code, _ := int32At(payload, 0)
		if code != sslRequestCode {
			serverDone <- &unexpectedCodeError{got: code, want: sslRequestCode}
			return
		}
		if _, err := connection.Write([]byte{'S'}); err != nil {
			serverDone <- err
			return
		}
		secure := tls.Server(connection, serverTLS)
		if err := secure.Handshake(); err != nil {
			serverDone <- err
			return
		}
		payload, err = readStartup(bufio.NewReader(secure))
		if err != nil {
			serverDone <- err
			return
		}
		pid, _ := int32At(payload, 4)
		secret, _ := int32At(payload, 8)
		received <- cancelKey{pid: pid, secret: secret}
		serverDone <- nil
	}()

	local := cancelKey{pid: 10, secret: 20}
	actual := cancelKey{pid: 30, secret: 40}
	adapter := New()
	adapter.cancels[local] = cancelTarget{address: listener.Addr().String(), pid: actual.pid, secret: actual.secret}
	events := make(chan observation.Interaction, 1)
	upstream := config.Upstream{ID: "postgres", Target: listener.Addr().String(), Postgres: &config.PostgresOptions{UpstreamTLS: config.ClientTLSOptions{Enabled: true, CAFile: caPath}}}
	adapter.forwardCancel(context.Background(), local, upstream, collectingSink{events: events}, "connection", time.Now())
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if got := <-received; got != actual {
		t.Fatalf("upstream cancel key = %#v, want %#v", got, actual)
	}
	if event := <-events; event.Outcome != "ok" {
		t.Fatalf("cancel observation = %#v", event)
	}
}

type unexpectedCodeError struct{ got, want int32 }

func (err *unexpectedCodeError) Error() string { return "unexpected PostgreSQL startup code" }

func postgresTestTLSCertificate(t *testing.T) (*tls.Config, *x509.CertPool, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Portscope PostgreSQL test"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("could not load PostgreSQL test CA")
	}
	caPath := filepath.Join(t.TempDir(), "postgres-ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, NextProtos: []string{"postgresql"}}, roots, caPath
}
