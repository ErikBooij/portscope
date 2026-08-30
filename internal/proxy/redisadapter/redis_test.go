package redisadapter

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type testSink struct{ events chan observation.Interaction }

func (s testSink) Record(item observation.Interaction) { s.events <- item }

func TestRESPReaderHandlesNestedFrames(t *testing.T) {
	frame, err := readFrame(bufio.NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n*2\r\n:1\r\n:2\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if got := frame.value.render(100); got != `["GET", [1, 2]]` {
		t.Fatalf("render = %q", got)
	}
}

func TestRESPReaderHandlesStreamedRESP3Values(t *testing.T) {
	streamedString, err := readFrame(bufio.NewReader(strings.NewReader("$?\r\n;5\r\nhello\r\n;1\r\n!\r\n;0\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if streamedString.value.text != "hello!" {
		t.Fatalf("streamed string = %q", streamedString.value.text)
	}
	streamedMap, err := readFrame(bufio.NewReader(strings.NewReader("%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if got := streamedMap.value.render(100); got != "{a, 1, b, 2}" {
		t.Fatalf("streamed map = %q", got)
	}
}

func TestRESPReaderRejectsCollectionCountBeforeAllocating(t *testing.T) {
	_, err := readFrame(bufio.NewReader(strings.NewReader("*999999999\r\n")))
	if err == nil || !strings.Contains(err.Error(), "collection exceeds") {
		t.Fatalf("expected bounded collection error, got %v", err)
	}
}

func TestTLSUpstreamAuthenticationAndDatabaseSelection(t *testing.T) {
	serverTLS, caPath := testTLSCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	commands := make(chan frame, 3)
	go func() {
		connection, _ := listener.Accept()
		if connection == nil {
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		for _, response := range []string{"+OK\r\n", "+OK\r\n", "+PONG\r\n"} {
			request, readErr := readFrame(reader)
			if readErr != nil {
				return
			}
			commands <- request
			_, _ = io.WriteString(connection, response)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	upstream := config.Upstream{ID: "secure-cache", Protocol: "redis", ListenAddr: "127.0.0.1:0", Target: listener.Addr().String(), Redis: &config.RedisOptions{Username: "service", Password: "top-secret", Database: 4, TLS: config.ClientTLSOptions{Enabled: true, CAFile: caPath}}}
	go func() { _ = New().Run(ctx, upstream, sink, func(addr string) { ready <- addr }) }()
	client, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write(encodeCommand("PING")); err != nil {
		t.Fatal(err)
	}
	response, err := readFrame(bufio.NewReader(client))
	if err != nil || response.value.text != "PONG" {
		t.Fatalf("response = %#v, error = %v", response.value, err)
	}
	for index, want := range []string{"AUTH", "SELECT", "PING"} {
		request := <-commands
		if got := command(request.value); got != want {
			t.Fatalf("command %d = %q, want %q", index, got, want)
		}
		if index == 0 && (request.value.items[1].scalar() != "service" || request.value.items[2].scalar() != "top-secret") {
			t.Fatalf("ACL AUTH frame was malformed: %s", request.value.render(100))
		}
	}
	item := <-sink.events
	if item.Operation != "PING" || strings.Contains(item.Request.Text, "top-secret") || item.Attributes["upstreamTLS"] != "enabled" || item.Attributes["database"] != "4" {
		t.Fatalf("unexpected interaction: %#v", item)
	}
}

func TestRejectedConfiguredCredentialsNeverLeak(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		connection, _ := listener.Accept()
		if connection == nil {
			return
		}
		defer connection.Close()
		_, _ = readFrame(bufio.NewReader(connection))
		_, _ = io.WriteString(connection, "-WRONGPASS invalid username-password pair\r\n")
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	go func() {
		_ = New().Run(ctx, config.Upstream{ID: "bad-auth", Protocol: "redis", ListenAddr: "127.0.0.1:0", Target: listener.Addr().String(), Redis: &config.RedisOptions{Password: "never-expose-me"}}, sink, func(addr string) { ready <- addr })
	}()
	client, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	select {
	case item := <-sink.events:
		if item.Operation != "AUTH" || strings.Contains(item.Error, "never-expose-me") || strings.Contains(item.Response.Text, "never-expose-me") {
			t.Fatalf("unsafe auth failure: %#v", item)
		}
	case <-time.After(time.Second):
		t.Fatal("missing auth failure observation")
	}
}

func testTLSCertificate(t *testing.T) (*tls.Config, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Portscope test"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
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
	caPath := filepath.Join(t.TempDir(), "redis-ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}, caPath
}

func TestProxyCorrelatesPipelinedCommandsAndResponses(t *testing.T) {
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		connection, _ := upstream.Accept()
		if connection == nil {
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		for range 2 {
			_, _ = readFrame(reader)
		}
		_, _ = connection.Write([]byte("|1\r\n+source\r\n+cache\r\n>2\r\n+notice\r\n+warm\r\n+PONG\r\n$5\r\nvalue\r\n"))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 4)}
	go func() {
		_ = New().Run(ctx, config.Upstream{ID: "cache", Protocol: "redis", ListenAddr: "127.0.0.1:0", Target: upstream.Addr().String()}, sink, func(addr string) { ready <- addr })
	}()
	address := <-ready
	client, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, _ = client.Write([]byte("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	reader := bufio.NewReader(client)
	attribute, _ := readFrame(reader)
	push, _ := readFrame(reader)
	first, _ := readFrame(reader)
	second, _ := readFrame(reader)
	if attribute.value.kind != '|' {
		t.Fatalf("first response was not a RESP3 attribute: %#v", attribute.value)
	}
	if push.value.kind != '>' {
		t.Fatalf("first response was not a RESP3 push: %#v", push.value)
	}
	if first.value.text != "PONG" || second.value.text != "value" {
		t.Fatalf("responses = %q, %q", first.value.text, second.value.text)
	}
	for _, want := range []string{"ATTRIBUTE", "PUSH", "PING", "GET"} {
		select {
		case item := <-sink.events:
			if item.Operation != want {
				t.Fatalf("operation = %q, want %q", item.Operation, want)
			}
		case <-time.After(time.Second):
			t.Fatal("missing interaction")
		}
	}
}

func TestAuthPayloadIsRedacted(t *testing.T) {
	request, _ := readFrame(bufio.NewReader(strings.NewReader("*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n")))
	payload := redisPayload(request, "AUTH")
	if command(request.value) == "AUTH" {
		payload.Text = "[redacted]"
	}
	if strings.Contains(payload.Text, "secret") {
		t.Fatal("AUTH secret leaked")
	}
}
