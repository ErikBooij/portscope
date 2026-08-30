package httpadapter

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestProxyCapturesHTTPExchangeAndRedactsCredentials(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/base/orders" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":42}`)
	}))
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	go func() {
		_ = New().Run(ctx, config.Upstream{ID: "orders", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: target.URL + "/base"}, sink, func(addr string) { ready <- addr })
	}()
	var address string
	select {
	case address = <-ready:
	case <-time.After(time.Second):
		t.Fatal("proxy did not become ready")
	}
	request, _ := http.NewRequest(http.MethodPost, "http://"+address+"/orders", strings.NewReader(`{"sku":"lamp"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", response.StatusCode)
	}
	select {
	case item := <-sink.events:
		if item.Operation != "POST /orders" || item.Attributes["status"] != "201" {
			t.Fatalf("unexpected interaction: %#v", item)
		}
		if string(item.Request.JSON) != `{"sku":"lamp"}` {
			t.Fatalf("request = %s", item.Request.JSON)
		}
		encoded, _ := json.Marshal(item.Request.Headers)
		if strings.Contains(string(encoded), "secret") {
			t.Fatal("authorization was not redacted")
		}
	case <-time.After(time.Second):
		t.Fatal("interaction was not recorded")
	}
}

func TestWebSocketUpgradeForwardsFramesAndRecordsBothDirections(t *testing.T) {
	upstreamHeaders := make(chan http.Header, 1)
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamHeaders <- request.Header.Clone()
		connection, buffered, err := http.NewResponseController(writer).Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		accept := webSocketAccept(request.Header.Get("Sec-WebSocket-Key"))
		_, _ = fmt.Fprintf(buffered, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade, X-Upstream-Hop\r\nX-Upstream-Hop: remove-me\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: chat\r\n\r\n", accept)
		_ = buffered.Flush()
		incoming, err := forwardWebSocketFrame(io.Discard, buffered)
		if err != nil {
			return
		}
		_ = writeTestWebSocketFrame(connection, 1, incoming.body.Bytes(), false)
	}))
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 8)}
	upstream := config.Upstream{ID: "socket", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: target.URL + "/base", HTTP: &config.HTTPOptions{
		RequestHeaders:  []config.HeaderRule{{Action: "set", Name: "X-WebSocket-Test", Value: "injected"}, {Action: "set", Name: "Authorization", Value: "Bearer websocket-secret", Sensitive: true}},
		ResponseHeaders: []config.HeaderRule{{Action: "set", Name: "X-Proxied-WebSocket", Value: "yes"}},
	}}
	go func() { _ = New().Run(ctx, upstream, sink, func(address string) { ready <- address }) }()
	connection, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	_, _ = fmt.Fprintf(connection, "GET /socket?room=one HTTP/1.1\r\nHost: proxy.test\r\nConnection: keep-alive, X-Hop, Upgrade\r\nUpgrade: websocket\r\nX-Hop: remove-me\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Protocol: chat\r\n\r\n", key)
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols || response.Header.Get("X-Proxied-WebSocket") != "yes" || response.Header.Get("Sec-WebSocket-Accept") != webSocketAccept(key) || response.Header.Get("X-Upstream-Hop") != "" {
		t.Fatalf("unexpected upgrade response: %#v", response)
	}
	if err := writeTestWebSocketFrame(connection, 1, []byte(`{"hello":"world"}`), true); err != nil {
		t.Fatal(err)
	}
	echoed, err := forwardWebSocketFrame(io.Discard, reader)
	if err != nil {
		t.Fatal(err)
	}
	if echoed.masked || string(echoed.body.Bytes()) != `{"hello":"world"}` {
		t.Fatalf("echoed frame = %#v %q", echoed, echoed.body.Bytes())
	}
	headers := <-upstreamHeaders
	if headers.Get("X-WebSocket-Test") != "injected" || headers.Get("Authorization") != "Bearer websocket-secret" || headers.Get("X-Hop") != "" {
		t.Fatalf("header policies were not applied to upgrade: %#v", headers)
	}

	interactions := make([]observation.Interaction, 0, 3)
	for len(interactions) < 3 {
		select {
		case item := <-sink.events:
			interactions = append(interactions, item)
		case <-time.After(time.Second):
			t.Fatalf("only received %d WebSocket observations: %#v", len(interactions), interactions)
		}
	}
	var handshake, sent, received *observation.Interaction
	for index := range interactions {
		item := &interactions[index]
		switch item.Attributes["direction"] {
		case "handshake":
			handshake = item
		case "client_to_upstream":
			sent = item
		case "upstream_to_client":
			received = item
		}
	}
	if handshake == nil || handshake.Protocol != "websocket" || handshake.Attributes["subprotocol"] != "chat" {
		t.Fatalf("missing handshake observation: %#v", interactions)
	}
	encoded, _ := json.Marshal(handshake.Request.Headers)
	if strings.Contains(string(encoded), "websocket-secret") {
		t.Fatalf("WebSocket handshake leaked a sensitive header: %s", encoded)
	}
	if sent == nil || received == nil || sent.Operation != "WS → TEXT /socket?room=one" || received.Operation != "WS ← TEXT /socket?room=one" || string(sent.Request.JSON) != `{"hello":"world"}` || string(received.Response.JSON) != `{"hello":"world"}` {
		t.Fatalf("unexpected frame observations: %#v", interactions)
	}
}

func TestWebSocketConnectionClosesWhenAdapterContextIsCancelled(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, buffered, err := http.NewResponseController(writer).Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		_, _ = fmt.Fprintf(buffered, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", webSocketAccept(request.Header.Get("Sec-WebSocket-Key")))
		_ = buffered.Flush()
		_, _ = io.Copy(io.Discard, buffered)
	}))
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- New().Run(ctx, config.Upstream{ID: "cancel", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: target.URL}, testSink{events: make(chan observation.Interaction, 4)}, func(address string) { ready <- address })
	}()
	connection, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	key := "Y2FuY2VsIHdlYnNvY2tldA=="
	_, _ = fmt.Fprintf(connection, "GET /socket HTTP/1.1\r\nHost: proxy.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", key)
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade response = %#v, error = %v", response, err)
	}
	cancel()
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("WebSocket client connection remained open after adapter cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("adapter did not stop after context cancellation")
	}
}

func TestWebSocketFrameCaptureIsBoundedWithoutLimitingForwarding(t *testing.T) {
	payload := strings.Repeat("x", captureLimit+1024)
	var wire strings.Builder
	if err := writeTestWebSocketFrame(&wire, 1, []byte(payload), true); err != nil {
		t.Fatal(err)
	}
	var forwarded strings.Builder
	frame, err := forwardWebSocketFrame(&forwarded, strings.NewReader(wire.String()))
	if err != nil {
		t.Fatal(err)
	}
	if frame.length != int64(len(payload)) || !frame.body.truncated || frame.body.Len() != captureLimit || forwarded.String() != wire.String() {
		t.Fatalf("bounded forwarding failed: length=%d capture=%d truncated=%v", frame.length, frame.body.Len(), frame.body.truncated)
	}
}

func TestWebSocketUpgradeUsesVerifiedTLSUpstream(t *testing.T) {
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, buffered, err := http.NewResponseController(writer).Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		_, _ = fmt.Fprintf(buffered, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", webSocketAccept(request.Header.Get("Sec-WebSocket-Key")))
		_ = buffered.Flush()
		incoming, err := forwardWebSocketFrame(io.Discard, buffered)
		if err == nil {
			_ = writeTestWebSocketFrame(connection, 1, incoming.body.Bytes(), false)
		}
	}))
	target.StartTLS()
	defer target.Close()
	caPath := filepath.Join(t.TempDir(), "websocket-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: target.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 4)}
	upstream := config.Upstream{ID: "wss", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: target.URL, HTTP: &config.HTTPOptions{UpstreamTLS: config.ClientTLSOptions{Enabled: true, CAFile: caPath}}}
	go func() { _ = New().Run(ctx, upstream, sink, func(address string) { ready <- address }) }()
	connection, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	key := "c2VjdXJlIHdlYnNvY2tldA=="
	_, _ = fmt.Fprintf(connection, "GET /secure HTTP/1.1\r\nHost: proxy.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", key)
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("secure upgrade response = %#v, error = %v", response, err)
	}
	if err := writeTestWebSocketFrame(connection, 1, []byte("secure"), true); err != nil {
		t.Fatal(err)
	}
	frame, err := forwardWebSocketFrame(io.Discard, reader)
	if err != nil || string(frame.body.Bytes()) != "secure" {
		t.Fatalf("secure echo = %q, error = %v", frame.body.Bytes(), err)
	}
	select {
	case item := <-sink.events:
		if item.Attributes["direction"] != "handshake" || item.Attributes["upstreamTLS"] == "" {
			t.Fatalf("missing WSS handshake metadata: %#v", item)
		}
	case <-time.After(time.Second):
		t.Fatal("missing WSS handshake observation")
	}
}

func webSocketAccept(key string) string {
	digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(digest[:])
}

func writeTestWebSocketFrame(writer io.Writer, opcode byte, payload []byte, masked bool) error {
	header := []byte{0x80 | opcode}
	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch {
	case len(payload) < 126:
		header = append(header, maskBit|byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, maskBit|126, byte(len(payload)>>8), byte(len(payload)))
	default:
		header = append(header, maskBit|127)
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(payload)))
		header = append(header, length[:]...)
	}
	data := append([]byte(nil), payload...)
	if masked {
		mask := [4]byte{0x12, 0x34, 0x56, 0x78}
		header = append(header, mask[:]...)
		for index := range data {
			data[index] ^= mask[index%4]
		}
	}
	if err := writeWebSocketBytes(writer, header); err != nil {
		return err
	}
	return writeWebSocketBytes(writer, data)
}

func TestHeaderPoliciesMutateBothDirectionsAndRedactInjectedSecrets(t *testing.T) {
	received := make(chan http.Header, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		w.Header().Set("X-Remove-Response", "gone")
		w.Header().Set("X-Replace", "old")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	upstream := config.Upstream{ID: "policy", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: target.URL, HTTP: &config.HTTPOptions{
		RequestHeaders:  []config.HeaderRule{{Action: "set", Name: "X-Tenant", Value: "portscope"}, {Action: "add", Name: "X-Multi", Value: "two"}, {Action: "remove", Name: "X-Remove"}, {Action: "set", Name: "X-Injected-Secret", Value: "classified", Sensitive: true}},
		ResponseHeaders: []config.HeaderRule{{Action: "remove", Name: "X-Remove-Response"}, {Action: "set", Name: "X-Replace", Value: "new"}},
	}}
	go func() { _ = New().Run(ctx, upstream, sink, func(addr string) { ready <- addr }) }()
	address := <-ready
	request, _ := http.NewRequest(http.MethodGet, "http://"+address+"/", nil)
	request.Header.Set("X-Multi", "one")
	request.Header.Set("X-Remove", "please")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.Header.Get("X-Remove-Response") != "" || response.Header.Get("X-Replace") != "new" {
		t.Fatalf("unexpected response headers: %#v", response.Header)
	}
	headers := <-received
	if headers.Get("X-Tenant") != "portscope" || headers.Get("X-Remove") != "" || strings.Join(headers.Values("X-Multi"), ",") != "one,two" {
		t.Fatalf("unexpected upstream request headers: %#v", headers)
	}
	item := <-sink.events
	encoded, _ := json.Marshal(item.Request.Headers)
	if strings.Contains(string(encoded), "classified") || !strings.Contains(string(encoded), "[redacted]") {
		t.Fatalf("sensitive injected value was not redacted: %s", encoded)
	}
}

func TestHTTPSUpstreamNegotiatesHTTP2WithCustomCA(t *testing.T) {
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	target.EnableHTTP2 = true
	target.StartTLS()
	defer target.Close()
	certificate := target.Certificate()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	upstream := config.Upstream{ID: "h2", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: target.URL, HTTP: &config.HTTPOptions{UpstreamTLS: config.ClientTLSOptions{Enabled: true, CAFile: caPath}}}
	go func() { _ = New().Run(ctx, upstream, sink, func(addr string) { ready <- addr }) }()
	response, err := http.Get("http://" + <-ready + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if string(body) != "HTTP/2.0" {
		t.Fatalf("upstream protocol = %q", body)
	}
	item := <-sink.events
	if item.Attributes["upstreamProtocol"] != "HTTP/2.0" || item.Attributes["upstreamTLS"] == "" {
		t.Fatalf("missing protocol metadata: %#v", item.Attributes)
	}
}

func TestH2CUpstreamUsesCleartextHTTP2(t *testing.T) {
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	target.Config.Protocols = new(http.Protocols)
	target.Config.Protocols.SetHTTP1(true)
	target.Config.Protocols.SetUnencryptedHTTP2(true)
	target.Start()
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	targetURL := "h2c://" + strings.TrimPrefix(target.URL, "http://")
	go func() {
		_ = New().Run(ctx, config.Upstream{ID: "h2c-upstream", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: targetURL}, sink, func(addr string) { ready <- addr })
	}()
	response, err := http.Get("http://" + <-ready + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if string(body) != "HTTP/2.0" {
		t.Fatalf("h2c upstream protocol = %q", body)
	}
	if item := <-sink.events; item.Attributes["upstreamProtocol"] != "HTTP/2.0" {
		t.Fatalf("capture metadata = %#v", item.Attributes)
	}
}

func TestPlaintextListenerAcceptsHTTP2(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	go func() {
		_ = New().Run(ctx, config.Upstream{ID: "echo", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: "internal://echo"}, sink, func(addr string) { ready <- addr })
	}()
	transport := &http.Transport{Protocols: new(http.Protocols)}
	transport.Protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	response, err := client.Get("http://" + <-ready + "/h2c")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.Proto != "HTTP/2.0" {
		t.Fatalf("downstream protocol = %s", response.Proto)
	}
	if item := <-sink.events; item.Attributes["downstreamProtocol"] != "HTTP/2.0" {
		t.Fatalf("capture metadata = %#v", item.Attributes)
	}
}

func TestTLSListenerNegotiatesHTTP2(t *testing.T) {
	certPath, keyPath, pool := testServerCertificate(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 1)}
	upstream := config.Upstream{ID: "https-listener", Protocol: "http", ListenAddr: "127.0.0.1:0", Target: "internal://echo", ListenerTLS: &config.ListenerTLSOptions{Enabled: true, CertFile: certPath, KeyFile: keyPath}}
	go func() { _ = New().Run(ctx, upstream, sink, func(addr string) { ready <- addr }) }()
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}, ForceAttemptHTTP2: true}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	response, err := client.Get("https://" + <-ready + "/h2")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.Proto != "HTTP/2.0" {
		t.Fatalf("listener protocol = %s", response.Proto)
	}
	if item := <-sink.events; item.Attributes["downstreamProtocol"] != "HTTP/2.0" {
		t.Fatalf("capture metadata = %#v", item.Attributes)
	}
}

func testServerCertificate(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "Portscope HTTP test"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	directory := t.TempDir()
	certPath, keyPath := filepath.Join(directory, "server.pem"), filepath.Join(directory, "server-key.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return certPath, keyPath, pool
}
