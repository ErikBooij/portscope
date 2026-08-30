package mysqladapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type testSink struct{ events chan observation.Interaction }

func (sink testSink) Record(item observation.Interaction) { sink.events <- item }

func TestAdapterTerminatesAuthenticationAndCapturesTextResult(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamListener.Close()
	serverDone := make(chan error, 1)
	serverScramble := []byte("12345678901234567890")
	go func() {
		connection, acceptErr := upstreamListener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		capabilities := inspectableCapabilities | clientCompress | clientLocalFiles | clientQueryAttributes
		if err := writePacket(connection, packet{sequence: 0, payload: makeGreeting(77, serverScramble, capabilities, false, "5.6.51")}); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(connection)
		handshakePacket, err := readPacket(reader)
		if err != nil {
			serverDone <- err
			return
		}
		handshake, err := parseClientHandshake(handshakePacket.payload)
		if err != nil {
			serverDone <- err
			return
		}
		if handshake.username != "database-user" || handshake.database != "appdb" || !verifyNativePassword(handshake.authResponse, "database-password", serverScramble) {
			serverDone <- errUnexpected("upstream received wrong independent credentials")
			return
		}
		if handshake.capabilities&(clientCompress|clientLocalFiles|clientQueryAttributes) != 0 {
			serverDone <- errUnexpected("unsafe capabilities reached upstream")
			return
		}
		if err := writePacket(connection, packet{sequence: 2, payload: mysqlOKPayload()}); err != nil {
			serverDone <- err
			return
		}
		query, err := readPacket(reader)
		if err != nil {
			serverDone <- err
			return
		}
		if query.sequence != 0 || string(query.payload) != "\x03SELECT 42 AS answer" {
			serverDone <- errUnexpected("unexpected query packet")
			return
		}
		packets := []packet{
			{sequence: 1, payload: []byte{1}},
			{sequence: 2, payload: columnDefinitionPayload("appdb", "answer")},
			{sequence: 3, payload: eofPayload()},
			{sequence: 4, payload: appendLenencString(nil, "42")},
			{sequence: 5, payload: eofPayload()},
		}
		for _, item := range packets {
			if err := writePacket(connection, item); err != nil {
				serverDone <- err
				return
			}
		}
		prepare, err := readPacket(reader)
		if err != nil || string(prepare.payload) != "\x16SELECT 42" {
			serverDone <- errUnexpected("unexpected prepare packet")
			return
		}
		prepareOK := []byte{0x00, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		if err := writePacket(connection, packet{sequence: 1, payload: prepareOK}); err != nil {
			serverDone <- err
			return
		}
		execute, err := readPacket(reader)
		if err != nil || len(execute.payload) < 5 || execute.payload[0] != comStmtExecute || execute.payload[1] != 42 {
			serverDone <- errUnexpected("unexpected execute packet")
			return
		}
		if err := writePacket(connection, packet{sequence: 1, payload: mysqlOKPayload()}); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	sink := testSink{events: make(chan observation.Interaction, 8)}
	upstream := config.Upstream{ID: "mysql", Name: "MySQL", Protocol: "mysql", ListenAddr: "127.0.0.1:0", Target: upstreamListener.Addr().String(), Enabled: true, MySQL: &config.MySQLOptions{
		ListenerUsername: "application", ListenerPassword: "listener-password", UpstreamUsername: "database-user", UpstreamPassword: "database-password", Database: "appdb",
	}}
	go func() { _ = New().Run(ctx, upstream, sink, func(address string) { ready <- address }) }()
	client, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reader := bufio.NewReader(client)
	greetingPacket, err := readPacket(reader)
	if err != nil {
		t.Fatal(err)
	}
	proxyGreeting, err := parseGreeting(greetingPacket.payload)
	if err != nil {
		t.Fatal(err)
	}
	if proxyGreeting.serverVersion != "5.6.51-portscope" || proxyGreeting.capabilities&(clientCompress|clientLocalFiles|clientQueryAttributes) != 0 {
		t.Fatalf("unsafe proxy greeting: %#v", proxyGreeting)
	}
	clientCapabilities := clientProtocol41 | clientSecureConnection | clientPluginAuth | clientConnectWithDB
	handshake, err := buildHandshakeResponse(clientCapabilities, "application", "listener-password", "ignored-client-db", "mysql_native_password", proxyGreeting.scramble, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writePacket(client, packet{sequence: 1, payload: handshake}); err != nil {
		t.Fatal(err)
	}
	authOK, err := readPacket(reader)
	if err != nil || len(authOK.payload) == 0 || authOK.payload[0] != 0x00 {
		t.Fatalf("auth OK = %#v, error = %v", authOK, err)
	}
	if err := writePacket(client, packet{sequence: 0, payload: []byte("\x03SELECT 42 AS answer")}); err != nil {
		t.Fatal(err)
	}
	for sequence := byte(1); sequence <= 5; sequence++ {
		item, readErr := readPacket(reader)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if item.sequence != sequence {
			t.Fatalf("response sequence = %d, want %d", item.sequence, sequence)
		}
	}
	if err := writePacket(client, packet{sequence: 0, payload: []byte("\x16SELECT 42")}); err != nil {
		t.Fatal(err)
	}
	prepared, err := readPacket(reader)
	if err != nil || len(prepared.payload) < 5 || prepared.payload[1] != 42 {
		t.Fatalf("prepare response = %#v, error = %v", prepared, err)
	}
	executePayload := []byte{comStmtExecute, 42, 0, 0, 0, 0, 1, 0, 0, 0}
	if err := writePacket(client, packet{sequence: 0, payload: executePayload}); err != nil {
		t.Fatal(err)
	}
	executed, err := readPacket(reader)
	if err != nil || len(executed.payload) == 0 || executed.payload[0] != 0x00 {
		t.Fatalf("execute response = %#v, error = %v", executed, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}

	var connect, query, prepare, execute observation.Interaction
	deadline := time.After(time.Second)
	for connect.ID == "" || query.ID == "" || prepare.ID == "" || execute.ID == "" {
		select {
		case item := <-sink.events:
			if item.Operation == "CONNECT" {
				connect = item
			} else if strings.HasPrefix(item.Operation, "QUERY") {
				query = item
			} else if strings.HasPrefix(item.Operation, "PREPARE") {
				prepare = item
			} else if strings.HasPrefix(item.Operation, "EXECUTE") {
				execute = item
			}
		case <-deadline:
			t.Fatalf("missing observations: connect=%#v query=%#v prepare=%#v execute=%#v", connect, query, prepare, execute)
		}
	}
	if connect.Attributes["auth"] != "terminated" || connect.Attributes["clientUser"] != "application" || connect.Attributes["upstreamUser"] != "database-user" {
		t.Fatalf("unexpected connect observation: %#v", connect)
	}
	if query.Request.Kind != "sql" || query.Request.Text != "SELECT 42 AS answer" || query.Response.Kind != "json" || query.Attributes["rows"] != "1" {
		t.Fatalf("unexpected query observation: %#v", query)
	}
	var result struct {
		Columns []string `json:"columns"`
		Rows    [][]any  `json:"rows"`
	}
	if err := json.Unmarshal(query.Response.JSON, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Columns) != 1 || result.Columns[0] != "answer" || len(result.Rows) != 1 || result.Rows[0][0] != "42" {
		t.Fatalf("decoded result = %#v", result)
	}
	if prepare.Response.Summary != "PREPARED · id 42 · 0 params · 0 columns" || execute.Operation != "EXECUTE SELECT" || execute.Attributes["statementId"] != "42" {
		t.Fatalf("prepared statement was not correlated: prepare=%#v execute=%#v", prepare, execute)
	}
}

func TestNativeAndCachingSHA2AuthenticationResponses(t *testing.T) {
	scramble := []byte("12345678901234567890")
	native, err := authenticationResponse("mysql_native_password", "secret", scramble)
	if err != nil || len(native) != 20 || !verifyNativePassword(native, "secret", scramble) || verifyNativePassword(native, "wrong", scramble) {
		t.Fatalf("native response invalid: %x, %v", native, err)
	}
	caching, err := authenticationResponse("caching_sha2_password", "secret", scramble)
	if err != nil || len(caching) != 32 || string(caching) == string(native) {
		t.Fatalf("caching SHA-2 response invalid: %x, %v", caching, err)
	}
	shaRequest, err := authenticationResponseForTransport("sha256_password", "secret", scramble, false)
	if err != nil || !bytes.Equal(shaRequest, []byte{0x01}) {
		t.Fatalf("SHA-256 RSA request invalid: %x, %v", shaRequest, err)
	}
	shaTLS, err := authenticationResponseForTransport("sha256_password", "secret", scramble, true)
	if err != nil || string(shaTLS) != "secret\x00" {
		t.Fatalf("SHA-256 TLS response invalid: %x, %v", shaTLS, err)
	}
}

func TestUpstreamCapabilitiesTerminateAuthenticationButMirrorWireSemantics(t *testing.T) {
	server := inspectableCapabilities | clientCompress | clientQueryAttributes
	legacyClient := clientProtocol41 | clientSecureConnection
	capabilities := upstreamCapabilities(server, legacyClient, true)
	if capabilities&clientPluginAuth == 0 || capabilities&clientPluginAuthLenencClientData == 0 {
		t.Fatalf("upstream plugin auth was constrained by the legacy client: %#x", capabilities)
	}
	if capabilities&clientDeprecateEOF != 0 || capabilities&clientSessionTrack != 0 {
		t.Fatalf("response-shaping capabilities were not mirrored: %#x", capabilities)
	}
	if capabilities&clientConnectWithDB == 0 {
		t.Fatalf("configured upstream database was not negotiated: %#x", capabilities)
	}
	if capabilities&(clientCompress|clientQueryAttributes) != 0 {
		t.Fatalf("unsupported capabilities escaped the inspection boundary: %#x", capabilities)
	}
}

func TestProxyGreetingPreservesUpstreamCompatibilityVersion(t *testing.T) {
	for _, test := range []struct {
		upstream string
		want     string
	}{
		{upstream: "5.6.51", want: "5.6.51-portscope"},
		{upstream: "5.7.44-log", want: "5.7.44-log-portscope"},
		{upstream: "8.0.45", want: "8.0.45-portscope"},
		{upstream: "8.4.12", want: "8.4.12-portscope"},
		{upstream: "9.7.1", want: "9.7.1-portscope"},
		{upstream: "", want: "5.6.0-portscope"},
	} {
		if got := proxyServerVersion(test.upstream); got != test.want {
			t.Errorf("proxyServerVersion(%q) = %q, want %q", test.upstream, got, test.want)
		}
	}
}

func TestSHA256PasswordRSAAuthenticationAgainstLegacyServer(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicKey})
	scramble := []byte("legacy-sha256-seed!!")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		greetingPayload := makeGreetingForPlugin(56, scramble, inspectableCapabilities, false, "5.6.51", "sha256_password")
		if err := writePacket(connection, packet{sequence: 0, payload: greetingPayload}); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(connection)
		handshakePacket, err := readPacket(reader)
		if err != nil {
			serverDone <- err
			return
		}
		handshake, err := parseClientHandshake(handshakePacket.payload)
		if err != nil || handshake.plugin != "sha256_password" || !bytes.Equal(handshake.authResponse, []byte{0x01}) {
			serverDone <- errUnexpected("proxy did not request the legacy SHA-256 RSA key")
			return
		}
		if err := writePacket(connection, packet{sequence: 2, payload: append([]byte{0x01}, encodedKey...)}); err != nil {
			serverDone <- err
			return
		}
		encrypted, err := readPacket(reader)
		if err != nil {
			serverDone <- err
			return
		}
		plain, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, privateKey, encrypted.payload, nil)
		if err != nil {
			serverDone <- err
			return
		}
		for index := range plain {
			plain[index] ^= scramble[index%len(scramble)]
		}
		if string(plain) != "database-password\x00" {
			serverDone <- errUnexpected("proxy encrypted the wrong upstream password")
			return
		}
		serverDone <- writePacket(connection, packet{sequence: 4, payload: mysqlOKPayload()})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := config.Upstream{Target: listener.Addr().String(), MySQL: &config.MySQLOptions{UpstreamUsername: "database-user", UpstreamPassword: "database-password"}}
	session, err := openUpstream(ctx, upstream)
	if err != nil {
		t.Fatal(err)
	}
	defer session.connection.Close()
	// Model a connector old enough not to advertise pluggable auth. The proxy's
	// independently authenticated upstream leg must still negotiate it.
	if err := authenticateUpstream(ctx, upstream, session, clientProtocol41|clientSecureConnection); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestUpstreamTLSUpgradeVerifiesCertificate(t *testing.T) {
	serverTLS, caPath, _, _ := mysqlTestTLSCertificate(t)
	scramble := []byte("tls-upstream-seed!!!")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		if err := writePacket(connection, packet{sequence: 0, payload: makeGreeting(84, scramble, inspectableCapabilities, true, "8.4.11")}); err != nil {
			serverDone <- err
			return
		}
		reader := bufio.NewReader(connection)
		request, err := readPacket(reader)
		if err != nil || request.sequence != 1 || !isSSLRequest(request.payload) {
			serverDone <- errUnexpected("proxy did not send a valid MySQL SSLRequest")
			return
		}
		secure := tls.Server(connection, serverTLS)
		if err := secure.Handshake(); err != nil {
			serverDone <- err
			return
		}
		handshakePacket, err := readPacket(bufio.NewReader(secure))
		if err != nil {
			serverDone <- err
			return
		}
		handshake, err := parseClientHandshake(handshakePacket.payload)
		if err != nil || handshakePacket.sequence != 2 || !verifyNativePassword(handshake.authResponse, "database-password", scramble) {
			serverDone <- errUnexpected("invalid authenticated handshake inside MySQL TLS")
			return
		}
		serverDone <- writePacket(secure, packet{sequence: 3, payload: mysqlOKPayload()})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := config.Upstream{Target: listener.Addr().String(), MySQL: &config.MySQLOptions{
		UpstreamUsername: "database-user", UpstreamPassword: "database-password",
		UpstreamTLS: config.ClientTLSOptions{Enabled: true, CAFile: caPath},
	}}
	session, err := openUpstream(ctx, upstream)
	if err != nil {
		t.Fatal(err)
	}
	defer session.connection.Close()
	if err := authenticateUpstream(ctx, upstream, session, inspectableCapabilities); err != nil {
		t.Fatal(err)
	}
	if !session.tls {
		t.Fatal("authenticated upstream session did not record TLS")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestListenerTLSUpgradeAuthenticatesApplication(t *testing.T) {
	_, caPath, certPath, keyPath := mysqlTestTLSCertificate(t)
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamListener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := upstreamListener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		scramble := []byte("listener-tls-seed!!!")
		if err := writePacket(connection, packet{sequence: 0, payload: makeGreeting(97, scramble, inspectableCapabilities, false, "9.7.2")}); err != nil {
			serverDone <- err
			return
		}
		handshakePacket, err := readPacket(bufio.NewReader(connection))
		if err != nil {
			serverDone <- err
			return
		}
		handshake, err := parseClientHandshake(handshakePacket.payload)
		if err != nil || !verifyNativePassword(handshake.authResponse, "upstream-secret", scramble) {
			serverDone <- errUnexpected("proxy sent invalid independent upstream credentials")
			return
		}
		serverDone <- writePacket(connection, packet{sequence: 2, payload: mysqlOKPayload()})
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 4)
	configured := config.Upstream{
		ID: "mysql-listener-tls", Name: "MySQL listener TLS", Protocol: "mysql", Enabled: true,
		ListenAddr: "127.0.0.1:0", Target: upstreamListener.Addr().String(),
		ListenerTLS: &config.ListenerTLSOptions{Enabled: true, CertFile: certPath, KeyFile: keyPath},
		MySQL:       &config.MySQLOptions{ListenerUsername: "application", ListenerPassword: "listener-secret", UpstreamUsername: "database", UpstreamPassword: "upstream-secret"},
	}
	go func() {
		_ = New().Run(ctx, configured, testSink{events: events}, func(address string) { ready <- address })
	}()
	raw, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	// Real clients can put the SSLRequest and the first TLS record in one TCP
	// read. Force that shape so the proxy must preserve bytes prefetched while
	// parsing the plaintext packet before it wraps the connection in TLS.
	plain := &coalescingWriteConn{Conn: raw, remaining: 3}
	defer plain.Close()
	reader := bufio.NewReader(plain)
	greetingPacket, err := readPacket(reader)
	if err != nil {
		t.Fatal(err)
	}
	greeting, err := parseGreeting(greetingPacket.payload)
	if err != nil || greeting.capabilities&clientSSL == 0 {
		t.Fatalf("listener greeting does not advertise TLS: %#v, %v", greeting, err)
	}
	capabilities := clientProtocol41 | clientSecureConnection | clientPluginAuth | clientSSL
	sslRequest := make([]byte, 32)
	binary.LittleEndian.PutUint32(sslRequest, capabilities)
	binary.LittleEndian.PutUint32(sslRequest[4:], 64<<20)
	sslRequest[8] = 45
	if err := writePacket(plain, packet{sequence: 1, payload: sslRequest}); err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("could not load MySQL listener test CA")
	}
	secure := tls.Client(plain, &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"})
	if err := secure.HandshakeContext(ctx); err != nil {
		t.Fatal(err)
	}
	response, err := buildHandshakeResponse(capabilities, "application", "listener-secret", "", "mysql_native_password", greeting.scramble, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := writePacket(secure, packet{sequence: 2, payload: response}); err != nil {
		t.Fatal(err)
	}
	authOK, err := readPacket(bufio.NewReader(secure))
	if err != nil || authOK.sequence != 3 || len(authOK.payload) == 0 || authOK.payload[0] != 0x00 {
		t.Fatalf("listener TLS authentication = %#v, %v", authOK, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Operation != "CONNECT" || event.Attributes["downstreamTLS"] != "true" {
			t.Fatalf("listener TLS observation = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing listener TLS connection observation")
	}
}

type coalescingWriteConn struct {
	net.Conn
	mu        sync.Mutex
	remaining int
	buffer    []byte
}

func (connection *coalescingWriteConn) Write(data []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.remaining == 0 {
		return connection.Conn.Write(data)
	}
	connection.buffer = append(connection.buffer, data...)
	connection.remaining--
	if connection.remaining > 0 {
		return len(data), nil
	}
	buffer := connection.buffer
	connection.buffer = nil
	for len(buffer) > 0 {
		written, err := connection.Conn.Write(buffer)
		if err != nil {
			return 0, err
		}
		buffer = buffer[written:]
	}
	return len(data), nil
}

func mysqlTestTLSCertificate(t *testing.T) (*tls.Config, string, string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Portscope MySQL test"},
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
	directory := t.TempDir()
	caPath := filepath.Join(directory, "mysql-ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(directory, "mysql-cert.pem")
	keyPath := filepath.Join(directory, "mysql-key.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}, caPath, certPath, keyPath
}

func TestPreparedStatementParametersAreDecodedAndTypesAreReused(t *testing.T) {
	statement := &preparedStatement{query: "SELECT ?, ?, ?, ?, ?", parameters: 5}
	statements := map[uint32]*preparedStatement{7: statement}
	payload := []byte{comStmtExecute, 7, 0, 0, 0, 0, 1, 0, 0, 0, 0x10, 1}
	payload = append(payload,
		mysqlTypeLong, 0,
		mysqlTypeVarString, 0,
		mysqlTypeDouble, 0,
		mysqlTypeDateTime, 0,
		mysqlTypeNull, 0,
	)
	var integer [4]byte
	signedInteger := int32(-7)
	binary.LittleEndian.PutUint32(integer[:], uint32(signedInteger))
	payload = append(payload, integer[:]...)
	payload = appendLenencString(payload, "hello")
	var floating [8]byte
	binary.LittleEndian.PutUint64(floating[:], math.Float64bits(10.5))
	payload = append(payload, floating[:]...)
	payload = append(payload, 7, 0xea, 0x07, 8, 30, 14, 15, 16)

	command := parseCommand(logicalPacket{payload: payload, size: int64(len(payload))}, statements)
	if command.request.Kind != "json" || command.operation != "EXECUTE SELECT" {
		t.Fatalf("execute was not decoded: %#v", command)
	}
	var capture struct {
		StatementID uint32 `json:"statementId"`
		Query       string `json:"query"`
		Parameters  []any  `json:"parameters"`
	}
	if err := json.Unmarshal(command.request.JSON, &capture); err != nil {
		t.Fatal(err)
	}
	if capture.StatementID != 7 || capture.Query != statement.query || len(capture.Parameters) != 5 || capture.Parameters[0] != float64(-7) || capture.Parameters[1] != "hello" || capture.Parameters[2] != 10.5 || capture.Parameters[3] != "2026-08-30 14:15:16" || capture.Parameters[4] != nil {
		t.Fatalf("decoded parameters = %#v", capture)
	}

	reused := append([]byte{comStmtExecute, 7, 0, 0, 0, 0, 1, 0, 0, 0, 0x10, 0}, integer[:]...)
	reused = appendLenencString(reused, "again")
	reused = append(reused, floating[:]...)
	reused = append(reused, 7, 0xea, 0x07, 8, 30, 14, 15, 16)
	command = parseCommand(logicalPacket{payload: reused, size: int64(len(reused))}, statements)
	if command.request.Kind != "json" || strings.Contains(command.request.Text, "unavailable") {
		t.Fatalf("cached parameter types were not reused: %#v", command.request)
	}
}

func TestPreparedLongDataIsCountedWithoutCapturingContent(t *testing.T) {
	statement := &preparedStatement{query: "INSERT INTO files VALUES (?)", parameters: 1, parameterTypes: []binaryType{{code: mysqlTypeBlob}}}
	statements := map[uint32]*preparedStatement{9: statement}
	secret := "binary-secret-that-must-not-be-captured"
	longData := []byte{comStmtLongData, 9, 0, 0, 0, 0, 0}
	longData = append(longData, secret...)
	command := parseCommand(logicalPacket{payload: longData, size: int64(len(longData))}, statements)
	if strings.Contains(command.request.Text, secret) || !strings.Contains(command.request.Summary, "39 bytes") {
		t.Fatalf("long data capture was unsafe: %#v", command.request)
	}
	execute := []byte{comStmtExecute, 9, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0}
	command = parseCommand(logicalPacket{payload: execute, size: int64(len(execute))}, statements)
	if !strings.Contains(string(command.request.JSON), `"longDataBytes":39`) || strings.Contains(string(command.request.JSON), secret) {
		t.Fatalf("long data execute capture = %s", command.request.JSON)
	}
}

func TestBinaryResultRowIsDecodedFromColumnMetadata(t *testing.T) {
	columns := []columnDefinition{
		{name: "id", typeInfo: binaryType{code: mysqlTypeLongLong, unsigned: true}},
		{name: "name", typeInfo: binaryType{code: mysqlTypeVarString}},
		{name: "score", typeInfo: binaryType{code: mysqlTypeDouble}},
		{name: "created", typeInfo: binaryType{code: mysqlTypeDateTime}},
	}
	row := []byte{0x00, 0x10}
	var id [8]byte
	binary.LittleEndian.PutUint64(id[:], 42)
	row = append(row, id[:]...)
	row = appendLenencString(row, "Ada")
	row = append(row, 7, 0xea, 0x07, 8, 30, 9, 5, 4)
	values, err := decodeBinaryRow(row, columns)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 4 || values[0] != uint64(42) || values[1] != "Ada" || values[2] != nil || values[3] != "2026-08-30 09:05:04" {
		t.Fatalf("decoded row = %#v", values)
	}
}

func TestLargeBinaryValuesUseBoundedPreviews(t *testing.T) {
	value := capturedBinaryValue([]byte(strings.Repeat("x", binaryValuePreviewLimit+100)))
	metadata, ok := value.(map[string]any)
	if !ok || metadata["size"] != binaryValuePreviewLimit+100 || metadata["truncated"] != true || len(metadata["preview"].(string)) != binaryValuePreviewLimit {
		t.Fatalf("large value was not bounded: %#v", value)
	}
	if !hasTruncatedBinaryValue([]any{value}) {
		t.Fatal("truncated binary value was not propagated to the observation")
	}
}

func TestWrongListenerCredentialsNeverReachUpstreamAuthentication(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamListener.Close()
	receivedHandshake := make(chan bool, 1)
	go func() {
		connection, acceptErr := upstreamListener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_ = writePacket(connection, packet{sequence: 0, payload: makeGreeting(1, []byte("12345678901234567890"), inspectableCapabilities, false, "9.7.1")})
		_ = connection.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, readErr := readPacket(bufio.NewReader(connection))
		receivedHandshake <- readErr == nil
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	upstream := config.Upstream{ID: "mysql", Name: "MySQL", Protocol: "mysql", ListenAddr: "127.0.0.1:0", Target: upstreamListener.Addr().String(), Enabled: true, MySQL: &config.MySQLOptions{ListenerUsername: "app", ListenerPassword: "right", UpstreamUsername: "root"}}
	go func() {
		_ = New().Run(ctx, upstream, testSink{events: make(chan observation.Interaction, 4)}, func(address string) { ready <- address })
	}()
	client, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reader := bufio.NewReader(client)
	greetingPacket, _ := readPacket(reader)
	greeting, _ := parseGreeting(greetingPacket.payload)
	response, _ := buildHandshakeResponse(clientProtocol41|clientSecureConnection|clientPluginAuth, "app", "wrong", "", "mysql_native_password", greeting.scramble, false)
	_ = writePacket(client, packet{sequence: 1, payload: response})
	denied, err := readPacket(reader)
	if err != nil || len(denied.payload) == 0 || denied.payload[0] != 0xff {
		t.Fatalf("denied packet = %#v, error = %v", denied, err)
	}
	if <-receivedHandshake {
		t.Fatal("Portscope sent upstream credentials before validating its listener credentials")
	}
}

type errUnexpected string

func (err errUnexpected) Error() string { return string(err) }

func appendLenencString(target []byte, value string) []byte {
	target = appendLenenc(target, uint64(len(value)))
	return append(target, value...)
}

func columnDefinitionPayload(schema, name string) []byte {
	payload := make([]byte, 0, 64)
	for _, value := range []string{"def", schema, "", "", name, name} {
		payload = appendLenencString(payload, value)
	}
	payload = append(payload, 0x0c, 45, 0, 32, 0, 0, 0, 0xfd, 0, 0, 0x1f, 0, 0)
	return payload
}

func eofPayload() []byte {
	return []byte{0xfe, 0, 0, byte(serverStatusAutocommit), byte(serverStatusAutocommit >> 8)}
}
