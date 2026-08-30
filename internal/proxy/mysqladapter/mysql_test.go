package mysqladapter

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strings"
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
		if err := writePacket(connection, packet{sequence: 0, payload: makeGreeting(77, serverScramble, capabilities, false)}); err != nil {
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
			{sequence: 2, payload: columnDefinition("appdb", "answer")},
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
	if proxyGreeting.serverVersion != "8.0.0-portscope" || proxyGreeting.capabilities&(clientCompress|clientLocalFiles|clientQueryAttributes) != 0 {
		t.Fatalf("unsafe proxy greeting: %#v", proxyGreeting)
	}
	clientCapabilities := clientProtocol41 | clientSecureConnection | clientPluginAuth | clientConnectWithDB
	handshake, err := buildHandshakeResponse(clientCapabilities, "application", "listener-password", "ignored-client-db", "mysql_native_password", proxyGreeting.scramble)
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
		_ = writePacket(connection, packet{sequence: 0, payload: makeGreeting(1, []byte("12345678901234567890"), inspectableCapabilities, false)})
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
	response, _ := buildHandshakeResponse(clientProtocol41|clientSecureConnection|clientPluginAuth, "app", "wrong", "", "mysql_native_password", greeting.scramble)
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

func columnDefinition(schema, name string) []byte {
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
