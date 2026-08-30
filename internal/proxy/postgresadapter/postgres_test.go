package postgresadapter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type collectingSink struct{ events chan observation.Interaction }

func (sink collectingSink) Record(item observation.Interaction) { sink.events <- item }

func TestListenerAndClientSCRAMRoundTrip(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- authenticateListener(bufio.NewReader(server), server, "app,user", "correct horse battery staple")
	}()
	reader := bufio.NewReader(client)
	offer, err := readMessage(reader)
	if err != nil || offer.typ != 'R' || !bytes.Contains(offer.body, []byte("SCRAM-SHA-256")) {
		t.Fatalf("SCRAM offer = %#v, %v", offer, err)
	}
	scram, err := newSCRAMClient("app,user", "correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	initial := append([]byte("SCRAM-SHA-256\x00"), appendInt32(nil, int32(len(scram.clientFirst)))...)
	initial = append(initial, scram.clientFirst...)
	if err := writeMessage(client, message{typ: 'p', body: initial}); err != nil {
		t.Fatal(err)
	}
	continuation, err := readMessage(reader)
	if err != nil {
		t.Fatal(err)
	}
	response, err := scram.continueWith(string(continuation.body[4:]))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeMessage(client, message{typ: 'p', body: []byte(response)}); err != nil {
		t.Fatal(err)
	}
	final, err := readMessage(reader)
	if err != nil || !scram.verify(string(final.body[4:])) {
		t.Fatalf("SCRAM final verification failed: %#v, %v", final, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestWrongSCRAMPasswordIsRejected(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	serverDone := make(chan error, 1)
	go func() { serverDone <- authenticateListener(bufio.NewReader(server), server, "app", "right") }()
	reader := bufio.NewReader(client)
	_, _ = readMessage(reader)
	scram, _ := newSCRAMClient("app", "wrong")
	initial := append([]byte("SCRAM-SHA-256\x00"), appendInt32(nil, int32(len(scram.clientFirst)))...)
	_ = writeMessage(client, message{typ: 'p', body: append(initial, scram.clientFirst...)})
	continuation, _ := readMessage(reader)
	response, _ := scram.continueWith(string(continuation.body[4:]))
	_ = writeMessage(client, message{typ: 'p', body: []byte(response)})
	select {
	case err := <-serverDone:
		if err == nil {
			t.Fatal("wrong SCRAM password was accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("listener SCRAM verification hung")
	}
}

func TestProtocolFramingAndStartupBounds(t *testing.T) {
	var wire bytes.Buffer
	if err := writeMessage(&wire, message{typ: 'Q', body: []byte("SELECT 1\x00")}); err != nil {
		t.Fatal(err)
	}
	decoded, err := readMessage(bufio.NewReader(&wire))
	if err != nil || decoded.typ != 'Q' || string(decoded.body) != "SELECT 1\x00" {
		t.Fatalf("decoded = %#v, %v", decoded, err)
	}
	parameters := map[string]string{"user": "client", "database": "ignored", "application_name": "matrix", "replication": "database"}
	payload := buildStartup(parameters, "upstream", "appdb")
	parsed, err := parseStartup(payload)
	if err != nil || parsed["user"] != "upstream" || parsed["database"] != "appdb" || parsed["application_name"] != "matrix" || parsed["replication"] != "" {
		t.Fatalf("sanitized startup = %#v, %v", parsed, err)
	}
}

func TestProtocolMinorVersionAndUnknownOptionsAreNegotiated(t *testing.T) {
	payload := appendInt32(nil, protocolVersion30+2)
	payload = append(payload, "user\x00app\x00_pq_.feature\x00on\x00\x00"...)
	parameters, err := parseStartup(payload)
	if err != nil {
		t.Fatal(err)
	}
	item, needed := negotiateProtocolVersion(payload, parameters)
	if !needed || item.typ != 'v' || !bytes.Contains(item.body, []byte("_pq_.feature\x00")) {
		t.Fatalf("negotiation = %#v, needed=%v", item, needed)
	}
	if forwarded := buildStartup(parameters, "upstream", "database"); bytes.Contains(forwarded, []byte("_pq_.feature")) {
		t.Fatal("unsupported startup option was forwarded upstream")
	}
}

func TestExtendedParseErrorIsCorrelatedAtReadyForQuery(t *testing.T) {
	events := make(chan observation.Interaction, 1)
	state := newTracker(config.Upstream{ID: "postgres"}, collectingSink{events: events}, "connection", false, false)
	parse := append([]byte("statement\x00SELECT broken\x00"), 0, 0)
	state.frontend(message{typ: 'P', body: parse})
	state.frontend(message{typ: 'B', body: []byte("\x00statement\x00\x00\x00\x00\x00\x00")})
	errorBody := errorResponse("ERROR", "42601", "syntax error").body
	state.backend(message{typ: 'E', body: errorBody})
	state.frontend(message{typ: 'E', body: []byte("\x00\x00\x00\x00\x00")})
	state.backend(message{typ: 'Z', body: []byte{'I'}})
	select {
	case event := <-events:
		if event.Outcome != "error" || event.Attributes["sqlstate"] != "42601" || event.Operation != "QUERY SELECT" {
			t.Fatalf("parse error observation = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("extended parse error was not recorded")
	}
}

func TestPreparedParametersAndCopyBytesAreBoundedAndAccounted(t *testing.T) {
	events := make(chan observation.Interaction, 1)
	state := newTracker(config.Upstream{ID: "postgres"}, collectingSink{events: events}, "connection", false, false)
	parse := append([]byte("statement\x00COPY things FROM STDIN\x00"), 0, 0)
	state.frontend(message{typ: 'P', body: parse})
	bind := []byte("portal\x00statement\x00\x00\x00\x00\x00\x00")
	state.frontend(message{typ: 'B', body: bind})
	state.frontend(message{typ: 'E', body: []byte("portal\x00\x00\x00\x00\x00")})
	state.frontend(message{typ: 'd', body: bytes.Repeat([]byte{'x'}, 1024)})
	state.backend(message{typ: 'C', body: []byte("COPY 1\x00")})
	event := <-events
	if event.Request.Size < 1024 {
		t.Fatalf("COPY request size = %d", event.Request.Size)
	}
	if event.Request.Kind == "json" {
		var decoded any
		if err := json.Unmarshal(event.Request.JSON, &decoded); err != nil {
			t.Fatalf("invalid parameter capture: %v", err)
		}
	}
}

func TestMD5PasswordResponse(t *testing.T) {
	if got := postgresMD5Password("user", "password", []byte{1, 2, 3, 4}); got != "md5a3576f1ae039b8996bc4fc2720f9c71a" {
		t.Fatalf("MD5 response = %q", got)
	}
}

func TestCancelKeyIsRandomAndNonzero(t *testing.T) {
	first, err := randomCancelKey()
	if err != nil {
		t.Fatal(err)
	}
	second, _ := randomCancelKey()
	if first.pid == 0 || first.secret == 0 || first == second {
		t.Fatalf("unsafe cancel keys: %#v %#v", first, second)
	}
}

func TestAcceptStartupRejectsReplication(t *testing.T) {
	// The policy itself lives in the adapter; ensure the sanitized upstream
	// startup builder can never opt into replication mode.
	payload := buildStartup(map[string]string{"replication": "database"}, "user", "db")
	parsed, err := parseStartup(payload)
	if err != nil || parsed["replication"] != "" {
		t.Fatalf("replication leaked into upstream startup: %#v, %v", parsed, err)
	}
}
