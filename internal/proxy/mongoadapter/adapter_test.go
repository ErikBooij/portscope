package mongoadapter

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type testSink struct{ events chan observation.Interaction }

func (sink testSink) Record(item observation.Interaction) { sink.events <- item }

func TestProxyTerminatesSCRAMAndInspectsCommands(t *testing.T) {
	upstreamAddress, _, stopUpstream := startFakeMongo(t)
	defer stopUpstream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 100)
	upstream := config.Upstream{
		ID: "mongo", Name: "Documents", Protocol: "mongodb", ListenAddr: "127.0.0.1:0", Target: upstreamAddress, Enabled: true,
		MongoDB: &config.MongoDBOptions{ListenerUsername: "portscope", ListenerPassword: "listener-secret", ListenerAuthSource: "admin"},
	}
	go func() {
		_ = New().Run(ctx, upstream, testSink{events: events}, func(address string) { ready <- address })
	}()
	address := <-ready

	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + address + "/?directConnection=true").SetAuth(options.Credential{Username: "portscope", Password: "listener-secret", AuthSource: "admin"}).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Database("library").Collection("books").InsertOne(ctx, bson.D{{Key: "title", Value: "The Dispossessed"}}); err != nil {
		t.Fatal(err)
	}
	var found bson.M
	if err := client.Database("library").Collection("books").FindOne(ctx, bson.D{{Key: "title", Value: "The Dispossessed"}}).Decode(&found); err != nil {
		t.Fatal(err)
	}

	observed := waitForOperations(t, events, "INSERT library.books", "FIND library.books", "AUTH")
	insert := observed["INSERT library.books"]
	if insert.Protocol != "mongodb" || insert.Request.Kind != "json" || insert.Attributes["database"] != "library" || insert.Attributes["collection"] != "books" || insert.Attributes["n"] != "1" {
		t.Fatalf("insert observation = %#v", insert)
	}
	find := observed["FIND library.books"]
	if find.Response.Summary != "1 documents" || find.Attributes["documents"] != "1" || find.Outcome != "ok" {
		t.Fatalf("find observation = %#v", find)
	}
	auth := observed["AUTH"]
	if strings.Contains(string(auth.Request.JSON), "listener-secret") || !strings.Contains(string(auth.Request.JSON), "redacted") {
		t.Fatalf("authentication capture was not redacted: %s", auth.Request.JSON)
	}
}

func TestWrongListenerCredentialsNeverTriggerUpstreamSCRAM(t *testing.T) {
	upstreamAddress, upstreamAuthStarts, stopUpstream := startFakeMongo(t)
	defer stopUpstream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	upstream := config.Upstream{
		ID: "mongo", Name: "Documents", Protocol: "mongodb", ListenAddr: "127.0.0.1:0", Target: upstreamAddress, Enabled: true,
		MongoDB: &config.MongoDBOptions{ListenerUsername: "portscope", ListenerPassword: "right-password", UpstreamUsername: "database-user", UpstreamPassword: "database-secret"},
	}
	go func() {
		_ = New().Run(ctx, upstream, testSink{events: make(chan observation.Interaction, 100)}, func(address string) { ready <- address })
	}()
	address := <-ready
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + address + "/?directConnection=true").SetAuth(options.Credential{Username: "portscope", Password: "wrong-password", AuthSource: "admin"}).SetServerSelectionTimeout(750 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, readpref.Primary()); err == nil {
		t.Fatal("wrong MongoDB listener password was accepted")
	}
	if upstreamAuthStarts.Load() != 0 {
		t.Fatal("upstream SCRAM started before listener authentication succeeded")
	}
}

func TestSCRAMRejectsWrongPassword(t *testing.T) {
	server, first, err := newSCRAMServer("SCRAM-SHA-256", "portscope", "right-password", []byte("n,,n=portscope,r=client-nonce"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := newSCRAMClient("SCRAM-SHA-256", "portscope", "wrong-password")
	if err != nil {
		t.Fatal(err)
	}
	client.clientFirstBare = "n=portscope,r=client-nonce"
	client.nonce = "client-nonce"
	final, err := client.final(string(first))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.finish(final); err == nil {
		t.Fatal("wrong SCRAM password was accepted")
	}
}

func TestSanitizedHelloRemovesTopologyAndCompression(t *testing.T) {
	hello := mustDocument(bson.D{{Key: "isWritablePrimary", Value: true}, {Key: "setName", Value: "rs0"}, {Key: "hosts", Value: bson.A{"db1:27017", "db2:27017"}}, {Key: "compression", Value: bson.A{"zstd"}}, {Key: "serviceId", Value: bson.ObjectID{}}, {Key: "maxWireVersion", Value: 25}, {Key: "ok", Value: 1}})
	result, err := sanitizedHello(hello, &config.MongoDBOptions{ListenerUsername: "portscope"})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"setName", "hosts", "compression", "serviceId"} {
		if result.Lookup(field).Type != 0 {
			t.Fatalf("sanitized hello retained %s", field)
		}
	}
	if _, ok := result.Lookup("saslSupportedMechs").ArrayOK(); !ok {
		t.Fatal("sanitized hello did not advertise listener SCRAM")
	}
}

func TestSpeculativeAuthenticationCaptureIsRedacted(t *testing.T) {
	document := mustDocument(bson.D{{Key: "hello", Value: 1}, {Key: "speculativeAuthenticate", Value: bson.D{{Key: "saslStart", Value: 1}, {Key: "payload", Value: bson.Binary{Data: []byte("client-proof-material")}}}}})
	payload := mongoPayload(document, nil, int64(len(document)), "", false)
	if strings.Contains(string(payload.JSON), "client-proof-material") || !strings.Contains(string(payload.JSON), "redacted") {
		t.Fatalf("speculative authentication was not redacted: %s", payload.JSON)
	}
}

func waitForOperations(t *testing.T, events <-chan observation.Interaction, operations ...string) map[string]observation.Interaction {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	wanted := make(map[string]struct{}, len(operations))
	for _, operation := range operations {
		wanted[operation] = struct{}{}
	}
	result := make(map[string]observation.Interaction, len(operations))
	for len(result) < len(wanted) {
		select {
		case event := <-events:
			if _, found := wanted[event.Operation]; found {
				result[event.Operation] = event
			}
		case <-timer.C:
			t.Fatalf("MongoDB operations %v were not all observed; got %v", operations, result)
		}
	}
	return result
}

func startFakeMongo(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var connections sync.WaitGroup
	var responseID atomic.Int32
	var authStarts atomic.Int32
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer connection.Close()
				reader := bufio.NewReader(connection)
				for {
					message, err := readWireMessage(reader)
					if err != nil {
						return
					}
					command, err := parseCommand(message)
					if err != nil {
						return
					}
					var response bson.Raw
					switch strings.ToLower(command.name) {
					case "hello", "ismaster":
						response = mustDocument(bson.D{{Key: "isWritablePrimary", Value: true}, {Key: "ismaster", Value: true}, {Key: "minWireVersion", Value: 0}, {Key: "maxWireVersion", Value: 25}, {Key: "maxBsonObjectSize", Value: 16 * 1024 * 1024}, {Key: "maxMessageSizeBytes", Value: 48 * 1024 * 1024}, {Key: "maxWriteBatchSize", Value: 100000}, {Key: "logicalSessionTimeoutMinutes", Value: 30}, {Key: "connectionId", Value: responseID.Add(1)}, {Key: "ok", Value: 1}})
					case "insert":
						response = mustDocument(bson.D{{Key: "n", Value: 1}, {Key: "ok", Value: 1}})
					case "find":
						response = mustDocument(bson.D{{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "library.books"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "title", Value: "The Dispossessed"}}}}}}, {Key: "ok", Value: 1}})
					case "saslstart":
						authStarts.Add(1)
						response = mongoErrorDocument(18, "AuthenticationFailed", "unexpected upstream authentication")
					default:
						response = mustDocument(bson.D{{Key: "ok", Value: 1}})
					}
					reply := makeCommandReply(message, responseID.Add(1), response)
					if _, err := connection.Write(reply.raw); err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String(), &authStarts, func() {
		cancel()
		_ = listener.Close()
		connections.Wait()
		_ = ctx
	}
}
