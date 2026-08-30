package mongoadapter

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

var responseRequestID atomic.Int32

func (adapter *Adapter) Run(ctx context.Context, upstream config.Upstream, sink observation.Sink, ready func(string)) error {
	listener, err := net.Listen("tcp", upstream.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", upstream.ListenAddr, err)
	}
	defer listener.Close()
	serveListener := listener
	if upstream.ListenerTLS != nil && upstream.ListenerTLS.Enabled {
		tlsConfig, tlsErr := tlsutil.Server(*upstream.ListenerTLS)
		if tlsErr != nil {
			return tlsErr
		}
		serveListener = tls.NewListener(listener, tlsConfig)
	}
	ready(listener.Addr().String())
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	var connections sync.WaitGroup
	defer connections.Wait()
	for {
		connection, acceptErr := serveListener.Accept()
		if acceptErr != nil {
			if ctx.Err() != nil || errors.Is(acceptErr, net.ErrClosed) {
				return ctx.Err()
			}
			return acceptErr
		}
		connections.Add(1)
		go func(connection net.Conn) {
			defer connections.Done()
			_ = adapter.handleConnection(ctx, upstream, sink, connection)
		}(connection)
	}
}

type lockedWriter struct {
	mu         sync.Mutex
	connection net.Conn
}

func (writer *lockedWriter) write(raw []byte) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	_, err := writer.connection.Write(raw)
	return err
}

type pendingSet struct {
	mu    sync.Mutex
	items map[int32]pendingRequest
}

func (set *pendingSet) put(request pendingRequest) {
	set.mu.Lock()
	set.items[request.wire.requestID] = request
	set.mu.Unlock()
}

func (set *pendingSet) get(id int32, keep bool) (pendingRequest, bool) {
	set.mu.Lock()
	defer set.mu.Unlock()
	request, found := set.items[id]
	if found && !keep {
		delete(set.items, id)
	}
	return request, found
}

func (set *pendingSet) drain() []pendingRequest {
	set.mu.Lock()
	defer set.mu.Unlock()
	result := make([]pendingRequest, 0, len(set.items))
	for id, request := range set.items {
		result = append(result, request)
		delete(set.items, id)
	}
	return result
}

func (adapter *Adapter) handleConnection(ctx context.Context, upstream config.Upstream, sink observation.Sink, downstream net.Conn) error {
	defer downstream.Close()
	connectContext, cancel := context.WithTimeout(ctx, 15*time.Second)
	session, err := openUpstream(connectContext, upstream)
	cancel()
	if err != nil {
		return err
	}
	defer session.connection.Close()
	downstreamWriter := &lockedWriter{connection: downstream}
	pending := &pendingSet{items: make(map[int32]pendingRequest)}
	authenticator := newLocalAuthenticator(upstream.MongoDB)
	errorsChannel := make(chan error, 2)
	connectionName := downstream.RemoteAddr().String()
	connectionContext := map[string]string{"downstreamTLS": fmt.Sprintf("%t", isTLSConnection(downstream)), "upstreamTLS": fmt.Sprintf("%t", session.tls)}
	if version, ok := session.hello.Lookup("version").StringValueOK(); ok {
		connectionContext["serverVersion"] = version
	}
	if wireVersion, ok := rawNumber(session.hello.Lookup("maxWireVersion")); ok {
		connectionContext["maxWireVersion"] = fmt.Sprintf("%d", int64(wireVersion))
	}
	var upstreamReaderOnce sync.Once
	startUpstreamReader := func() {
		upstreamReaderOnce.Do(func() {
			go func() {
				for {
					response, readErr := readWireMessage(session.reader)
					if readErr != nil {
						errorsChannel <- readErr
						return
					}
					parsed, parseErr := parseCommand(response)
					keep := parseErr == nil && parsed.flags&2 != 0
					request, found := pending.get(response.responseTo, keep)
					if writeErr := downstreamWriter.write(response.raw); writeErr != nil {
						errorsChannel <- writeErr
						return
					}
					if found {
						document, documentErr := responseDocument(response)
						if documentErr != nil {
							recordMongo(sink, upstream, connectionName, request, response, nil, documentErr)
						} else {
							recordMongo(sink, upstream, connectionName, request, response, document, nil)
						}
					}
				}
			}()
		})
	}

	go func() {
		reader := bufio.NewReader(downstream)
		upstreamAuthenticated := false
		for {
			request, readErr := readWireMessage(reader)
			if readErr != nil {
				errorsChannel <- readErr
				return
			}
			started := time.Now()
			command, parseErr := parseCommand(request)
			if parseErr != nil {
				errorsChannel <- parseErr
				return
			}
			pendingRequest := pendingRequest{message: command, wire: request, started: started, context: connectionContext}
			if isHandshake(command.name) {
				document, helloErr := sanitizedHello(session.hello, upstream.MongoDB)
				if helloErr != nil {
					errorsChannel <- helloErr
					return
				}
				response := makeCommandReply(request, responseRequestID.Add(1), document)
				if writeErr := downstreamWriter.write(response.raw); writeErr != nil {
					errorsChannel <- writeErr
					return
				}
				recordMongo(sink, upstream, connectionName, pendingRequest, response, document, nil)
				continue
			}
			if mongoAuthCommand(command.name) {
				document := authenticator.handle(command)
				response := makeCommandReply(request, responseRequestID.Add(1), document)
				if writeErr := downstreamWriter.write(response.raw); writeErr != nil {
					errorsChannel <- writeErr
					return
				}
				recordMongo(sink, upstream, connectionName, pendingRequest, response, document, nil)
				continue
			}
			if !authenticator.authenticated {
				document := mongoErrorDocument(13, "Unauthorized", "MongoDB listener authentication is required")
				response := makeCommandReply(request, responseRequestID.Add(1), document)
				if writeErr := downstreamWriter.write(response.raw); writeErr != nil {
					errorsChannel <- writeErr
					return
				}
				recordMongo(sink, upstream, connectionName, pendingRequest, response, document, nil)
				continue
			}
			if !upstreamAuthenticated {
				authContext, authCancel := context.WithTimeout(ctx, 15*time.Second)
				if upstream.MongoDB != nil && upstream.MongoDB.UpstreamUsername != "" {
					if authErr := session.authenticate(authContext, *upstream.MongoDB); authErr != nil {
						authCancel()
						recordMongo(sink, upstream, connectionName, pendingRequest, wireMessage{}, nil, authErr)
						errorsChannel <- authErr
						return
					}
				}
				authCancel()
				upstreamAuthenticated = true
				startUpstreamReader()
			}
			if command.flags&2 == 0 {
				pending.put(pendingRequest)
			}
			if _, writeErr := session.connection.Write(request.raw); writeErr != nil {
				if command.flags&2 == 0 {
					pending.get(request.requestID, false)
				}
				recordMongo(sink, upstream, connectionName, pendingRequest, wireMessage{}, nil, writeErr)
				errorsChannel <- writeErr
				return
			}
			if command.flags&2 != 0 {
				recordMongo(sink, upstream, connectionName, pendingRequest, wireMessage{}, nil, nil)
			}
		}
	}()

	select {
	case err = <-errorsChannel:
	case <-ctx.Done():
		err = ctx.Err()
	}
	_ = downstream.Close()
	_ = session.connection.Close()
	for _, request := range pending.drain() {
		recordMongo(sink, upstream, connectionName, request, wireMessage{}, nil, err)
	}
	return err
}

func isTLSConnection(connection net.Conn) bool {
	_, ok := connection.(*tls.Conn)
	return ok
}

type localAuthenticator struct {
	options       *config.MongoDBOptions
	authenticated bool
	conversation  *scramServer
}

func newLocalAuthenticator(options *config.MongoDBOptions) *localAuthenticator {
	return &localAuthenticator{options: options, authenticated: options == nil || options.ListenerUsername == ""}
}

func (authenticator *localAuthenticator) handle(command commandMessage) bson.Raw {
	if authenticator.options == nil || authenticator.options.ListenerUsername == "" {
		return mongoErrorDocument(18, "AuthenticationFailed", "MongoDB listener credentials are not configured")
	}
	if !strings.EqualFold(command.database, authSource(authenticator.options.ListenerAuthSource)) {
		return mongoErrorDocument(18, "AuthenticationFailed", "invalid MongoDB listener authentication database")
	}
	switch {
	case strings.EqualFold(command.name, "saslStart"):
		mechanism, _ := command.document.Lookup("mechanism").StringValueOK()
		_, payload, ok := command.document.Lookup("payload").BinaryOK()
		if !ok {
			return mongoErrorDocument(18, "AuthenticationFailed", "SCRAM payload is missing")
		}
		conversation, serverFirst, err := newSCRAMServer(mechanism, authenticator.options.ListenerUsername, authenticator.options.ListenerPassword, payload)
		if err != nil {
			return mongoErrorDocument(18, "AuthenticationFailed", err.Error())
		}
		authenticator.conversation = conversation
		return mustDocument(bson.D{{Key: "conversationId", Value: int32(1)}, {Key: "done", Value: false}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: serverFirst}}, {Key: "ok", Value: 1}})
	case strings.EqualFold(command.name, "saslContinue"):
		conversationID, ok := rawInt32(command.document.Lookup("conversationId"))
		_, payload, payloadOK := command.document.Lookup("payload").BinaryOK()
		if authenticator.conversation == nil || !ok || conversationID != 1 || !payloadOK {
			return mongoErrorDocument(18, "AuthenticationFailed", "invalid SCRAM conversation")
		}
		serverFinal, err := authenticator.conversation.finish(payload)
		if err != nil {
			authenticator.conversation = nil
			return mongoErrorDocument(18, "AuthenticationFailed", err.Error())
		}
		authenticator.authenticated = true
		authenticator.conversation = nil
		return mustDocument(bson.D{{Key: "conversationId", Value: int32(1)}, {Key: "done", Value: true}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: serverFinal}}, {Key: "ok", Value: 1}})
	default:
		return mongoErrorDocument(18, "AuthenticationFailed", "unsupported MongoDB authentication command")
	}
}

func sanitizedHello(upstreamHello bson.Raw, options *config.MongoDBOptions) (bson.Raw, error) {
	var source bson.D
	if err := bson.Unmarshal(upstreamHello, &source); err != nil {
		return nil, err
	}
	blocked := map[string]struct{}{
		"hosts": {}, "passives": {}, "arbiters": {}, "primary": {}, "setName": {}, "me": {}, "msg": {}, "compression": {},
		"serviceId": {}, "topologyVersion": {}, "speculativeAuthenticate": {}, "saslSupportedMechs": {},
	}
	result := make(bson.D, 0, len(source)+2)
	for _, element := range source {
		if _, remove := blocked[element.Key]; !remove {
			result = append(result, element)
		}
	}
	result = setElement(result, "helloOk", true)
	if options != nil && options.ListenerUsername != "" {
		result = setElement(result, "saslSupportedMechs", bson.A{"SCRAM-SHA-256", "SCRAM-SHA-1"})
	}
	return marshalDocument(result)
}

func setElement(document bson.D, key string, value any) bson.D {
	for index := range document {
		if document[index].Key == key {
			document[index].Value = value
			return document
		}
	}
	return append(document, bson.E{Key: key, Value: value})
}

func mongoErrorDocument(code int32, codeName, message string) bson.Raw {
	return mustDocument(bson.D{{Key: "ok", Value: 0}, {Key: "errmsg", Value: message}, {Key: "code", Value: code}, {Key: "codeName", Value: codeName}})
}

func mustDocument(value any) bson.Raw {
	document, err := marshalDocument(value)
	if err != nil {
		panic(err)
	}
	return document
}
