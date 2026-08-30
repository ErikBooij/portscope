package redisadapter

import (
	"bufio"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
)

const maxPendingCommands = 1024

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

type pending struct {
	started       time.Time
	operation     string
	request       observation.Payload
	forward       []byte
	localResponse []byte
	accepted      chan struct{}
}

type responseEvent struct {
	frame frame
	err   error
}

type protocolError struct {
	direction string
	err       error
}

func (a *Adapter) Run(ctx context.Context, upstream config.Upstream, sink observation.Sink, ready func(string)) error {
	listener, err := net.Listen("tcp", upstream.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", upstream.ListenAddr, err)
	}
	if upstream.ListenerTLS != nil && upstream.ListenerTLS.Enabled {
		tlsConfig, configErr := tlsutil.Server(*upstream.ListenerTLS)
		if configErr != nil {
			_ = listener.Close()
			return configErr
		}
		listener = tls.NewListener(listener, tlsConfig)
	}
	defer listener.Close()
	ready(listener.Addr().String())
	go func() { <-ctx.Done(); _ = listener.Close() }()
	var clients sync.WaitGroup
	defer clients.Wait()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		clients.Add(1)
		go func() { defer clients.Done(); a.handle(ctx, upstream, connection, sink) }()
	}
}

func (a *Adapter) handle(ctx context.Context, upstream config.Upstream, client net.Conn, sink observation.Sink) {
	defer client.Close()
	connectionID := observation.NewID()
	options := config.RedisOptions{}
	if upstream.Redis != nil {
		options = *upstream.Redis
	}
	clientReader := bufio.NewReader(client)
	authenticated := options.ListenerPassword == ""

	// Redis has no server greeting, so listener authentication can be completed
	// before Portscope sends configured credentials to the upstream at all.
	var first *pending
	for !authenticated {
		request, err := readFrame(clientReader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !isClosed(err) {
				recordProtocolError(upstream, connectionID, sink, "request", err)
			}
			return
		}
		decision, nextAuthenticated := inspectRequest(request, options, authenticated)
		authenticated = nextAuthenticated
		if len(decision.forward) > 0 {
			first = &decision
			break
		}
		if err := writeAll(client, decision.localResponse); err != nil {
			return
		}
		recordRedisInteraction(upstream, sink, connectionID, decision, mustFrame(decision.localResponse))
	}

	server, phase, err := dialUpstream(ctx, upstream)
	if err != nil {
		recordConnectionError(upstream, client, sink, phase, err)
		return
	}
	defer server.Close()

	commands := make(chan pending, maxPendingCommands)
	responses := make(chan responseEvent, 32)
	errorsChannel := make(chan protocolError, 2)
	done := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(done); _ = client.Close(); _ = server.Close() }) }
	defer stop()

	go func(initiallyAuthenticated bool) {
		currentAuthenticated := initiallyAuthenticated
		forward := func(decision pending) bool {
			decision.accepted = make(chan struct{})
			select {
			case commands <- decision:
			case <-done:
				return false
			}
			select {
			case <-decision.accepted:
			case <-done:
				return false
			}
			if len(decision.forward) > 0 {
				if writeErr := writeAll(server, decision.forward); writeErr != nil {
					select {
					case errorsChannel <- protocolError{direction: "request", err: writeErr}:
					case <-done:
					}
					return false
				}
			}
			return true
		}
		if first != nil && !forward(*first) {
			return
		}
		for {
			request, readErr := readFrame(clientReader)
			if readErr != nil {
				select {
				case errorsChannel <- protocolError{direction: "request", err: readErr}:
				case <-done:
				}
				return
			}
			decision, nextAuthenticated := inspectRequest(request, options, currentAuthenticated)
			currentAuthenticated = nextAuthenticated
			if !forward(decision) {
				return
			}
		}
	}(authenticated)

	go func() {
		reader := bufio.NewReader(server)
		for {
			response, readErr := readFrame(reader)
			select {
			case responses <- responseEvent{frame: response, err: readErr}:
			case <-done:
				return
			}
			if readErr != nil {
				return
			}
		}
	}()

	queue := make([]pending, 0, 16)
	for {
		for len(queue) > 0 && len(queue[0].localResponse) > 0 {
			item := queue[0]
			queue = queue[1:]
			if err := writeAll(client, item.localResponse); err != nil {
				return
			}
			recordRedisInteraction(upstream, sink, connectionID, item, mustFrame(item.localResponse))
		}
		var commandInput <-chan pending
		if len(queue) < maxPendingCommands {
			commandInput = commands
		}
		select {
		case item := <-commandInput:
			queue = append(queue, item)
			close(item.accepted)
		case event := <-responses:
			if event.err != nil {
				if !errors.Is(event.err, io.EOF) && !isClosed(event.err) {
					recordProtocolError(upstream, connectionID, sink, "response", event.err)
				}
				return
			}
			response := event.frame
			if err := writeAll(client, response.raw); err != nil {
				return
			}
			if response.value.kind == '>' || response.value.kind == '|' {
				operation := "PUSH"
				if response.value.kind == '|' {
					operation = "ATTRIBUTE"
				}
				recordRedisInteraction(upstream, sink, connectionID, pending{started: time.Now(), operation: operation, request: observation.Payload{Kind: "redis", Summary: "unsolicited server metadata"}}, response)
				continue
			}
			if len(queue) == 0 {
				recordRedisInteraction(upstream, sink, connectionID, pending{started: time.Now(), operation: "PUSH", request: observation.Payload{Kind: "redis", Summary: "unsolicited server frame"}}, response)
				continue
			}
			item := queue[0]
			queue = queue[1:]
			recordRedisInteraction(upstream, sink, connectionID, item, response)
		case issue := <-errorsChannel:
			if !errors.Is(issue.err, io.EOF) && !isClosed(issue.err) {
				recordProtocolError(upstream, connectionID, sink, issue.direction, issue.err)
			}
			return
		case <-ctx.Done():
			return
		}
	}
}

func inspectRequest(request frame, options config.RedisOptions, authenticated bool) (pending, bool) {
	operation := command(request.value)
	item := pending{started: time.Now(), operation: operation, request: redisPayload(request, operation)}
	arguments := commandArguments(request.value)
	if operation == "AUTH" || operation == "HELLO" && containsAuth(request.value) {
		item.request.Text = "[redacted]"
	}

	switch operation {
	case "AUTH":
		valid, syntax := validateListenerAuth(arguments, options)
		if syntax != "" {
			item.localResponse = []byte("-ERR " + syntax + "\r\n")
			return item, authenticated
		}
		if !valid {
			item.localResponse = []byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n")
			return item, authenticated
		}
		item.localResponse = []byte("+OK\r\n")
		return item, true
	case "HELLO":
		rewritten, suppliedAuth, valid, syntax := inspectHello(arguments, options)
		if syntax != "" {
			item.localResponse = []byte("-ERR " + syntax + "\r\n")
			return item, authenticated
		}
		if suppliedAuth && !valid {
			item.localResponse = []byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n")
			return item, authenticated
		}
		if !authenticated && !suppliedAuth {
			item.localResponse = []byte("-NOAUTH Authentication required.\r\n")
			return item, false
		}
		item.forward = encodeCommand(rewritten...)
		return item, authenticated || valid
	}
	if !authenticated {
		item.localResponse = []byte("-NOAUTH Authentication required.\r\n")
		return item, false
	}
	if operation == "SELECT" {
		if len(arguments) != 2 {
			item.localResponse = []byte("-ERR wrong number of arguments for 'select' command\r\n")
			return item, true
		}
		database, err := strconv.Atoi(arguments[1])
		if err != nil || database != options.Database {
			item.localResponse = []byte(fmt.Sprintf("-ERR Portscope listener is pinned to Redis database %d\r\n", options.Database))
			return item, true
		}
		item.localResponse = []byte("+OK\r\n")
		return item, true
	}
	if operation == "RESET" {
		item.localResponse = []byte("-ERR RESET is disabled because Portscope owns the upstream authentication and database state\r\n")
		return item, true
	}
	item.forward = request.raw
	return item, true
}

func validateListenerAuth(arguments []string, options config.RedisOptions) (bool, string) {
	if options.ListenerPassword == "" {
		return false, "AUTH called without a password configured for the Portscope listener"
	}
	username := "default"
	password := ""
	switch len(arguments) {
	case 2:
		password = arguments[1]
	case 3:
		username, password = arguments[1], arguments[2]
	default:
		return false, "wrong number of arguments for 'auth' command"
	}
	expectedUser := options.ListenerUsername
	if expectedUser == "" {
		expectedUser = "default"
	}
	return secretEqual(username, expectedUser) && secretEqual(password, options.ListenerPassword), ""
}

func inspectHello(arguments []string, options config.RedisOptions) ([]string, bool, bool, string) {
	if len(arguments) == 1 {
		return arguments, false, false, ""
	}
	if arguments[1] != "2" && arguments[1] != "3" {
		return nil, false, false, "NOPROTO unsupported protocol version"
	}
	rewritten := append([]string(nil), arguments[:2]...)
	suppliedAuth, valid := false, false
	for index := 2; index < len(arguments); {
		switch strings.ToUpper(arguments[index]) {
		case "AUTH":
			if suppliedAuth || index+2 >= len(arguments) {
				return nil, false, false, "syntax error"
			}
			suppliedAuth = true
			valid, _ = validateListenerAuth([]string{"AUTH", arguments[index+1], arguments[index+2]}, options)
			index += 3
		case "SETNAME":
			if index+1 >= len(arguments) {
				return nil, false, false, "syntax error"
			}
			rewritten = append(rewritten, arguments[index], arguments[index+1])
			index += 2
		default:
			return nil, false, false, "syntax error"
		}
	}
	return rewritten, suppliedAuth, valid, ""
}

func secretEqual(value, expected string) bool {
	return subtle.ConstantTimeCompare([]byte(value), []byte(expected)) == 1
}

func commandArguments(v value) []string {
	if len(v.items) > 0 {
		arguments := make([]string, len(v.items))
		for index := range v.items {
			arguments[index] = v.items[index].scalar()
		}
		return arguments
	}
	return strings.Fields(v.text)
}

func mustFrame(raw []byte) frame {
	item, err := readFrame(bufio.NewReader(strings.NewReader(string(raw))))
	if err != nil {
		panic(err)
	}
	return item
}

func dialUpstream(ctx context.Context, upstream config.Upstream) (net.Conn, string, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	var connection net.Conn
	var err error
	options := config.RedisOptions{}
	if upstream.Redis != nil {
		options = *upstream.Redis
	}
	if options.UpstreamTLS.Enabled {
		host, _, _ := net.SplitHostPort(upstream.Target)
		tlsConfig, configErr := tlsutil.Client(options.UpstreamTLS, host)
		if configErr != nil {
			return nil, "CONNECT", configErr
		}
		connection, err = (&tls.Dialer{NetDialer: dialer, Config: tlsConfig}).DialContext(ctx, "tcp", upstream.Target)
	} else {
		connection, err = dialer.DialContext(ctx, "tcp", upstream.Target)
	}
	if err != nil {
		return nil, "CONNECT", err
	}
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = connection.Close()
		return nil, "CONNECT", err
	}
	fail := func(phase string, err error) (net.Conn, string, error) {
		_ = connection.Close()
		return nil, phase, err
	}
	reader := bufio.NewReader(connection)
	if options.UpstreamPassword != "" {
		arguments := []string{"AUTH"}
		if options.UpstreamUsername != "" {
			arguments = append(arguments, options.UpstreamUsername)
		}
		arguments = append(arguments, options.UpstreamPassword)
		if err := writeAll(connection, encodeCommand(arguments...)); err != nil {
			return fail("AUTH", err)
		}
		response, err := readFrame(reader)
		if err != nil {
			return fail("AUTH", err)
		}
		if response.value.kind == '-' || response.value.kind == '!' {
			return fail("AUTH", errors.New("Redis upstream rejected configured credentials"))
		}
	}
	if options.Database > 0 {
		if err := writeAll(connection, encodeCommand("SELECT", strconv.Itoa(options.Database))); err != nil {
			return fail("SELECT", err)
		}
		response, err := readFrame(reader)
		if err != nil {
			return fail("SELECT", err)
		}
		if response.value.kind == '-' || response.value.kind == '!' {
			return fail("SELECT", fmt.Errorf("Redis upstream rejected database %d", options.Database))
		}
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return fail("CONNECT", err)
	}
	return connection, "", nil
}

func encodeCommand(arguments ...string) []byte {
	var builder strings.Builder
	fmt.Fprintf(&builder, "*%d\r\n", len(arguments))
	for _, argument := range arguments {
		fmt.Fprintf(&builder, "$%d\r\n%s\r\n", len(argument), argument)
	}
	return []byte(builder.String())
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrUnexpectedEOF
		}
		data = data[written:]
	}
	return nil
}

func containsAuth(v value) bool {
	for _, item := range commandArguments(v) {
		if strings.EqualFold(item, "AUTH") {
			return true
		}
	}
	return false
}

func isClosed(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(strings.ToLower(err.Error()), "use of closed network connection")
}

func command(v value) string {
	arguments := commandArguments(v)
	if len(arguments) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(arguments[0])
}

func redisPayload(item frame, summary string) observation.Payload {
	rendered := item.value.render(256 * 1024)
	return observation.Payload{Kind: "redis", Summary: summary, Text: rendered, Size: int64(len(item.raw)), Truncated: len(rendered) >= 256*1024}
}

func recordRedisInteraction(upstream config.Upstream, sink observation.Sink, connectionID string, item pending, response frame) {
	outcome, errorText := "ok", ""
	if response.value.kind == '-' || response.value.kind == '!' {
		outcome, errorText = "error", response.value.text
	}
	attributes := map[string]string{"command": item.operation, "auth": "terminated"}
	if upstream.ListenerTLS != nil && upstream.ListenerTLS.Enabled {
		attributes["downstreamTLS"] = "enabled"
	}
	if upstream.Redis != nil {
		if upstream.Redis.UpstreamTLS.Enabled {
			attributes["upstreamTLS"] = "enabled"
		}
		if upstream.Redis.UpstreamPassword != "" {
			attributes["upstreamAuth"] = "configured"
		}
		if upstream.Redis.ListenerPassword != "" {
			attributes["listenerAuth"] = "configured"
		}
		attributes["database"] = strconv.Itoa(upstream.Redis.Database)
	}
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "redis", Connection: connectionID, Operation: item.operation, StartedAt: item.started, DurationUS: time.Since(item.started).Microseconds(), Outcome: outcome, Error: errorText, Request: item.request, Response: redisPayload(response, response.value.render(100)), Attributes: attributes})
}

func recordConnectionError(upstream config.Upstream, connection net.Conn, sink observation.Sink, phase string, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "redis", Connection: connection.RemoteAddr().String(), Operation: phase, StartedAt: time.Now(), Outcome: "error", Error: err.Error(), Response: observation.Payload{Kind: "text", Summary: "upstream connection setup failed"}})
}

func recordProtocolError(upstream config.Upstream, connection string, sink observation.Sink, direction string, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "redis", Connection: connection, Operation: "PROTOCOL", StartedAt: time.Now(), Outcome: "error", Error: err.Error(), Attributes: map[string]string{"direction": direction}})
}
