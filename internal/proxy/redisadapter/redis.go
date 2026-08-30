package redisadapter

import (
	"bufio"
	"context"
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

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

type pending struct {
	started   time.Time
	operation string
	request   observation.Payload
}

func (a *Adapter) Run(ctx context.Context, upstream config.Upstream, sink observation.Sink, ready func(string)) error {
	listener, err := net.Listen("tcp", upstream.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", upstream.ListenAddr, err)
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
	server, phase, err := dialUpstream(ctx, upstream)
	if err != nil {
		recordConnectionError(upstream, client, sink, phase, err)
		return
	}
	defer server.Close()
	connectionID := observation.NewID()
	queue := make(chan pending, 1024)
	done := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(done); _ = client.Close(); _ = server.Close() }) }
	go func() {
		reader := bufio.NewReader(client)
		for {
			request, err := readFrame(reader)
			if err != nil {
				if !errors.Is(err, io.EOF) && !isClosed(err) {
					recordProtocolError(upstream, connectionID, sink, "request", err)
				}
				stop()
				return
			}
			operation := command(request.value)
			payload := redisPayload(request, operation)
			if operation == "AUTH" || operation == "HELLO" && containsAuth(request.value) {
				payload.Text = "[redacted]"
			}
			select {
			case queue <- pending{started: time.Now(), operation: operation, request: payload}:
			case <-done:
				return
			}
			if err = writeAll(server, request.raw); err != nil {
				stop()
				return
			}
		}
	}()
	reader := bufio.NewReader(server)
	for {
		response, err := readFrame(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !isClosed(err) {
				recordProtocolError(upstream, connectionID, sink, "response", err)
			}
			stop()
			return
		}
		if err = writeAll(client, response.raw); err != nil {
			stop()
			return
		}
		var item pending
		if response.value.kind == '>' || response.value.kind == '|' {
			operation := "PUSH"
			if response.value.kind == '|' {
				operation = "ATTRIBUTE"
			}
			item = pending{started: time.Now(), operation: operation, request: observation.Payload{Kind: "redis", Summary: "unsolicited server metadata"}}
		} else {
			select {
			case item = <-queue:
			default:
				item = pending{started: time.Now(), operation: "PUSH", request: observation.Payload{Kind: "redis", Summary: "unsolicited server frame"}}
			}
		}
		outcome := "ok"
		errorText := ""
		if response.value.kind == '-' || response.value.kind == '!' {
			outcome = "error"
			errorText = response.value.text
		}
		attributes := map[string]string{"command": item.operation}
		if upstream.Redis != nil {
			if upstream.Redis.TLS.Enabled {
				attributes["upstreamTLS"] = "enabled"
			}
			if upstream.Redis.Password != "" {
				attributes["upstreamAuth"] = "configured"
			}
			if upstream.Redis.Database > 0 {
				attributes["database"] = strconv.Itoa(upstream.Redis.Database)
			}
		}
		sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "redis", Connection: connectionID, Operation: item.operation, StartedAt: item.started, DurationUS: time.Since(item.started).Microseconds(), Outcome: outcome, Error: errorText, Request: item.request, Response: redisPayload(response, response.value.render(100)), Attributes: attributes})
	}
}

func dialUpstream(ctx context.Context, upstream config.Upstream) (net.Conn, string, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	var connection net.Conn
	var err error
	options := config.RedisOptions{}
	if upstream.Redis != nil {
		options = *upstream.Redis
	}
	if options.TLS.Enabled {
		host, _, _ := net.SplitHostPort(upstream.Target)
		tlsConfig, configErr := tlsutil.Client(options.TLS, host)
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
	if options.Password != "" {
		arguments := []string{"AUTH"}
		if options.Username != "" {
			arguments = append(arguments, options.Username)
		}
		arguments = append(arguments, options.Password)
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
	for _, item := range v.items {
		if strings.EqualFold(item.scalar(), "AUTH") {
			return true
		}
	}
	return false
}

func isClosed(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(strings.ToLower(err.Error()), "use of closed network connection")
}

func command(v value) string {
	if len(v.items) > 0 {
		return strings.ToUpper(v.items[0].scalar())
	}
	fields := strings.Fields(v.text)
	if len(fields) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(fields[0])
}
func redisPayload(item frame, summary string) observation.Payload {
	rendered := item.value.render(256 * 1024)
	return observation.Payload{Kind: "redis", Summary: summary, Text: rendered, Size: int64(len(item.raw)), Truncated: len(rendered) >= 256*1024}
}
func recordConnectionError(upstream config.Upstream, connection net.Conn, sink observation.Sink, phase string, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "redis", Connection: connection.RemoteAddr().String(), Operation: phase, StartedAt: time.Now(), Outcome: "error", Error: err.Error(), Response: observation.Payload{Kind: "text", Summary: "upstream connection setup failed"}})
}
func recordProtocolError(upstream config.Upstream, connection string, sink observation.Sink, direction string, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "redis", Connection: connection, Operation: "PROTOCOL", StartedAt: time.Now(), Outcome: "error", Error: err.Error(), Attributes: map[string]string{"direction": direction}})
}
