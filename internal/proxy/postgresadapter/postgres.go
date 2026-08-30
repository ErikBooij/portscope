package postgresadapter

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
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

type cancelKey struct{ pid, secret int32 }
type cancelTarget struct {
	address     string
	pid, secret int32
}

// bufferedConn lets TLS consume bytes that bufio.Reader may already have read
// while detecting PostgreSQL's direct TLS negotiation.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (connection *bufferedConn) Read(target []byte) (int, error) {
	return connection.reader.Read(target)
}

type Adapter struct {
	mu      sync.RWMutex
	cancels map[cancelKey]cancelTarget
}

func New() *Adapter { return &Adapter{cancels: make(map[cancelKey]cancelTarget)} }

func (adapter *Adapter) Run(ctx context.Context, upstream config.Upstream, sink observation.Sink, ready func(string)) error {
	listenerTLS, err := postgresListenerTLS(upstream.ListenerTLS)
	if err != nil {
		return err
	}
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
		go func() {
			defer clients.Done()
			adapter.handle(ctx, upstream, connection, listenerTLS, sink)
		}()
	}
}

func (adapter *Adapter) handle(ctx context.Context, upstream config.Upstream, client net.Conn, listenerTLS *tls.Config, sink observation.Sink) {
	defer client.Close()
	started := time.Now()
	connectionID := observation.NewID()
	_ = client.SetDeadline(time.Now().Add(20 * time.Second))
	reader := bufio.NewReader(client)
	startup, secureClient, cancel, err := adapter.acceptStartup(ctx, client, reader, listenerTLS)
	if cancel != nil {
		adapter.forwardCancel(ctx, *cancel, upstream, sink, connectionID, started)
		return
	}
	if err != nil {
		_ = writeMessage(client, errorResponse("FATAL", "08P01", err.Error()))
		return
	}
	if secureClient != nil {
		client = secureClient
		reader = bufio.NewReader(client)
	}
	parameters, err := parseStartup(startup)
	if err != nil {
		_ = writeMessage(client, errorResponse("FATAL", "08P01", err.Error()))
		return
	}
	if negotiation, needed := negotiateProtocolVersion(startup, parameters); needed {
		if err := writeMessage(client, negotiation); err != nil {
			return
		}
	}
	if replication := parameters["replication"]; replication != "" && replication != "false" && replication != "0" {
		_ = writeMessage(client, errorResponse("FATAL", "0A000", "replication connections are not supported by this Portscope adapter"))
		return
	}
	options := *upstream.Postgres
	if parameters["user"] != options.ListenerUsername {
		_ = writeMessage(client, errorResponse("FATAL", "28P01", "invalid Portscope listener credentials"))
		recordConnectFailure(upstream, sink, connectionID, started, errors.New("downstream PostgreSQL username did not match listener configuration"))
		return
	}
	if err := authenticateListener(reader, client, options.ListenerUsername, options.ListenerPassword); err != nil {
		_ = writeMessage(client, errorResponse("FATAL", "28P01", "invalid Portscope listener credentials"))
		recordConnectFailure(upstream, sink, connectionID, started, err)
		return
	}
	server, err := openUpstream(ctx, upstream, parameters)
	if err != nil {
		_ = writeMessage(client, errorResponse("FATAL", "08001", "Portscope could not authenticate to the configured PostgreSQL upstream"))
		recordConnectFailure(upstream, sink, connectionID, started, err)
		return
	}
	defer server.connection.Close()
	localKey, err := randomCancelKey()
	if err != nil {
		return
	}
	adapter.mu.Lock()
	adapter.cancels[localKey] = cancelTarget{address: upstream.Target, pid: server.pid, secret: server.secret}
	adapter.mu.Unlock()
	defer func() { adapter.mu.Lock(); delete(adapter.cancels, localKey); adapter.mu.Unlock() }()

	if err := writeMessage(client, authenticationMessage(0, nil)); err != nil {
		return
	}
	for _, item := range server.startup {
		if item.typ == 'K' {
			item.body = appendInt32(appendInt32(nil, localKey.pid), localKey.secret)
		}
		if err := writeMessage(client, item); err != nil {
			return
		}
	}
	_ = client.SetDeadline(time.Time{})
	recordConnect(upstream, sink, connectionID, started, parameters, server, secureClient != nil)

	state := newTracker(upstream, sink, connectionID, secureClient != nil, server.tls)
	closeOnCancel := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = client.Close()
			_ = server.connection.Close()
		case <-closeOnCancel:
		}
	}()
	defer close(closeOnCancel)
	errors := make(chan error, 2)
	go relayFrontend(reader, server.connection, state, errors)
	go relayBackend(server.reader, client, state, errors)
	if err := <-errors; err != nil && !closedError(err) {
		recordProtocolFailure(upstream, sink, connectionID, err)
	}
}

func (adapter *Adapter) acceptStartup(ctx context.Context, connection net.Conn, reader *bufio.Reader, listenerTLS *tls.Config) ([]byte, *tls.Conn, *cancelKey, error) {
	usingTLS := false
	first, err := reader.Peek(1)
	if err != nil {
		return nil, nil, nil, err
	}
	if first[0] == 0x16 {
		if listenerTLS == nil {
			return nil, nil, nil, errors.New("direct PostgreSQL TLS was attempted on a plaintext listener")
		}
		secure := tls.Server(&bufferedConn{Conn: connection, reader: reader}, listenerTLS.Clone())
		if err := secure.HandshakeContext(ctx); err != nil {
			return nil, nil, nil, err
		}
		connection, reader, usingTLS = secure, bufio.NewReader(secure), true
	}
	for attempts := 0; attempts < 3; attempts++ {
		payload, err := readStartup(reader)
		if err != nil {
			return nil, nil, nil, err
		}
		code, _ := int32At(payload, 0)
		switch code {
		case sslRequestCode:
			if len(payload) != 4 || usingTLS {
				return nil, nil, nil, errors.New("invalid repeated PostgreSQL SSLRequest")
			}
			if listenerTLS == nil {
				if _, err := connection.Write([]byte{'N'}); err != nil {
					return nil, nil, nil, err
				}
				continue
			}
			if _, err := connection.Write([]byte{'S'}); err != nil {
				return nil, nil, nil, err
			}
			secure := tls.Server(connection, listenerTLS.Clone())
			if err := secure.HandshakeContext(ctx); err != nil {
				return nil, nil, nil, err
			}
			connection, reader, usingTLS = secure, bufio.NewReader(secure), true
		case gssRequestCode:
			if len(payload) != 4 || usingTLS {
				return nil, nil, nil, errors.New("invalid PostgreSQL GSSENCRequest")
			}
			if _, err := connection.Write([]byte{'N'}); err != nil {
				return nil, nil, nil, err
			}
		case cancelRequestCode:
			if len(payload) != 12 {
				return nil, nil, nil, errors.New("invalid PostgreSQL CancelRequest")
			}
			pid, _ := int32At(payload, 4)
			secret, _ := int32At(payload, 8)
			return nil, nil, &cancelKey{pid: pid, secret: secret}, nil
		default:
			if listenerTLS != nil && !usingTLS {
				return nil, nil, nil, errors.New("this Portscope PostgreSQL listener requires TLS")
			}
			if usingTLS {
				return payload, connection.(*tls.Conn), nil, nil
			}
			return payload, nil, nil, nil
		}
	}
	return nil, nil, nil, errors.New("too many PostgreSQL encryption negotiation requests")
}

func relayFrontend(reader *bufio.Reader, upstream net.Conn, state *tracker, result chan<- error) {
	for {
		item, err := readMessage(reader)
		if err != nil {
			result <- err
			return
		}
		state.frontend(item)
		if err := writeMessage(upstream, item); err != nil {
			result <- err
			return
		}
		if item.typ == 'X' {
			result <- io.EOF
			return
		}
	}
}

func relayBackend(reader *bufio.Reader, client net.Conn, state *tracker, result chan<- error) {
	for {
		item, err := readMessage(reader)
		if err != nil {
			result <- err
			return
		}
		state.backend(item)
		if err := writeMessage(client, item); err != nil {
			result <- err
			return
		}
	}
}

func (adapter *Adapter) forwardCancel(ctx context.Context, key cancelKey, upstream config.Upstream, sink observation.Sink, connectionID string, started time.Time) {
	adapter.mu.RLock()
	target, exists := adapter.cancels[key]
	adapter.mu.RUnlock()
	if !exists {
		return
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", target.address)
	if err == nil {
		if upstream.Postgres.UpstreamTLS.Enabled {
			err = writeStartup(connection, appendInt32(nil, sslRequestCode))
			var response [1]byte
			if err == nil {
				_, err = io.ReadFull(connection, response[:])
			}
			if err == nil && response[0] != 'S' {
				err = errors.New("PostgreSQL upstream refused TLS for cancellation")
			}
			if err == nil {
				host, _, _ := net.SplitHostPort(target.address)
				var configured *tls.Config
				configured, err = tlsutil.Client(upstream.Postgres.UpstreamTLS, host)
				if err == nil {
					configured = configured.Clone()
					configured.NextProtos = []string{"postgresql"}
					secure := tls.Client(connection, configured)
					err = secure.HandshakeContext(ctx)
					connection = secure
				}
			}
		}
		payload := appendInt32(appendInt32(appendInt32(nil, cancelRequestCode), target.pid), target.secret)
		if err == nil {
			err = writeStartup(connection, payload)
		}
		_ = connection.Close()
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "postgres", Connection: connectionID, Operation: "CANCEL", StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: outcome, Error: errorText(err), Request: observation.Payload{Kind: "postgres", Summary: "cancel active query", Size: 16}, Response: observation.Payload{Kind: "postgres", Summary: outcome}})
}

func recordConnect(upstream config.Upstream, sink observation.Sink, connectionID string, started time.Time, startup map[string]string, server *upstreamSession, downstreamTLS bool) {
	attributes := map[string]string{
		"clientUser": startup["user"], "upstreamUser": upstream.Postgres.UpstreamUsername,
		"clientDatabase": startup["database"], "upstreamDatabase": upstream.Postgres.Database,
		"downstreamTLS": strconv.FormatBool(downstreamTLS), "upstreamTLS": strconv.FormatBool(server.tls), "auth": "terminated", "authMethod": "SCRAM-SHA-256",
	}
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "postgres", Connection: connectionID, Operation: "CONNECT", StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: "ok", Request: observation.Payload{Kind: "postgres", Summary: "authenticated as " + startup["user"]}, Response: observation.Payload{Kind: "postgres", Summary: "connected to " + upstream.Postgres.Database}, Attributes: attributes})
}

func recordConnectFailure(upstream config.Upstream, sink observation.Sink, connectionID string, started time.Time, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "postgres", Connection: connectionID, Operation: "CONNECT", StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: "error", Error: err.Error(), Request: observation.Payload{Kind: "postgres", Summary: "connection setup"}, Response: observation.Payload{Kind: "postgres", Summary: "connection failed"}})
}

func recordProtocolFailure(upstream config.Upstream, sink observation.Sink, connectionID string, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "postgres", Connection: connectionID, Operation: "PROTOCOL", StartedAt: time.Now(), Outcome: "error", Error: err.Error(), Request: observation.Payload{Kind: "postgres", Summary: "stream"}, Response: observation.Payload{Kind: "postgres", Summary: "connection closed"}})
}

func randomCancelKey() (cancelKey, error) {
	var data [8]byte
	if _, err := rand.Read(data[:]); err != nil {
		return cancelKey{}, err
	}
	return cancelKey{pid: int32(binary.BigEndian.Uint32(data[:4]) | 1), secret: int32(binary.BigEndian.Uint32(data[4:]) | 1)}, nil
}

func postgresListenerTLS(options *config.ListenerTLSOptions) (*tls.Config, error) {
	if options == nil || !options.Enabled {
		return nil, nil
	}
	configured, err := tlsutil.Server(*options)
	if err != nil {
		return nil, err
	}
	configured = configured.Clone()
	configured.NextProtos = []string{"postgresql"}
	return configured, nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func closedError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || strings.Contains(strings.ToLower(err.Error()), "closed network connection")
}
