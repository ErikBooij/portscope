package rabbitadapter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
)

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

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

func (adapter *Adapter) handleConnection(ctx context.Context, upstream config.Upstream, sink observation.Sink, downstream net.Conn) error {
	defer downstream.Close()
	started := time.Now()
	negotiated, err := negotiate(ctx, downstream, upstream)
	connection := downstream.RemoteAddr().String()
	if err != nil {
		recordAMQPConnect(sink, upstream, connection, started, negotiated, err)
		return err
	}
	defer negotiated.upstream.Close()
	recordAMQPConnect(sink, upstream, connection, started, negotiated, nil)
	inspector := newAMQPInspector(upstream, sink, connection, negotiated)
	errorsChannel := make(chan error, 2)
	go relayAMQP(downstream, negotiated.upstream, negotiated.frameMax, clientToServer, inspector, errorsChannel)
	go relayAMQP(negotiated.upstream, downstream, negotiated.frameMax, serverToClient, inspector, errorsChannel)
	select {
	case err = <-errorsChannel:
	case <-ctx.Done():
		err = ctx.Err()
	}
	_ = downstream.Close()
	_ = negotiated.upstream.Close()
	inspector.failPending(err)
	return err
}

func relayAMQP(source net.Conn, target net.Conn, frameMax uint32, direction amqpDirection, inspector *amqpInspector, result chan<- error) {
	for {
		frame, err := readFrame(source, frameMax)
		if err != nil {
			result <- err
			return
		}
		if direction == clientToServer {
			if classID, methodID, ok := methodID(frame); ok && classID == 10 && (methodID == 11 || methodID == 70) {
				blocked := errors.New("application attempted to replace terminated RabbitMQ credentials")
				inspector.observeBlocked(frame, blocked)
				result <- blocked
				return
			}
		}
		if err := writeAll(target, frame.raw); err != nil {
			result <- err
			return
		}
		inspector.observe(direction, frame)
	}
}

func recordAMQPConnect(sink observation.Sink, upstream config.Upstream, connection string, started time.Time, negotiated negotiatedConnection, err error) {
	outcome, errorText := "ok", ""
	if err != nil {
		outcome, errorText = "error", err.Error()
	}
	attributes := map[string]string{
		"downstreamTLS": fmt.Sprintf("%t", negotiated.downstreamTLS || upstream.ListenerTLS != nil && upstream.ListenerTLS.Enabled),
		"upstreamTLS":   fmt.Sprintf("%t", negotiated.upstreamTLS),
	}
	if upstream.RabbitMQ != nil {
		attributes["listenerVhost"] = upstream.RabbitMQ.ListenerVHost
		attributes["upstreamVhost"] = upstream.RabbitMQ.UpstreamVHost
	}
	if negotiated.frameMax != 0 {
		attributes["frameMax"] = fmt.Sprintf("%d", negotiated.frameMax)
	}
	if negotiated.channelMax != 0 {
		attributes["channelMax"] = fmt.Sprintf("%d", negotiated.channelMax)
	}
	attributes["heartbeatSeconds"] = fmt.Sprintf("%d", negotiated.heartbeat)
	if negotiated.serverProduct != "" {
		attributes["serverProduct"] = negotiated.serverProduct
	}
	if negotiated.serverVersion != "" {
		attributes["serverVersion"] = negotiated.serverVersion
	}
	sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "rabbitmq", Connection: connection, Operation: "CONNECT",
		StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: outcome, Error: errorText,
		Request:  observation.Payload{Kind: "json", Summary: "AMQP 0-9-1 · PLAIN credentials [redacted]", JSON: []byte(`{"protocol":"AMQP 0-9-1","authentication":"[redacted]"}`)},
		Response: observation.Payload{Kind: "text", Summary: map[bool]string{true: "connection open", false: "connection failed"}[err == nil]}, Attributes: attributes,
	})
}
