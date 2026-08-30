package mysqladapter

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

func (adapter *Adapter) Run(ctx context.Context, upstream config.Upstream, sink observation.Sink, ready func(string)) error {
	tlsConfig, err := serverTLSConfig(upstream.ListenerTLS)
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
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return acceptErr
		}
		clients.Add(1)
		go func() {
			defer clients.Done()
			adapter.handle(ctx, upstream, connection, tlsConfig, sink)
		}()
	}
}

func (adapter *Adapter) handle(ctx context.Context, upstream config.Upstream, client net.Conn, listenerTLS *tls.Config, sink observation.Sink) {
	defer client.Close()
	started := time.Now()
	connectionID := observation.NewID()
	server, err := openUpstream(ctx, upstream)
	if err != nil {
		recordConnectFailure(upstream, sink, connectionID, started, err)
		return
	}
	defer server.connection.Close()
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

	scramble, err := randomScramble()
	if err != nil {
		recordConnectFailure(upstream, sink, connectionID, started, err)
		return
	}
	localConnectionID, err := randomConnectionID()
	if err != nil {
		recordConnectFailure(upstream, sink, connectionID, started, err)
		return
	}
	localCapabilities := server.greeting.capabilities & inspectableCapabilities
	if err := writePacket(client, packet{sequence: 0, payload: makeGreeting(localConnectionID, scramble, localCapabilities, listenerTLS != nil)}); err != nil {
		return
	}
	_ = client.SetDeadline(time.Now().Add(15 * time.Second))
	reader := bufio.NewReader(client)
	first, err := readPacket(reader)
	if err != nil {
		return
	}
	if first.sequence != 1 {
		sendMySQLError(client, 2, 1043, "08S01", "invalid MySQL handshake sequence")
		return
	}
	sequence := byte(2)
	downstreamTLS := false
	if isSSLRequest(first.payload) {
		if listenerTLS == nil {
			sendMySQLError(client, sequence, 3159, "HY000", "TLS is not enabled on this Portscope listener")
			return
		}
		secure := tls.Server(client, listenerTLS.Clone())
		if err := secure.HandshakeContext(ctx); err != nil {
			return
		}
		client = secure
		reader = bufio.NewReader(secure)
		downstreamTLS = true
		first, err = readPacket(reader)
		if err != nil {
			return
		}
		if first.sequence != sequence {
			sendMySQLError(client, sequence+1, 1043, "08S01", "invalid MySQL TLS handshake sequence")
			return
		}
		sequence++
	} else if listenerTLS != nil {
		sendMySQLError(client, sequence, 3159, "HY000", "this Portscope MySQL listener requires TLS")
		return
	}
	handshake, err := parseClientHandshake(first.payload)
	if err != nil {
		sendMySQLError(client, sequence, 1043, "08S01", err.Error())
		return
	}
	if handshake.plugin != "" && handshake.plugin != "mysql_native_password" {
		switchPayload := append([]byte{0xfe}, []byte("mysql_native_password")...)
		switchPayload = append(switchPayload, 0)
		switchPayload = append(switchPayload, scramble...)
		switchPayload = append(switchPayload, 0)
		if err := writePacket(client, packet{sequence: sequence, payload: switchPayload}); err != nil {
			return
		}
		sequence++
		reply, readErr := readPacket(reader)
		if readErr != nil {
			return
		}
		if reply.sequence != sequence {
			sendMySQLError(client, sequence+1, 1043, "08S01", "invalid MySQL auth switch sequence")
			return
		}
		handshake.authResponse = append([]byte(nil), reply.payload...)
		sequence++
	}
	options := upstream.MySQL
	if handshake.username != options.ListenerUsername || !verifyNativePassword(handshake.authResponse, options.ListenerPassword, scramble) {
		sendMySQLError(client, sequence, 1045, "28000", "access denied by Portscope listener authentication")
		recordConnectFailure(upstream, sink, connectionID, started, errors.New("downstream MySQL authentication failed"))
		return
	}
	negotiated := sanitizedClientCapabilities(handshake.capabilities) & localCapabilities
	if err := authenticateUpstream(ctx, upstream, server, negotiated); err != nil {
		sendMySQLError(client, sequence, 1045, "28000", "Portscope could not authenticate to the configured MySQL upstream")
		recordConnectFailure(upstream, sink, connectionID, started, err)
		return
	}
	if err := writePacket(client, packet{sequence: sequence, payload: mysqlOKPayload()}); err != nil {
		return
	}
	_ = client.SetDeadline(time.Time{})
	recordConnect(upstream, sink, connectionID, started, handshake, server, downstreamTLS)

	statements := make(map[uint32]*preparedStatement)
	for {
		commandStarted := time.Now()
		request, err := readClientCommand(reader, server.connection)
		if err != nil {
			if !closedError(err) {
				recordProtocolFailure(upstream, sink, connectionID, "client_to_upstream", err)
			}
			return
		}
		command := parseCommand(request, statements)
		if command.code == comChangeUser || command.code == 0x12 || command.code == 0x15 {
			sendMySQLError(client, request.nextSequence, 1235, "42000", command.operation+" is not supported by this Portscope MySQL adapter")
			recordCommand(upstream, sink, connectionID, commandStarted, command, responseInfo{payload: observation.Payload{Kind: "mysql", Summary: "not supported"}, outcome: "error", errorText: "command is not supported"}, server, downstreamTLS)
			continue
		}
		if command.noResponse {
			if command.code == comStmtClose {
				delete(statements, command.statementID)
			}
			recordCommand(upstream, sink, connectionID, commandStarted, command, responseInfo{payload: observation.Payload{Kind: "mysql", Summary: "no response"}, outcome: "ok"}, server, downstreamTLS)
			if command.code == comQuit {
				return
			}
			continue
		}
		response, err := readCommandResponse(server.reader, client, request.nextSequence, server.capabilities, command)
		if err != nil {
			recordProtocolFailure(upstream, sink, connectionID, "upstream_to_client", err)
			return
		}
		if command.code == comStmtPrepare && response.outcome == "ok" {
			statements[response.statementID] = &preparedStatement{query: command.query, parameters: response.parameters, columns: response.definitions}
		}
		recordCommand(upstream, sink, connectionID, commandStarted, command, response, server, downstreamTLS)
	}
}

// readClientCommand inspects the command byte before forwarding, allowing
// connection-changing commands to be rejected locally without corrupting the
// independently authenticated upstream session.
func readClientCommand(reader *bufio.Reader, destination net.Conn) (logicalPacket, error) {
	first, err := readPacket(reader)
	if err != nil {
		return logicalPacket{}, err
	}
	if first.sequence != 0 {
		return logicalPacket{}, fmt.Errorf("MySQL command sequence id %d, expected 0", first.sequence)
	}
	result := logicalPacket{sequence: 0, size: int64(len(first.payload)), nextSequence: 1}
	result.payload = append(result.payload, first.payload[:min(captureLimit, len(first.payload))]...)
	result.truncated = len(first.payload) > captureLimit
	intercept := len(first.payload) > 0 && (first.payload[0] == comChangeUser || first.payload[0] == 0x12 || first.payload[0] == 0x15)
	if !intercept {
		if err := writePacket(destination, first); err != nil {
			return result, err
		}
	}
	length := len(first.payload)
	for length == maxPacketPayload {
		item, readErr := readPacket(reader)
		if readErr != nil {
			return result, readErr
		}
		if item.sequence != result.nextSequence {
			return result, fmt.Errorf("MySQL command sequence id %d, expected %d", item.sequence, result.nextSequence)
		}
		if !intercept {
			if err := writePacket(destination, item); err != nil {
				return result, err
			}
		}
		remaining := captureLimit - len(result.payload)
		if remaining > 0 {
			result.payload = append(result.payload, item.payload[:min(remaining, len(item.payload))]...)
		}
		if len(item.payload) > remaining {
			result.truncated = true
		}
		result.size += int64(len(item.payload))
		result.nextSequence++
		length = len(item.payload)
	}
	return result, nil
}

func sendMySQLError(connection net.Conn, sequence byte, code uint16, state, message string) {
	_ = writePacket(connection, packet{sequence: sequence, payload: mysqlErrorPayload(code, state, message)})
}

func recordConnect(upstream config.Upstream, sink observation.Sink, connectionID string, started time.Time, client clientHandshake, server *authenticatedUpstream, downstreamTLS bool) {
	attributes := map[string]string{
		"clientUser":         client.username,
		"upstreamUser":       upstream.MySQL.UpstreamUsername,
		"serverVersion":      server.greeting.serverVersion,
		"serverConnectionId": strconv.FormatUint(uint64(server.greeting.connectionID), 10),
		"downstreamTLS":      strconv.FormatBool(downstreamTLS),
		"upstreamTLS":        strconv.FormatBool(server.tls),
		"auth":               "terminated",
	}
	if upstream.MySQL.Database != "" {
		attributes["database"] = upstream.MySQL.Database
	}
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "mysql", Connection: connectionID, Operation: "CONNECT", StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: "ok", Request: observation.Payload{Kind: "mysql", Summary: "authenticated as " + client.username}, Response: observation.Payload{Kind: "mysql", Summary: "connected to " + server.greeting.serverVersion}, Attributes: attributes})
}

func recordConnectFailure(upstream config.Upstream, sink observation.Sink, connectionID string, started time.Time, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "mysql", Connection: connectionID, Operation: "CONNECT", StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: "error", Error: err.Error(), Request: observation.Payload{Kind: "mysql", Summary: "connection setup"}, Response: observation.Payload{Kind: "mysql", Summary: "connection failed"}})
}

func recordCommand(upstream config.Upstream, sink observation.Sink, connectionID string, started time.Time, command commandInfo, response responseInfo, server *authenticatedUpstream, downstreamTLS bool) {
	attributes := map[string]string{"command": commandName(command.code), "downstreamTLS": strconv.FormatBool(downstreamTLS), "upstreamTLS": strconv.FormatBool(server.tls)}
	if command.statementID != 0 {
		attributes["statementId"] = strconv.FormatUint(uint64(command.statementID), 10)
	}
	if response.columns > 0 {
		attributes["columns"] = strconv.Itoa(response.columns)
	}
	if response.rows > 0 {
		attributes["rows"] = strconv.Itoa(response.rows)
	}
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "mysql", Connection: connectionID, Operation: command.operation, StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: response.outcome, Error: response.errorText, Request: command.request, Response: response.payload, Attributes: attributes})
}

func recordProtocolFailure(upstream config.Upstream, sink observation.Sink, connectionID, direction string, err error) {
	sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "mysql", Connection: connectionID, Operation: "PROTOCOL", StartedAt: time.Now(), Outcome: "error", Error: err.Error(), Request: observation.Payload{Kind: "mysql", Summary: "protocol error"}, Response: observation.Payload{Kind: "mysql", Summary: "connection closed"}, Attributes: map[string]string{"direction": direction}})
}
