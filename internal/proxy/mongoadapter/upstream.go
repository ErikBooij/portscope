package mongoadapter

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type upstreamSession struct {
	connection net.Conn
	reader     *bufio.Reader
	hello      bson.Raw
	nextID     int32
	tls        bool
}

func openUpstream(ctx context.Context, upstream config.Upstream) (_ *upstreamSession, err error) {
	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", upstream.Target)
	if err != nil {
		return nil, fmt.Errorf("dial MongoDB upstream %s: %w", upstream.Target, err)
	}
	defer func() {
		if err != nil {
			_ = connection.Close()
		}
	}()
	session := &upstreamSession{connection: connection, reader: bufio.NewReader(connection), nextID: 1}
	options := upstream.MongoDB
	if options != nil && options.UpstreamTLS.Enabled {
		host, _, _ := net.SplitHostPort(upstream.Target)
		tlsConfig, tlsErr := tlsutil.Client(options.UpstreamTLS, host)
		if tlsErr != nil {
			return nil, tlsErr
		}
		secure := tls.Client(connection, tlsConfig)
		if tlsErr := secure.HandshakeContext(ctx); tlsErr != nil {
			return nil, fmt.Errorf("MongoDB upstream TLS: %w", tlsErr)
		}
		connection = secure
		session.connection = secure
		session.reader = bufio.NewReader(secure)
		session.tls = true
	}
	hello := bson.D{{Key: "hello", Value: 1}, {Key: "helloOk", Value: true}, {Key: "$db", Value: "admin"}}
	helloResponse, err := session.command(ctx, hello)
	if err != nil {
		return nil, fmt.Errorf("MongoDB upstream hello: %w", err)
	}
	if err := commandError(helloResponse); err != nil {
		return nil, fmt.Errorf("MongoDB upstream hello: %w", err)
	}
	session.hello = helloResponse
	return session, nil
}

func (session *upstreamSession) command(ctx context.Context, document any) (bson.Raw, error) {
	raw, err := marshalDocument(document)
	if err != nil {
		return nil, err
	}
	message := makeOPMsg(session.nextID, 0, raw)
	session.nextID++
	if deadline, ok := ctx.Deadline(); ok {
		_ = session.connection.SetDeadline(deadline)
		defer session.connection.SetDeadline(time.Time{})
	}
	if _, err := session.connection.Write(message.raw); err != nil {
		return nil, err
	}
	response, err := readWireMessage(session.reader)
	if err != nil {
		return nil, err
	}
	if response.responseTo != message.requestID {
		return nil, fmt.Errorf("MongoDB responseTo %d does not match request %d", response.responseTo, message.requestID)
	}
	return responseDocument(response)
}

func (session *upstreamSession) authenticate(ctx context.Context, options config.MongoDBOptions) error {
	mechanism := options.AuthMechanism
	if mechanism == "" {
		discovery, err := session.command(ctx, bson.D{{Key: "hello", Value: 1}, {Key: "saslSupportedMechs", Value: authSource(options.UpstreamAuthSource) + "." + options.UpstreamUsername}, {Key: "$db", Value: "admin"}})
		if err != nil {
			return fmt.Errorf("MongoDB upstream authentication discovery: %w", err)
		}
		if err := commandError(discovery); err != nil {
			return fmt.Errorf("MongoDB upstream authentication discovery: %w", err)
		}
		mechanism = preferredMechanism(discovery)
	}
	client, err := newSCRAMClient(mechanism, options.UpstreamUsername, options.UpstreamPassword)
	if err != nil {
		return err
	}
	database := authSource(options.UpstreamAuthSource)
	response, err := session.command(ctx, bson.D{
		{Key: "saslStart", Value: 1},
		{Key: "mechanism", Value: mechanism},
		{Key: "payload", Value: bson.Binary{Subtype: 0, Data: client.first()}},
		{Key: "autoAuthorize", Value: 1},
		{Key: "$db", Value: database},
	})
	if err != nil {
		return fmt.Errorf("MongoDB upstream authentication: %w", err)
	}
	if err := commandError(response); err != nil {
		return fmt.Errorf("MongoDB upstream authentication: %w", err)
	}
	conversationID, ok := rawInt32(response.Lookup("conversationId"))
	if !ok {
		return errors.New("MongoDB upstream authentication omitted conversationId")
	}
	_, serverFirst, ok := response.Lookup("payload").BinaryOK()
	if !ok {
		return errors.New("MongoDB upstream authentication omitted SCRAM payload")
	}
	clientFinal, err := client.final(string(serverFirst))
	if err != nil {
		return fmt.Errorf("MongoDB upstream authentication: %w", err)
	}
	response, err = session.command(ctx, bson.D{
		{Key: "saslContinue", Value: 1},
		{Key: "conversationId", Value: conversationID},
		{Key: "payload", Value: bson.Binary{Subtype: 0, Data: clientFinal}},
		{Key: "$db", Value: database},
	})
	if err != nil {
		return fmt.Errorf("MongoDB upstream authentication: %w", err)
	}
	if err := commandError(response); err != nil {
		return fmt.Errorf("MongoDB upstream authentication: %w", err)
	}
	_, serverFinal, ok := response.Lookup("payload").BinaryOK()
	if !ok || client.verify(string(serverFinal)) != nil {
		return errors.New("MongoDB upstream SCRAM server signature did not verify")
	}
	if done, _ := response.Lookup("done").BooleanOK(); !done {
		response, err = session.command(ctx, bson.D{
			{Key: "saslContinue", Value: 1},
			{Key: "conversationId", Value: conversationID},
			{Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte{}}},
			{Key: "$db", Value: database},
		})
		if err != nil {
			return fmt.Errorf("MongoDB upstream authentication: %w", err)
		}
		if err := commandError(response); err != nil {
			return fmt.Errorf("MongoDB upstream authentication: %w", err)
		}
	}
	return nil
}

func preferredMechanism(hello bson.Raw) string {
	if array, ok := hello.Lookup("saslSupportedMechs").ArrayOK(); ok {
		values, _ := array.Values()
		for _, value := range values {
			if mechanism, ok := value.StringValueOK(); ok && mechanism == "SCRAM-SHA-256" {
				return mechanism
			}
		}
		for _, value := range values {
			if mechanism, ok := value.StringValueOK(); ok && mechanism == "SCRAM-SHA-1" {
				return mechanism
			}
		}
	}
	return "SCRAM-SHA-256"
}

func commandError(document bson.Raw) error {
	if ok, found := rawNumber(document.Lookup("ok")); found && ok != 0 {
		if writeErrors, exists := document.Lookup("writeErrors").ArrayOK(); exists {
			values, _ := writeErrors.Values()
			if len(values) > 0 {
				return fmt.Errorf("MongoDB command reported %d write errors", len(values))
			}
		}
		if writeConcern, exists := document.Lookup("writeConcernError").DocumentOK(); exists {
			message, _ := writeConcern.Lookup("errmsg").StringValueOK()
			if message == "" {
				message = "MongoDB write concern failed"
			}
			return errors.New(message)
		}
		return nil
	}
	message, _ := document.Lookup("errmsg").StringValueOK()
	if message == "" {
		message = "MongoDB command failed"
	}
	if code, found := rawNumber(document.Lookup("code")); found {
		return fmt.Errorf("%s (code %d)", message, int64(code))
	}
	return errors.New(message)
}

func rawNumber(value bson.RawValue) (float64, bool) {
	if number, ok := value.Int32OK(); ok {
		return float64(number), true
	}
	if number, ok := value.Int64OK(); ok {
		return float64(number), true
	}
	if number, ok := value.DoubleOK(); ok {
		return number, true
	}
	return 0, false
}

func rawInt32(value bson.RawValue) (int32, bool) {
	if number, ok := value.Int32OK(); ok {
		return number, true
	}
	if number, ok := value.Int64OK(); ok && number >= -1<<31 && number <= 1<<31-1 {
		return int32(number), true
	}
	return 0, false
}

func authSource(value string) string {
	if strings.TrimSpace(value) == "" {
		return "admin"
	}
	return value
}
