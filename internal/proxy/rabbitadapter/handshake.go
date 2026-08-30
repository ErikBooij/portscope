package rabbitadapter

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
)

type negotiatedConnection struct {
	upstream      net.Conn
	frameMax      uint32
	channelMax    uint16
	heartbeat     uint16
	downstreamTLS bool
	upstreamTLS   bool
	serverProduct string
	serverVersion string
}

type startFields struct {
	major, minor byte
	properties   []byte
	mechanisms   string
	locales      string
}

type startOKFields struct {
	properties []byte
	mechanism  string
	response   []byte
	locale     string
}

type tuneFields struct {
	channelMax uint16
	frameMax   uint32
	heartbeat  uint16
}

func negotiate(ctx context.Context, downstream net.Conn, upstream config.Upstream) (_ negotiatedConnection, err error) {
	options := upstream.RabbitMQ
	if options == nil {
		return negotiatedConnection{}, errors.New("RabbitMQ settings are required")
	}
	deadline := time.Now().Add(15 * time.Second)
	_ = downstream.SetDeadline(deadline)
	defer downstream.SetDeadline(time.Time{})
	header := make([]byte, len(protocolHeader))
	if _, err := io.ReadFull(downstream, header); err != nil {
		return negotiatedConnection{}, err
	}
	if !bytes.Equal(header, protocolHeader) {
		return negotiatedConnection{}, errors.New("client did not request AMQP 0-9-1")
	}

	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	upstreamConnection, err := dialer.DialContext(ctx, "tcp", upstream.Target)
	if err != nil {
		return negotiatedConnection{}, fmt.Errorf("dial RabbitMQ upstream %s: %w", upstream.Target, err)
	}
	defer func() {
		if err != nil {
			_ = upstreamConnection.Close()
		}
	}()
	upstreamTLS := false
	if options.UpstreamTLS.Enabled {
		host, _, _ := net.SplitHostPort(upstream.Target)
		tlsConfig, tlsErr := tlsutil.Client(options.UpstreamTLS, host)
		if tlsErr != nil {
			return negotiatedConnection{}, tlsErr
		}
		secure := tls.Client(upstreamConnection, tlsConfig)
		if tlsErr := secure.HandshakeContext(ctx); tlsErr != nil {
			return negotiatedConnection{}, fmt.Errorf("RabbitMQ upstream TLS: %w", tlsErr)
		}
		upstreamConnection = secure
		upstreamTLS = true
	}
	_ = upstreamConnection.SetDeadline(deadline)
	defer upstreamConnection.SetDeadline(time.Time{})
	if err := writeAll(upstreamConnection, protocolHeader); err != nil {
		return negotiatedConnection{}, err
	}
	serverStartFrame, err := readFrame(upstreamConnection, 0)
	if err != nil {
		return negotiatedConnection{}, fmt.Errorf("RabbitMQ upstream start: %w", err)
	}
	serverStart, err := parseStart(serverStartFrame)
	if err != nil {
		return negotiatedConnection{}, err
	}
	if !containsWord(serverStart.mechanisms, "PLAIN") {
		return negotiatedConnection{}, errors.New("RabbitMQ upstream does not offer PLAIN authentication")
	}
	locale := preferredLocale(serverStart.locales)
	listenerStart, err := buildStart(serverStart, "PLAIN", locale)
	if err != nil || writeAll(downstream, listenerStart.raw) != nil {
		return negotiatedConnection{}, errors.New("write RabbitMQ listener start")
	}
	clientStartOKFrame, err := readFrame(downstream, 0)
	if err != nil {
		return negotiatedConnection{}, err
	}
	clientStartOK, err := parseStartOK(clientStartOKFrame)
	if err != nil {
		return negotiatedConnection{}, err
	}
	if clientStartOK.mechanism != "PLAIN" || !validPlainResponse(clientStartOK.response, options.ListenerUsername, options.ListenerPassword) {
		return negotiatedConnection{}, errors.New("invalid RabbitMQ listener credentials")
	}
	upstreamStartOK, err := buildStartOK(clientStartOK.properties, "PLAIN", plainResponse(options.UpstreamUsername, options.UpstreamPassword), locale)
	if err != nil || writeAll(upstreamConnection, upstreamStartOK.raw) != nil {
		return negotiatedConnection{}, errors.New("write RabbitMQ upstream start-ok")
	}
	serverTuneFrame, err := readFrame(upstreamConnection, 0)
	if err != nil {
		return negotiatedConnection{}, fmt.Errorf("RabbitMQ upstream authentication: %w", err)
	}
	serverTune, err := parseTune(serverTuneFrame, 30)
	if err != nil {
		if classID, methodID, ok := methodID(serverTuneFrame); ok && classID == 10 && methodID == 50 {
			return negotiatedConnection{}, fmt.Errorf("RabbitMQ upstream authentication failed: %s", closeReason(serverTuneFrame))
		}
		return negotiatedConnection{}, err
	}
	if err := writeAll(downstream, serverTuneFrame.raw); err != nil {
		return negotiatedConnection{}, err
	}
	clientTuneFrame, err := readFrame(downstream, 0)
	if err != nil {
		return negotiatedConnection{}, err
	}
	clientTune, err := parseTune(clientTuneFrame, 31)
	if err != nil {
		return negotiatedConnection{}, err
	}
	if err := validateTune(serverTune, clientTune); err != nil {
		return negotiatedConnection{}, err
	}
	if err := writeAll(upstreamConnection, clientTuneFrame.raw); err != nil {
		return negotiatedConnection{}, err
	}
	clientOpenFrame, err := readFrame(downstream, clientTune.frameMax)
	if err != nil {
		return negotiatedConnection{}, err
	}
	listenerVHost, err := parseOpen(clientOpenFrame)
	if err != nil || listenerVHost != options.ListenerVHost {
		return negotiatedConnection{}, errors.New("RabbitMQ listener virtual host does not match configuration")
	}
	upstreamOpen, err := buildOpen(options.UpstreamVHost)
	if err != nil || writeAll(upstreamConnection, upstreamOpen.raw) != nil {
		return negotiatedConnection{}, errors.New("write RabbitMQ upstream open")
	}
	serverOpenOK, err := readFrame(upstreamConnection, clientTune.frameMax)
	if err != nil {
		return negotiatedConnection{}, fmt.Errorf("RabbitMQ upstream open: %w", err)
	}
	classID, methodID, ok := methodID(serverOpenOK)
	if !ok || classID != 10 || methodID != 41 {
		if ok && classID == 10 && methodID == 50 {
			return negotiatedConnection{}, fmt.Errorf("RabbitMQ upstream open failed: %s", closeReason(serverOpenOK))
		}
		return negotiatedConnection{}, errors.New("RabbitMQ upstream did not send connection.open-ok")
	}
	if err := writeAll(downstream, serverOpenOK.raw); err != nil {
		return negotiatedConnection{}, err
	}
	properties, _ := decodeTable(serverStart.properties)
	result := negotiatedConnection{upstream: upstreamConnection, frameMax: clientTune.frameMax, channelMax: clientTune.channelMax, heartbeat: clientTune.heartbeat, downstreamTLS: isTLSConnection(downstream), upstreamTLS: upstreamTLS}
	result.serverProduct, _ = properties["product"].(string)
	result.serverVersion, _ = properties["version"].(string)
	return result, nil
}

func parseStart(frame amqpFrame) (startFields, error) {
	classID, method, ok := methodID(frame)
	if !ok || frame.channel != 0 || classID != 10 || method != 10 {
		return startFields{}, errors.New("expected RabbitMQ connection.start")
	}
	cursor := newCursor(frame.payload[4:])
	major, err := cursor.octet()
	if err != nil {
		return startFields{}, err
	}
	minor, err := cursor.octet()
	if err != nil {
		return startFields{}, err
	}
	properties, err := cursor.tableRaw()
	if err != nil {
		return startFields{}, err
	}
	mechanisms, err := cursor.longstr()
	if err != nil {
		return startFields{}, err
	}
	locales, err := cursor.longstr()
	if err != nil || cursor.remaining() != 0 {
		return startFields{}, errors.New("invalid RabbitMQ connection.start")
	}
	return startFields{major: major, minor: minor, properties: properties, mechanisms: string(mechanisms), locales: string(locales)}, nil
}

func buildStart(source startFields, mechanisms, locales string) (amqpFrame, error) {
	var arguments bytes.Buffer
	arguments.WriteByte(source.major)
	arguments.WriteByte(source.minor)
	arguments.Write(source.properties)
	if err := writeLongstr(&arguments, []byte(mechanisms)); err != nil {
		return amqpFrame{}, err
	}
	if err := writeLongstr(&arguments, []byte(locales)); err != nil {
		return amqpFrame{}, err
	}
	return methodFrame(0, 10, 10, arguments.Bytes()), nil
}

func parseStartOK(frame amqpFrame) (startOKFields, error) {
	classID, method, ok := methodID(frame)
	if !ok || frame.channel != 0 || classID != 10 || method != 11 {
		return startOKFields{}, errors.New("expected RabbitMQ connection.start-ok")
	}
	cursor := newCursor(frame.payload[4:])
	properties, err := cursor.tableRaw()
	if err != nil {
		return startOKFields{}, err
	}
	mechanism, err := cursor.shortstr()
	if err != nil {
		return startOKFields{}, err
	}
	response, err := cursor.longstr()
	if err != nil {
		return startOKFields{}, err
	}
	locale, err := cursor.shortstr()
	if err != nil || cursor.remaining() != 0 {
		return startOKFields{}, errors.New("invalid RabbitMQ connection.start-ok")
	}
	return startOKFields{properties: properties, mechanism: mechanism, response: response, locale: locale}, nil
}

func buildStartOK(properties []byte, mechanism string, response []byte, locale string) (amqpFrame, error) {
	var arguments bytes.Buffer
	arguments.Write(properties)
	if err := writeShortstr(&arguments, mechanism); err != nil {
		return amqpFrame{}, err
	}
	if err := writeLongstr(&arguments, response); err != nil {
		return amqpFrame{}, err
	}
	if err := writeShortstr(&arguments, locale); err != nil {
		return amqpFrame{}, err
	}
	return methodFrame(0, 10, 11, arguments.Bytes()), nil
}

func parseTune(frame amqpFrame, expectedMethod uint16) (tuneFields, error) {
	classID, method, ok := methodID(frame)
	if !ok || frame.channel != 0 || classID != 10 || method != expectedMethod || len(frame.payload) != 12 {
		return tuneFields{}, fmt.Errorf("expected RabbitMQ connection tune method %d", expectedMethod)
	}
	return tuneFields{channelMax: binary.BigEndian.Uint16(frame.payload[4:6]), frameMax: binary.BigEndian.Uint32(frame.payload[6:10]), heartbeat: binary.BigEndian.Uint16(frame.payload[10:12])}, nil
}

func validateTune(server, client tuneFields) error {
	if client.frameMax != 0 && client.frameMax < frameMin {
		return errors.New("RabbitMQ frame maximum is below 4096")
	}
	if server.frameMax != 0 && (client.frameMax == 0 || client.frameMax > server.frameMax) {
		return errors.New("RabbitMQ client frame maximum exceeds the broker limit")
	}
	if server.channelMax != 0 && (client.channelMax == 0 || client.channelMax > server.channelMax) {
		return errors.New("RabbitMQ client channel maximum exceeds the broker limit")
	}
	return nil
}

func parseOpen(frame amqpFrame) (string, error) {
	classID, method, ok := methodID(frame)
	if !ok || frame.channel != 0 || classID != 10 || method != 40 {
		return "", errors.New("expected RabbitMQ connection.open")
	}
	cursor := newCursor(frame.payload[4:])
	vhost, err := cursor.shortstr()
	if err != nil {
		return "", err
	}
	if _, err := cursor.shortstr(); err != nil {
		return "", err
	}
	if _, err := cursor.octet(); err != nil || cursor.remaining() != 0 {
		return "", errors.New("invalid RabbitMQ connection.open")
	}
	return vhost, nil
}

func buildOpen(vhost string) (amqpFrame, error) {
	var arguments bytes.Buffer
	if err := writeShortstr(&arguments, vhost); err != nil {
		return amqpFrame{}, err
	}
	if err := writeShortstr(&arguments, ""); err != nil {
		return amqpFrame{}, err
	}
	arguments.WriteByte(0)
	return methodFrame(0, 10, 40, arguments.Bytes()), nil
}

func validPlainResponse(response []byte, username, password string) bool {
	expected := plainResponse(username, password)
	return subtle.ConstantTimeCompare(response, expected) == 1
}

func plainResponse(username, password string) []byte {
	return []byte("\x00" + username + "\x00" + password)
}

func containsWord(words, wanted string) bool {
	for _, word := range strings.Fields(words) {
		if word == wanted {
			return true
		}
	}
	return false
}

func preferredLocale(locales string) string {
	for _, locale := range strings.Fields(locales) {
		if locale == "en_US" {
			return locale
		}
	}
	fields := strings.Fields(locales)
	if len(fields) > 0 {
		return fields[0]
	}
	return "en_US"
}

func closeReason(frame amqpFrame) string {
	cursor := newCursor(frame.payload[4:])
	code, _ := cursor.short()
	message, _ := cursor.shortstr()
	return fmt.Sprintf("%d %s", code, message)
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		count, err := writer.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func isTLSConnection(connection net.Conn) bool {
	_, ok := connection.(*tls.Conn)
	return ok
}
