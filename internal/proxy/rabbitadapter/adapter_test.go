package rabbitadapter

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	amqp "github.com/rabbitmq/amqp091-go"
)

type testSink struct{ events chan observation.Interaction }

func (sink testSink) Record(item observation.Interaction) { sink.events <- item }

func TestProxyTerminatesPlainAndInspectsMessaging(t *testing.T) {
	brokerAddress, starts, stopBroker := startFakeBroker(t)
	defer stopBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan observation.Interaction, 100)
	ready := make(chan string, 1)
	go func() {
		_ = New().Run(ctx, rabbitTestUpstream(brokerAddress), testSink{events}, func(address string) { ready <- address })
	}()
	proxyAddress := <-ready

	connection, err := amqp.DialConfig(amqpURL(proxyAddress, "listener", "listener-secret", "local"), amqp.Config{Heartbeat: 0, Dial: amqp.DefaultDial(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	queue, err := channel.QueueDeclare("books", false, false, false, false, nil)
	if err != nil || queue.Name != "books" {
		t.Fatalf("declare queue = %#v, %v", queue, err)
	}
	body := []byte(`{"title":"The Dispossessed"}`)
	if err := channel.PublishWithContext(ctx, "", "books", false, false, amqp.Publishing{ContentType: "application/json", Headers: amqp.Table{"trace": "abc"}, Body: body}); err != nil {
		t.Fatal(err)
	}
	delivery, ok, err := channel.Get("books", true)
	if err != nil || !ok || !bytes.Equal(delivery.Body, body) {
		t.Fatalf("get = %#v, %t, %v", delivery, ok, err)
	}
	_ = channel.Close()
	_ = connection.Close()

	seen := waitForAMQPOperations(t, events, "CONNECT", "DECLARE QUEUE books", "PUBLISH  → books", "GET books")
	if starts.Load() != 1 {
		t.Fatalf("broker received %d start-ok frames", starts.Load())
	}
	publish := seen["PUBLISH  → books"]
	if publish.Request.Kind != "json" || string(publish.Request.JSON) != string(body) || headerValue(publish.Request.Headers, "header.trace") != "abc" {
		t.Fatalf("publish observation = %#v", publish)
	}
	get := seen["GET books"]
	if get.Response.Kind != "json" || get.Attributes["messageCount"] != "0" {
		t.Fatalf("get observation = %#v", get)
	}
	connect := seen["CONNECT"]
	if strings.Contains(string(connect.Request.JSON), "listener-secret") || !strings.Contains(string(connect.Request.JSON), "redacted") {
		t.Fatalf("credentials were captured: %s", connect.Request.JSON)
	}
}

func TestWrongListenerCredentialsNeverReachBrokerAuthentication(t *testing.T) {
	brokerAddress, starts, stopBroker := startFakeBroker(t)
	defer stopBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	go func() {
		_ = New().Run(ctx, rabbitTestUpstream(brokerAddress), testSink{make(chan observation.Interaction, 20)}, func(address string) { ready <- address })
	}()
	proxyAddress := <-ready
	if connection, err := amqp.DialConfig(amqpURL(proxyAddress, "listener", "wrong", "local"), amqp.Config{Heartbeat: 0, Dial: amqp.DefaultDial(time.Second)}); err == nil {
		_ = connection.Close()
		t.Fatal("wrong listener password was accepted")
	}
	time.Sleep(20 * time.Millisecond)
	if starts.Load() != 0 {
		t.Fatal("Portscope sent broker credentials before listener authentication succeeded")
	}
}

func TestDirectTLSOnBothRabbitMQLegs(t *testing.T) {
	certificate, roots, certificatePath, keyPath, caPath := rabbitTestCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	starts := &atomic.Int32{}
	go func() {
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go serveFakeBroker(ctx, connection, starts)
		}
	}()
	defer listener.Close()
	upstream := rabbitTestUpstream(listener.Addr().String())
	upstream.ListenerTLS = &config.ListenerTLSOptions{Enabled: true, CertFile: certificatePath, KeyFile: keyPath}
	upstream.RabbitMQ.UpstreamTLS = config.ClientTLSOptions{Enabled: true, CAFile: caPath, ServerName: "127.0.0.1"}
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 30)
	go func() { _ = New().Run(ctx, upstream, testSink{events}, func(address string) { ready <- address }) }()
	address := <-ready
	secureURL := strings.Replace(amqpURL(address, "listener", "listener-secret", "local"), "amqp://", "amqps://", 1)
	connection, err := amqp.DialConfig(secureURL, amqp.Config{Heartbeat: 0, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"}, Dial: amqp.DefaultDial(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	seen := waitForAMQPOperations(t, events, "CONNECT")
	if seen["CONNECT"].Attributes["downstreamTLS"] != "true" || seen["CONNECT"].Attributes["upstreamTLS"] != "true" || starts.Load() != 1 {
		t.Fatalf("TLS connection observation = %#v", seen["CONNECT"])
	}
}

func TestWireRejectsOversizedMalformedAndDeeplyNestedFrames(t *testing.T) {
	oversized := make([]byte, 7)
	oversized[0] = frameBody
	binary.BigEndian.PutUint32(oversized[3:], hardFrameMax)
	if _, err := readFrame(bytes.NewReader(oversized), 0); err == nil {
		t.Fatal("oversized frame was accepted")
	}
	malformed := makeFrame(frameHeartbeat, 1, nil)
	if _, err := readFrame(bytes.NewReader(malformed.raw), 0); err == nil {
		t.Fatal("heartbeat on a non-zero channel was accepted")
	}
	var array bytes.Buffer
	for range 18 {
		var next bytes.Buffer
		next.WriteByte('A')
		_ = binary.Write(&next, binary.BigEndian, uint32(array.Len()))
		next.Write(array.Bytes())
		array = next
	}
	var entry bytes.Buffer
	_ = writeShortstr(&entry, "nested")
	entry.Write(array.Bytes())
	var table bytes.Buffer
	_ = binary.Write(&table, binary.BigEndian, uint32(entry.Len()))
	table.Write(entry.Bytes())
	if _, err := decodeTable(table.Bytes()); err == nil {
		t.Fatal("overly nested field table was accepted")
	}
}

func rabbitTestUpstream(target string) config.Upstream {
	return config.Upstream{ID: "rabbit", Name: "Messages", Protocol: "rabbitmq", ListenAddr: "127.0.0.1:0", Target: target, Enabled: true, RabbitMQ: &config.RabbitMQOptions{ListenerUsername: "listener", ListenerPassword: "listener-secret", ListenerVHost: "local", UpstreamUsername: "broker-user", UpstreamPassword: "broker-secret", UpstreamVHost: "/broker"}}
}

func amqpURL(address, username, password, vhost string) string {
	return "amqp://" + url.PathEscape(username) + ":" + url.PathEscape(password) + "@" + address + "/" + url.PathEscape(strings.TrimPrefix(vhost, "/"))
}

func headerValue(headers []observation.Pair, name string) string {
	for _, header := range headers {
		if header.Name == name {
			return header.Value
		}
	}
	return ""
}

func waitForAMQPOperations(t *testing.T, events <-chan observation.Interaction, operations ...string) map[string]observation.Interaction {
	t.Helper()
	wanted := make(map[string]bool, len(operations))
	for _, operation := range operations {
		wanted[operation] = true
	}
	result := make(map[string]observation.Interaction, len(operations))
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for len(result) < len(wanted) {
		select {
		case item := <-events:
			if wanted[item.Operation] {
				result[item.Operation] = item
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for AMQP observations; got %#v", result)
		}
	}
	return result
}

func startFakeBroker(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	starts := &atomic.Int32{}
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go serveFakeBroker(ctx, connection, starts)
		}
	}()
	return listener.Addr().String(), starts, func() { cancel(); _ = listener.Close() }
}

func serveFakeBroker(ctx context.Context, connection net.Conn, starts *atomic.Int32) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
	header := make([]byte, len(protocolHeader))
	if _, err := io.ReadFull(connection, header); err != nil || !bytes.Equal(header, protocolHeader) {
		return
	}
	properties := encodeTestTable(map[string]string{"product": "RabbitMQ", "version": "4.test"})
	var start bytes.Buffer
	start.Write([]byte{0, 9})
	start.Write(properties)
	_ = writeLongstr(&start, []byte("PLAIN"))
	_ = writeLongstr(&start, []byte("en_US"))
	_ = writeAll(connection, methodFrame(0, 10, 10, start.Bytes()).raw)
	startOKFrame, err := readFrame(connection, 0)
	if err != nil {
		return
	}
	startOK, err := parseStartOK(startOKFrame)
	if err != nil || !validPlainResponse(startOK.response, "broker-user", "broker-secret") {
		return
	}
	starts.Add(1)
	var tune bytes.Buffer
	_ = binary.Write(&tune, binary.BigEndian, uint16(2047))
	_ = binary.Write(&tune, binary.BigEndian, uint32(131072))
	_ = binary.Write(&tune, binary.BigEndian, uint16(0))
	_ = writeAll(connection, methodFrame(0, 10, 30, tune.Bytes()).raw)
	if _, err := readFrame(connection, 0); err != nil {
		return
	}
	openFrame, err := readFrame(connection, 0)
	if err != nil {
		return
	}
	vhost, err := parseOpen(openFrame)
	if err != nil || vhost != "/broker" {
		return
	}
	_ = writeAll(connection, methodFrame(0, 10, 41, []byte{0}).raw)
	var emptyLongstr bytes.Buffer
	_ = writeLongstr(&emptyLongstr, nil)

	var published []byte
	for {
		frame, err := readFrame(connection, 131072)
		if err != nil {
			return
		}
		classID, methodID, ok := methodID(frame)
		if !ok {
			continue
		}
		switch {
		case classID == 20 && methodID == 10:
			_ = writeAll(connection, methodFrame(frame.channel, 20, 11, emptyLongstr.Bytes()).raw)
		case classID == 20 && methodID == 40:
			_ = writeAll(connection, methodFrame(frame.channel, 20, 41, nil).raw)
		case classID == 50 && methodID == 10:
			cursor := newCursor(frame.payload[4:])
			_, _ = cursor.short()
			queue, _ := cursor.shortstr()
			var arguments bytes.Buffer
			_ = writeShortstr(&arguments, queue)
			_ = binary.Write(&arguments, binary.BigEndian, uint32(0))
			_ = binary.Write(&arguments, binary.BigEndian, uint32(0))
			_ = writeAll(connection, methodFrame(frame.channel, 50, 11, arguments.Bytes()).raw)
		case classID == 60 && methodID == 40:
			header, headerErr := readFrame(connection, 131072)
			if headerErr != nil {
				return
			}
			bodySize, _, _, _ := inspectContentHeader(header)
			published = nil
			for uint64(len(published)) < bodySize {
				body, bodyErr := readFrame(connection, 131072)
				if bodyErr != nil {
					return
				}
				published = append(published, body.payload...)
			}
		case classID == 60 && methodID == 70:
			var arguments bytes.Buffer
			_ = binary.Write(&arguments, binary.BigEndian, uint64(1))
			arguments.WriteByte(0)
			_ = writeShortstr(&arguments, "")
			_ = writeShortstr(&arguments, "books")
			_ = binary.Write(&arguments, binary.BigEndian, uint32(0))
			_ = writeAll(connection, methodFrame(frame.channel, 60, 71, arguments.Bytes()).raw)
			_ = writeAll(connection, testContentHeader(frame.channel, published).raw)
			_ = writeAll(connection, makeFrame(frameBody, frame.channel, published).raw)
		case classID == 10 && methodID == 50:
			_ = writeAll(connection, methodFrame(0, 10, 51, nil).raw)
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func encodeTestTable(values map[string]string) []byte {
	var entries bytes.Buffer
	for name, value := range values {
		_ = writeShortstr(&entries, name)
		entries.WriteByte('S')
		_ = writeLongstr(&entries, []byte(value))
	}
	var result bytes.Buffer
	_ = binary.Write(&result, binary.BigEndian, uint32(entries.Len()))
	result.Write(entries.Bytes())
	return result.Bytes()
}

func testContentHeader(channel uint16, body []byte) amqpFrame {
	var payload bytes.Buffer
	_ = binary.Write(&payload, binary.BigEndian, uint16(60))
	_ = binary.Write(&payload, binary.BigEndian, uint16(0))
	_ = binary.Write(&payload, binary.BigEndian, uint64(len(body)))
	_ = binary.Write(&payload, binary.BigEndian, uint16(0x8000))
	_ = writeShortstr(&payload, "application/json")
	return makeFrame(frameHeader, channel, payload.Bytes())
}

func rabbitTestCertificate(t *testing.T) (tls.Certificate, *x509.CertPool, string, string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "127.0.0.1"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	directory := t.TempDir()
	certificatePath, keyPath, caPath := filepath.Join(directory, "server.pem"), filepath.Join(directory, "server-key.pem"), filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("append test CA")
	}
	return pair, roots, certificatePath, keyPath, caPath
}
