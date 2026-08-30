package mongoadapter

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestUpstreamDirectTLSVerifiesCertificate(t *testing.T) {
	serverTLS, caPath, _, _ := mongoTestTLSCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		secure := tls.Server(connection, serverTLS)
		message, err := readWireMessage(bufio.NewReader(secure))
		if err != nil {
			serverDone <- err
			return
		}
		hello := mustDocument(bson.D{{Key: "isWritablePrimary", Value: true}, {Key: "maxWireVersion", Value: 25}, {Key: "ok", Value: 1}})
		_, err = secure.Write(makeCommandReply(message, 1, hello).raw)
		serverDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := config.Upstream{Target: listener.Addr().String(), MongoDB: &config.MongoDBOptions{UpstreamTLS: config.ClientTLSOptions{Enabled: true, CAFile: caPath}}}
	session, err := openUpstream(ctx, upstream)
	if err != nil {
		t.Fatal(err)
	}
	_ = session.connection.Close()
	if !session.tls {
		t.Fatal("MongoDB upstream session did not record TLS")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestListenerDirectTLSWithOfficialDriver(t *testing.T) {
	_, caPath, certPath, keyPath := mongoTestTLSCertificate(t)
	upstreamAddress, _, stopUpstream := startFakeMongo(t)
	defer stopUpstream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	upstream := config.Upstream{
		ID: "mongo-tls", Name: "Mongo TLS", Protocol: "mongodb", ListenAddr: "127.0.0.1:0", Target: upstreamAddress, Enabled: true,
		ListenerTLS: &config.ListenerTLSOptions{Enabled: true, CertFile: certPath, KeyFile: keyPath}, MongoDB: &config.MongoDBOptions{},
	}
	go func() {
		_ = New().Run(ctx, upstream, testSink{events: make(chan observation.Interaction, 50)}, func(address string) { ready <- address })
	}()
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("failed to load MongoDB test CA")
	}
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + <-ready + "/?directConnection=true").SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatal(err)
	}
}

func mongoTestTLSCertificate(t *testing.T) (*tls.Config, string, string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Portscope MongoDB test"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	caPath := filepath.Join(directory, "mongo-ca.pem")
	certPath := filepath.Join(directory, "mongo-cert.pem")
	keyPath := filepath.Join(directory, "mongo-key.pem")
	for path, data := range map[string][]byte{caPath: certPEM, certPath: certPEM, keyPath: keyPEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}, caPath, certPath, keyPath
}
