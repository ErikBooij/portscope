package tlsutil

import (
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
)

func TestMutualTLSConfigurationsCompleteAHandshake(t *testing.T) {
	directory := t.TempDir()
	ca, caKey, caPath := makeCA(t, directory)
	serverCert, serverKey := makeLeaf(t, directory, ca, caKey, "server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := makeLeaf(t, directory, ca, caKey, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	serverConfig, err := Server(config.ListenerTLSOptions{Enabled: true, CertFile: serverCert, KeyFile: serverKey, ClientCAFile: caPath, RequireClientCert: true})
	if err != nil {
		t.Fatal(err)
	}
	clientConfig, err := Client(config.ClientTLSOptions{Enabled: true, CAFile: caPath, CertFile: clientCert, KeyFile: clientKey}, "server.test")
	if err != nil {
		t.Fatal(err)
	}
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()
	server := tls.Server(serverSide, serverConfig)
	client := tls.Client(clientSide, clientConfig)
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.HandshakeContext(context.Background()) }()
	if err := client.HandshakeContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
	if len(server.ConnectionState().PeerCertificates) != 1 || len(client.ConnectionState().PeerCertificates) != 1 {
		t.Fatal("mutual TLS peers were not both verified")
	}
	if client.ConnectionState().Version < tls.VersionTLS12 {
		t.Fatalf("negotiated obsolete TLS version %x", client.ConnectionState().Version)
	}
}

func makeCA(t *testing.T, directory string) (*x509.Certificate, *rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Portscope test CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "ca.pem")
	writePEM(t, path, "CERTIFICATE", der)
	return certificate, key, path
}

func makeLeaf(t *testing.T, directory string, ca *x509.Certificate, caKey *rsa.PrivateKey, name string, usages []x509.ExtKeyUsage) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(int64(len(name) + 10)), Subject: pkix.Name{CommonName: name + ".test"}, DNSNames: []string{name + ".test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: usages}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(directory, name+".pem")
	keyPath := filepath.Join(directory, name+"-key.pem")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: data}), 0o600); err != nil {
		t.Fatal(err)
	}
}
