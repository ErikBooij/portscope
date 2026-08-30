package postgresadapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
)

type upstreamSession struct {
	connection net.Conn
	reader     *bufio.Reader
	tls        bool
	startup    []message
	pid        int32
	secret     int32
}

func authenticateListener(reader *bufio.Reader, writer io.Writer, username, password string) error {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return err
	}
	if err := writeMessage(writer, authenticationMessage(10, []byte("SCRAM-SHA-256\x00\x00"))); err != nil {
		return err
	}
	initial, err := readMessage(reader)
	if err != nil {
		return err
	}
	if len(initial.body) > maxStartupSize {
		return errors.New("PostgreSQL SASL initial response exceeds 64 KiB")
	}
	if initial.typ != 'p' {
		return errors.New("expected PostgreSQL SASLInitialResponse")
	}
	offset := 0
	mechanism, err := readCString(initial.body, &offset)
	if err != nil || mechanism != "SCRAM-SHA-256" {
		return errors.New("PostgreSQL client did not select SCRAM-SHA-256")
	}
	length, err := int32At(initial.body, offset)
	offset += 4
	if err != nil || length < 0 || int(length) != len(initial.body)-offset {
		return errors.New("invalid PostgreSQL SASL initial response")
	}
	clientFirst := string(initial.body[offset:])
	firstComma := strings.IndexByte(clientFirst, ',')
	secondComma := -1
	if firstComma >= 0 {
		if relative := strings.IndexByte(clientFirst[firstComma+1:], ','); relative >= 0 {
			secondComma = firstComma + 1 + relative
		}
	}
	if secondComma < 0 {
		return errors.New("unsupported PostgreSQL SCRAM channel binding")
	}
	gs2Header := clientFirst[:secondComma+1]
	if gs2Header != "n,," && gs2Header != "y,," {
		return errors.New("unsupported PostgreSQL SCRAM channel binding")
	}
	clientFirstBare := clientFirst[secondComma+1:]
	attributes, err := parseSCRAMAttributes(clientFirstBare)
	clientUsername := unescapeSCRAMName(attributes["n"])
	if err != nil || attributes["m"] != "" || attributes["r"] == "" || strings.Contains(attributes["r"], ",") || (clientUsername != "" && clientUsername != username) {
		return errors.New("invalid PostgreSQL SCRAM client-first message")
	}
	serverNonce, err := randomNonce()
	if err != nil {
		return err
	}
	combinedNonce := attributes["r"] + serverNonce
	const iterations = 4096
	serverFirst := "r=" + combinedNonce + ",s=" + base64.StdEncoding.EncodeToString(salt) + ",i=" + strconv.Itoa(iterations)
	if err := writeMessage(writer, authenticationMessage(11, []byte(serverFirst))); err != nil {
		return err
	}
	final, err := readMessage(reader)
	if err != nil {
		return err
	}
	if len(final.body) > maxStartupSize {
		return errors.New("PostgreSQL SASL final response exceeds 64 KiB")
	}
	if final.typ != 'p' {
		return errors.New("expected PostgreSQL SASLResponse")
	}
	clientFinal := string(final.body)
	finalAttributes, err := parseSCRAMAttributes(clientFinal)
	expectedBinding := base64.StdEncoding.EncodeToString([]byte(gs2Header))
	if err != nil || finalAttributes["c"] != expectedBinding || finalAttributes["r"] != combinedNonce || finalAttributes["p"] == "" {
		return errors.New("invalid PostgreSQL SCRAM client-final message")
	}
	proofIndex := strings.LastIndex(clientFinal, ",p=")
	if proofIndex < 0 {
		return errors.New("invalid PostgreSQL SCRAM proof")
	}
	clientFinalWithoutProof := clientFinal[:proofIndex]
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	proof, err := base64.StdEncoding.DecodeString(finalAttributes["p"])
	if err != nil || len(proof) != sha256.Size {
		return errors.New("invalid PostgreSQL SCRAM proof encoding")
	}
	saltedPassword := pbkdf2SHA256([]byte(password), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))
	recoveredKey := xor(proof, clientSignature)
	recoveredStored := sha256.Sum256(recoveredKey)
	if !hmac.Equal(recoveredStored[:], storedKey[:]) {
		return errors.New("invalid PostgreSQL listener credentials")
	}
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	serverSignature := hmacSHA256(serverKey, []byte(authMessage))
	return writeMessage(writer, authenticationMessage(12, []byte("v="+base64.StdEncoding.EncodeToString(serverSignature))))
}

func openUpstream(ctx context.Context, upstream config.Upstream, startup map[string]string) (*upstreamSession, error) {
	options := *upstream.Postgres
	dialer := net.Dialer{Timeout: 7 * time.Second, KeepAlive: 30 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", upstream.Target)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*upstreamSession, error) { _ = connection.Close(); return nil, err }
	_ = connection.SetDeadline(time.Now().Add(20 * time.Second))
	usingTLS := false
	if options.UpstreamTLS.Enabled {
		if err := writeStartup(connection, appendInt32(nil, sslRequestCode)); err != nil {
			return fail(err)
		}
		var response [1]byte
		if _, err := io.ReadFull(connection, response[:]); err != nil {
			return fail(err)
		}
		if response[0] != 'S' {
			return fail(errors.New("PostgreSQL upstream refused TLS"))
		}
		host, _, _ := net.SplitHostPort(upstream.Target)
		tlsConfig, err := tlsutil.Client(options.UpstreamTLS, host)
		if err != nil {
			return fail(err)
		}
		tlsConfig = tlsConfig.Clone()
		tlsConfig.NextProtos = []string{"postgresql"}
		secure := tls.Client(connection, tlsConfig)
		if err := secure.HandshakeContext(ctx); err != nil {
			return fail(fmt.Errorf("PostgreSQL upstream TLS: %w", err))
		}
		connection = secure
		usingTLS = true
	}
	if err := writeStartup(connection, buildStartup(startup, options.UpstreamUsername, options.Database)); err != nil {
		return fail(err)
	}
	session := &upstreamSession{connection: connection, reader: bufio.NewReader(connection), tls: usingTLS}
	if err := authenticateUpstream(options, session); err != nil {
		return fail(err)
	}
	_ = connection.SetDeadline(time.Time{})
	return session, nil
}

func authenticateUpstream(options config.PostgresOptions, session *upstreamSession) error {
	var scram *scramClient
	for {
		item, err := readMessage(session.reader)
		if err != nil {
			return err
		}
		if item.typ == 'E' {
			severity, code, text := parseError(item.body)
			return fmt.Errorf("PostgreSQL %s %s: %s", severity, code, text)
		}
		if len(item.body) > maxStartupSize && (item.typ == 'R' || scram != nil) {
			return errors.New("PostgreSQL upstream authentication response exceeds 64 KiB")
		}
		if item.typ != 'R' {
			if item.typ == 'K' && len(item.body) == 8 {
				session.pid, _ = int32At(item.body, 0)
				session.secret, _ = int32At(item.body, 4)
			}
			session.startup = append(session.startup, item)
			if item.typ == 'Z' {
				return nil
			}
			continue
		}
		code, err := int32At(item.body, 0)
		if err != nil {
			return errors.New("invalid PostgreSQL authentication request")
		}
		switch code {
		case 0:
			// AuthenticationOk is replaced with the listener's AuthenticationOk.
		case 3:
			if err := writeMessage(session.connection, message{typ: 'p', body: append([]byte(options.UpstreamPassword), 0)}); err != nil {
				return err
			}
		case 5:
			if len(item.body) != 8 {
				return errors.New("invalid PostgreSQL MD5 authentication request")
			}
			response := postgresMD5Password(options.UpstreamUsername, options.UpstreamPassword, item.body[4:8])
			if err := writeMessage(session.connection, message{typ: 'p', body: append([]byte(response), 0)}); err != nil {
				return err
			}
		case 10:
			mechanisms := bytes.Split(item.body[4:], []byte{0})
			supported := false
			for _, mechanism := range mechanisms {
				if string(mechanism) == "SCRAM-SHA-256" {
					supported = true
					break
				}
			}
			if !supported {
				return errors.New("PostgreSQL upstream does not offer SCRAM-SHA-256")
			}
			scram, err = newSCRAMClient(options.UpstreamUsername, options.UpstreamPassword)
			if err != nil {
				return err
			}
			initial := append([]byte("SCRAM-SHA-256\x00"), appendInt32(nil, int32(len(scram.clientFirst)))...)
			initial = append(initial, scram.clientFirst...)
			if err := writeMessage(session.connection, message{typ: 'p', body: initial}); err != nil {
				return err
			}
		case 11:
			if scram == nil {
				return errors.New("unexpected PostgreSQL SASL continuation")
			}
			response, err := scram.continueWith(string(item.body[4:]))
			if err != nil {
				return err
			}
			if err := writeMessage(session.connection, message{typ: 'p', body: []byte(response)}); err != nil {
				return err
			}
		case 12:
			if scram == nil || !scram.verify(string(item.body[4:])) {
				return errors.New("PostgreSQL upstream SCRAM server signature is invalid")
			}
		default:
			return fmt.Errorf("unsupported PostgreSQL authentication method %d", code)
		}
	}
}

type scramClient struct {
	username            string
	password            string
	nonce               string
	clientFirstBare     string
	clientFirst         string
	expectedServerProof []byte
}

func newSCRAMClient(username, password string) (*scramClient, error) {
	nonce, err := randomNonce()
	if err != nil {
		return nil, err
	}
	bare := "n=,r=" + nonce
	return &scramClient{username: username, password: password, nonce: nonce, clientFirstBare: bare, clientFirst: "n,," + bare}, nil
}

func (client *scramClient) continueWith(serverFirst string) (string, error) {
	attributes, err := parseSCRAMAttributes(serverFirst)
	if err != nil || attributes["m"] != "" || !strings.HasPrefix(attributes["r"], client.nonce) || attributes["r"] == client.nonce || strings.Contains(attributes["r"], ",") {
		return "", errors.New("invalid PostgreSQL SCRAM server nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(attributes["s"])
	if err != nil || len(salt) == 0 || len(salt) > 1024 {
		return "", errors.New("invalid PostgreSQL SCRAM salt")
	}
	iterations, err := strconv.Atoi(attributes["i"])
	if err != nil || iterations < 4096 || iterations > 1_000_000 {
		return "", errors.New("unsafe PostgreSQL SCRAM iteration count")
	}
	withoutProof := "c=biws,r=" + attributes["r"]
	authMessage := client.clientFirstBare + "," + serverFirst + "," + withoutProof
	saltedPassword := pbkdf2SHA256([]byte(client.password), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))
	proof := xor(clientKey, clientSignature)
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	client.expectedServerProof = hmacSHA256(serverKey, []byte(authMessage))
	return withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
}

func (client *scramClient) verify(serverFinal string) bool {
	attributes, err := parseSCRAMAttributes(serverFinal)
	if err != nil || attributes["e"] != "" {
		return false
	}
	proof, err := base64.StdEncoding.DecodeString(attributes["v"])
	return err == nil && hmac.Equal(proof, client.expectedServerProof)
}

func postgresMD5Password(username, password string, salt []byte) string {
	first := md5.Sum([]byte(password + username))
	second := md5.Sum(append([]byte(hex.EncodeToString(first[:])), salt...))
	return "md5" + hex.EncodeToString(second[:])
}

func parseSCRAMAttributes(value string) (map[string]string, error) {
	result := make(map[string]string)
	for _, part := range strings.Split(value, ",") {
		if len(part) < 2 || part[1] != '=' {
			return nil, errors.New("invalid SCRAM attribute")
		}
		name := part[:1]
		if _, exists := result[name]; exists {
			return nil, errors.New("duplicate SCRAM attribute")
		}
		result[name] = part[2:]
	}
	return result, nil
}

func randomNonce() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(value), nil
}

func escapeSCRAMName(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "=", "=3D"), ",", "=2C")
}

func unescapeSCRAMName(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "=2C", ","), "=3D", "=")
}

func hmacSHA256(key, value []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(value)
	return mac.Sum(nil)
}

func xor(left, right []byte) []byte {
	result := make([]byte, min(len(left), len(right)))
	for index := range result {
		result[index] = left[index] ^ right[index]
	}
	return result
}

func pbkdf2SHA256(password, salt []byte, iterations, length int) []byte {
	result := make([]byte, 0, length)
	for block := uint32(1); len(result) < length; block++ {
		input := append(append([]byte(nil), salt...), byte(block>>24), byte(block>>16), byte(block>>8), byte(block))
		u := hmacSHA256(password, input)
		t := append([]byte(nil), u...)
		for iteration := 1; iteration < iterations; iteration++ {
			u = hmacSHA256(password, u)
			for index := range t {
				t[index] ^= u[index]
			}
		}
		result = append(result, t...)
	}
	return result[:length]
}
