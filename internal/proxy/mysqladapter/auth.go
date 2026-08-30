package mysqladapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
)

const (
	clientLongPassword               uint32 = 1 << 0
	clientFoundRows                  uint32 = 1 << 1
	clientLongFlag                   uint32 = 1 << 2
	clientConnectWithDB              uint32 = 1 << 3
	clientCompress                   uint32 = 1 << 5
	clientLocalFiles                 uint32 = 1 << 7
	clientProtocol41                 uint32 = 1 << 9
	clientSSL                        uint32 = 1 << 11
	clientTransactions               uint32 = 1 << 13
	clientSecureConnection           uint32 = 1 << 15
	clientMultiStatements            uint32 = 1 << 16
	clientMultiResults               uint32 = 1 << 17
	clientPSMultiResults             uint32 = 1 << 18
	clientPluginAuth                 uint32 = 1 << 19
	clientConnectAttrs               uint32 = 1 << 20
	clientPluginAuthLenencClientData uint32 = 1 << 21
	clientSessionTrack               uint32 = 1 << 23
	clientDeprecateEOF               uint32 = 1 << 24
	clientOptionalResultsetMetadata  uint32 = 1 << 25
	clientZSTDCompression            uint32 = 1 << 26
	clientQueryAttributes            uint32 = 1 << 27

	serverStatusAutocommit  uint16 = 0x0002
	serverMoreResultsExists uint16 = 0x0008
)

const inspectableCapabilities = clientLongPassword | clientFoundRows | clientLongFlag | clientConnectWithDB |
	clientProtocol41 | clientTransactions | clientSecureConnection | clientMultiStatements | clientMultiResults |
	clientPSMultiResults | clientPluginAuth | clientPluginAuthLenencClientData | clientSessionTrack | clientDeprecateEOF

type greeting struct {
	serverVersion string
	connectionID  uint32
	capabilities  uint32
	scramble      []byte
	plugin        string
}

type clientHandshake struct {
	capabilities uint32
	username     string
	database     string
	plugin       string
	authResponse []byte
}

type authenticatedUpstream struct {
	connection   net.Conn
	reader       *bufio.Reader
	greeting     greeting
	capabilities uint32
	tls          bool
}

func makeGreeting(connectionID uint32, scramble []byte, capabilities uint32, allowTLS bool) []byte {
	capabilities &= inspectableCapabilities
	capabilities |= clientProtocol41 | clientSecureConnection | clientPluginAuth
	if allowTLS {
		capabilities |= clientSSL
	}
	payload := []byte{10}
	payload = append(payload, "8.0.0-portscope"...)
	payload = append(payload, 0)
	var id [4]byte
	binary.LittleEndian.PutUint32(id[:], connectionID)
	payload = append(payload, id[:]...)
	payload = append(payload, scramble[:8]...)
	payload = append(payload, 0, byte(capabilities), byte(capabilities>>8), 45, byte(serverStatusAutocommit), byte(serverStatusAutocommit>>8), byte(capabilities>>16), byte(capabilities>>24), 21)
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, scramble[8:20]...)
	payload = append(payload, 0)
	payload = append(payload, "mysql_native_password"...)
	payload = append(payload, 0)
	return payload
}

func parseGreeting(payload []byte) (greeting, error) {
	if len(payload) == 0 || payload[0] == 0xff {
		return greeting{}, errors.New("MySQL upstream rejected the connection before greeting")
	}
	if payload[0] != 10 {
		return greeting{}, fmt.Errorf("unsupported MySQL handshake protocol %d", payload[0])
	}
	offset := 1
	version, err := readNULTerminated(payload, &offset)
	if err != nil || len(payload)-offset < 15 {
		return greeting{}, errors.New("truncated MySQL v10 greeting")
	}
	connectionID := binary.LittleEndian.Uint32(payload[offset:])
	offset += 4
	scramble := append([]byte(nil), payload[offset:offset+8]...)
	offset += 9
	capabilities := uint32(binary.LittleEndian.Uint16(payload[offset:]))
	offset += 2
	if offset == len(payload) {
		return greeting{serverVersion: version, connectionID: connectionID, capabilities: capabilities, scramble: scramble, plugin: "mysql_native_password"}, nil
	}
	if len(payload)-offset < 16 {
		return greeting{}, errors.New("truncated MySQL v10 greeting capabilities")
	}
	offset += 3 // character set and status flags
	capabilities |= uint32(binary.LittleEndian.Uint16(payload[offset:])) << 16
	offset += 2
	authLength := int(payload[offset])
	offset += 11
	partLength := 13
	if authLength > 8 && authLength-8 < partLength {
		partLength = authLength - 8
	}
	if partLength < 0 {
		partLength = 0
	}
	if partLength > len(payload)-offset {
		partLength = len(payload) - offset
	}
	second := bytes.TrimSuffix(payload[offset:offset+partLength], []byte{0})
	scramble = append(scramble, second...)
	if len(scramble) > 20 {
		scramble = scramble[:20]
	}
	offset += partLength
	plugin := "mysql_native_password"
	if capabilities&clientPluginAuth != 0 && offset < len(payload) {
		end := bytes.IndexByte(payload[offset:], 0)
		if end < 0 {
			end = len(payload) - offset
		}
		if end > 0 {
			plugin = string(payload[offset : offset+end])
		}
	}
	return greeting{serverVersion: version, connectionID: connectionID, capabilities: capabilities, scramble: scramble, plugin: plugin}, nil
}

func parseClientHandshake(payload []byte) (clientHandshake, error) {
	if len(payload) < 32 {
		return clientHandshake{}, errors.New("truncated MySQL handshake response")
	}
	capabilities := binary.LittleEndian.Uint32(payload)
	if capabilities&clientProtocol41 == 0 {
		return clientHandshake{}, errors.New("pre-4.1 MySQL clients are not supported")
	}
	offset := 32
	username, err := readNULTerminated(payload, &offset)
	if err != nil {
		return clientHandshake{}, errors.New("invalid MySQL handshake username")
	}
	var auth []byte
	switch {
	case capabilities&clientPluginAuthLenencClientData != 0:
		value, null, readErr := readLenencBytes(payload, &offset)
		if readErr != nil || null {
			return clientHandshake{}, errors.New("invalid MySQL authentication response")
		}
		auth = append([]byte(nil), value...)
	case capabilities&clientSecureConnection != 0:
		if offset >= len(payload) {
			return clientHandshake{}, io.ErrUnexpectedEOF
		}
		length := int(payload[offset])
		offset++
		if length > len(payload)-offset {
			return clientHandshake{}, io.ErrUnexpectedEOF
		}
		auth = append([]byte(nil), payload[offset:offset+length]...)
		offset += length
	default:
		value, readErr := readNULTerminated(payload, &offset)
		if readErr != nil {
			return clientHandshake{}, readErr
		}
		auth = []byte(value)
	}
	database := ""
	if capabilities&clientConnectWithDB != 0 {
		database, err = readNULTerminated(payload, &offset)
		if err != nil {
			return clientHandshake{}, errors.New("invalid MySQL handshake database")
		}
	}
	plugin := "mysql_native_password"
	if capabilities&clientPluginAuth != 0 && offset < len(payload) {
		plugin, err = readNULTerminated(payload, &offset)
		if err != nil {
			return clientHandshake{}, errors.New("invalid MySQL authentication plugin")
		}
	}
	return clientHandshake{capabilities: capabilities, username: username, database: database, plugin: plugin, authResponse: auth}, nil
}

func buildHandshakeResponse(capabilities uint32, username, password, database, plugin string, scramble []byte) ([]byte, error) {
	auth, err := authenticationResponse(plugin, password, scramble)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 0, 128)
	var fixed [4]byte
	binary.LittleEndian.PutUint32(fixed[:], capabilities)
	payload = append(payload, fixed[:]...)
	binary.LittleEndian.PutUint32(fixed[:], 64<<20)
	payload = append(payload, fixed[:]...)
	payload = append(payload, 45)
	payload = append(payload, make([]byte, 23)...)
	payload = append(payload, username...)
	payload = append(payload, 0)
	if capabilities&clientPluginAuthLenencClientData != 0 {
		payload = appendLenenc(payload, uint64(len(auth)))
	} else {
		if len(auth) > 255 {
			return nil, errors.New("MySQL authentication response exceeds 255 bytes")
		}
		payload = append(payload, byte(len(auth)))
	}
	payload = append(payload, auth...)
	if capabilities&clientConnectWithDB != 0 {
		payload = append(payload, database...)
		payload = append(payload, 0)
	}
	if capabilities&clientPluginAuth != 0 {
		payload = append(payload, plugin...)
		payload = append(payload, 0)
	}
	return payload, nil
}

func appendLenenc(target []byte, value uint64) []byte {
	switch {
	case value < 251:
		return append(target, byte(value))
	case value < 1<<16:
		return append(target, 0xfc, byte(value), byte(value>>8))
	case value < 1<<24:
		return appendUint24(append(target, 0xfd), int(value))
	default:
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], value)
		return append(append(target, 0xfe), encoded[:]...)
	}
}

func authenticationResponse(plugin, password string, scramble []byte) ([]byte, error) {
	if password == "" {
		return nil, nil
	}
	switch plugin {
	case "mysql_native_password", "":
		stage1 := sha1.Sum([]byte(password))
		stage2 := sha1.Sum(stage1[:])
		input := append(append([]byte(nil), scramble...), stage2[:]...)
		digest := sha1.Sum(input)
		return xorBytes(stage1[:], digest[:]), nil
	case "caching_sha2_password":
		stage1 := sha256.Sum256([]byte(password))
		stage2 := sha256.Sum256(stage1[:])
		input := append(append([]byte(nil), stage2[:]...), scramble...)
		digest := sha256.Sum256(input)
		return xorBytes(stage1[:], digest[:]), nil
	default:
		return nil, fmt.Errorf("unsupported MySQL authentication plugin %q", plugin)
	}
}

func verifyNativePassword(response []byte, password string, scramble []byte) bool {
	expected, err := authenticationResponse("mysql_native_password", password, scramble)
	return err == nil && bytes.Equal(response, expected)
}

func xorBytes(left, right []byte) []byte {
	result := make([]byte, min(len(left), len(right)))
	for index := range result {
		result[index] = left[index] ^ right[index]
	}
	return result
}

func openUpstream(ctx context.Context, upstream config.Upstream) (*authenticatedUpstream, error) {
	dialer := &net.Dialer{Timeout: 7 * time.Second, KeepAlive: 30 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", upstream.Target)
	if err != nil {
		return nil, err
	}
	_ = connection.SetDeadline(time.Now().Add(15 * time.Second))
	reader := bufio.NewReader(connection)
	initial, err := readPacket(reader)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if initial.sequence != 0 {
		_ = connection.Close()
		return nil, fmt.Errorf("MySQL greeting sequence id %d, expected 0", initial.sequence)
	}
	serverGreeting, err := parseGreeting(initial.payload)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	return &authenticatedUpstream{connection: connection, reader: reader, greeting: serverGreeting}, nil
}

func authenticateUpstream(ctx context.Context, upstream config.Upstream, session *authenticatedUpstream, requested uint32) error {
	options := *upstream.MySQL
	fail := func(err error) error { _ = session.connection.Close(); return err }
	capabilities := session.greeting.capabilities & requested & inspectableCapabilities
	capabilities &^= clientCompress | clientZSTDCompression | clientLocalFiles | clientOptionalResultsetMetadata | clientQueryAttributes | clientConnectAttrs
	if options.Database != "" && session.greeting.capabilities&clientConnectWithDB != 0 {
		capabilities |= clientConnectWithDB
	} else {
		capabilities &^= clientConnectWithDB
	}
	sequence := byte(1)
	usingTLS := false
	if options.UpstreamTLS.Enabled {
		if session.greeting.capabilities&clientSSL == 0 {
			return fail(errors.New("MySQL upstream does not advertise TLS"))
		}
		capabilities |= clientSSL
		sslRequest := make([]byte, 32)
		binary.LittleEndian.PutUint32(sslRequest, capabilities)
		binary.LittleEndian.PutUint32(sslRequest[4:], 64<<20)
		sslRequest[8] = 45
		if err := writePacket(session.connection, packet{sequence: sequence, payload: sslRequest}); err != nil {
			return fail(err)
		}
		sequence++
		host, _, _ := net.SplitHostPort(upstream.Target)
		tlsConfig, configErr := tlsutil.Client(options.UpstreamTLS, host)
		if configErr != nil {
			return fail(configErr)
		}
		secure := tls.Client(session.connection, tlsConfig)
		if err := secure.HandshakeContext(ctx); err != nil {
			return fail(fmt.Errorf("MySQL upstream TLS: %w", err))
		}
		session.connection = secure
		session.reader = bufio.NewReader(secure)
		usingTLS = true
	}
	response, err := buildHandshakeResponse(capabilities, options.UpstreamUsername, options.UpstreamPassword, options.Database, session.greeting.plugin, session.greeting.scramble)
	if err != nil {
		return fail(err)
	}
	if err := writePacket(session.connection, packet{sequence: sequence, payload: response}); err != nil {
		return fail(err)
	}
	sequence++
	for {
		item, readErr := readPacket(session.reader)
		if readErr != nil {
			return fail(readErr)
		}
		if item.sequence != sequence {
			return fail(fmt.Errorf("MySQL auth sequence id %d, expected %d", item.sequence, sequence))
		}
		sequence++
		if len(item.payload) == 0 {
			return fail(errors.New("empty MySQL authentication packet"))
		}
		switch item.payload[0] {
		case 0x00:
			_ = session.connection.SetDeadline(time.Time{})
			session.capabilities, session.tls = capabilities, usingTLS
			return nil
		case 0xff:
			return fail(parseMySQLError(item.payload))
		case 0xfe:
			offset := 1
			plugin, parseErr := readNULTerminated(item.payload, &offset)
			if parseErr != nil {
				return fail(errors.New("invalid MySQL auth switch request"))
			}
			scramble := bytes.TrimSuffix(item.payload[offset:], []byte{0})
			auth, authErr := authenticationResponse(plugin, options.UpstreamPassword, scramble)
			if authErr != nil {
				return fail(authErr)
			}
			if err := writePacket(session.connection, packet{sequence: sequence, payload: auth}); err != nil {
				return fail(err)
			}
			sequence++
			session.greeting.plugin, session.greeting.scramble = plugin, append([]byte(nil), scramble...)
		case 0x01:
			if len(item.payload) >= 2 && item.payload[1] == 0x03 {
				continue
			}
			if len(item.payload) >= 2 && item.payload[1] == 0x04 {
				var auth []byte
				if usingTLS {
					auth = append([]byte(options.UpstreamPassword), 0)
				} else {
					if err := writePacket(session.connection, packet{sequence: sequence, payload: []byte{0x02}}); err != nil {
						return fail(err)
					}
					sequence++
					keyPacket, keyErr := readPacket(session.reader)
					if keyErr != nil {
						return fail(keyErr)
					}
					if keyPacket.sequence != sequence {
						return fail(fmt.Errorf("MySQL RSA key sequence id %d, expected %d", keyPacket.sequence, sequence))
					}
					sequence++
					keyData := keyPacket.payload
					if len(keyData) > 0 && keyData[0] == 0x01 {
						keyData = keyData[1:]
					}
					auth, keyErr = encryptPassword(options.UpstreamPassword, session.greeting.scramble, keyData)
					if keyErr != nil {
						return fail(keyErr)
					}
				}
				if err := writePacket(session.connection, packet{sequence: sequence, payload: auth}); err != nil {
					return fail(err)
				}
				sequence++
				continue
			}
			return fail(errors.New("unsupported MySQL authentication continuation"))
		default:
			return fail(fmt.Errorf("unexpected MySQL authentication packet 0x%02x", item.payload[0]))
		}
	}
}

func encryptPassword(password string, scramble, keyData []byte) ([]byte, error) {
	block, _ := pem.Decode(keyData)
	if block == nil {
		return nil, errors.New("MySQL upstream returned an invalid RSA public key")
	}
	var publicKey *rsa.PublicKey
	if parsed, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		publicKey, _ = parsed.(*rsa.PublicKey)
	} else if parsed, pkcsErr := x509.ParsePKCS1PublicKey(block.Bytes); pkcsErr == nil {
		publicKey = parsed
	}
	if publicKey == nil {
		return nil, errors.New("MySQL upstream RSA key is not an RSA public key")
	}
	plain := append([]byte(password), 0)
	for index := range plain {
		plain[index] ^= scramble[index%len(scramble)]
	}
	return rsa.EncryptOAEP(sha1.New(), rand.Reader, publicKey, plain, nil)
}

func parseMySQLError(payload []byte) error {
	if len(payload) < 3 || payload[0] != 0xff {
		return errors.New("unknown MySQL error")
	}
	code := binary.LittleEndian.Uint16(payload[1:])
	offset := 3
	state := ""
	if len(payload) >= 9 && payload[3] == '#' {
		state = string(payload[4:9])
		offset = 9
	}
	message := string(payload[offset:])
	if state != "" {
		return fmt.Errorf("MySQL %d (%s): %s", code, state, message)
	}
	return fmt.Errorf("MySQL %d: %s", code, message)
}

func mysqlErrorPayload(code uint16, state, message string) []byte {
	payload := []byte{0xff, byte(code), byte(code >> 8), '#'}
	if len(state) != 5 {
		state = "HY000"
	}
	payload = append(payload, state...)
	payload = append(payload, message...)
	return payload
}

func mysqlOKPayload() []byte {
	return []byte{0x00, 0x00, 0x00, byte(serverStatusAutocommit), byte(serverStatusAutocommit >> 8), 0x00, 0x00}
}

func randomScramble() ([]byte, error) {
	value := make([]byte, 20)
	if _, err := rand.Read(value); err != nil {
		return nil, err
	}
	for index := range value {
		if value[index] == 0 {
			value[index] = 1
		}
	}
	return value, nil
}

func randomConnectionID() (uint32, error) {
	var value [4]byte
	if _, err := rand.Read(value[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value[:]), nil
}

func serverTLSConfig(options *config.ListenerTLSOptions) (*tls.Config, error) {
	if options == nil || !options.Enabled {
		return nil, nil
	}
	return tlsutil.Server(*options)
}

func isSSLRequest(payload []byte) bool {
	return len(payload) == 32 && binary.LittleEndian.Uint32(payload)&clientSSL != 0
}

func sanitizedClientCapabilities(value uint32) uint32 {
	return value & inspectableCapabilities &^ (clientCompress | clientZSTDCompression | clientLocalFiles | clientOptionalResultsetMetadata | clientQueryAttributes | clientConnectAttrs)
}

func authPluginData(payload []byte) (string, []byte, error) {
	if len(payload) < 2 || payload[0] != 0xfe {
		return "", nil, errors.New("not an auth switch packet")
	}
	offset := 1
	plugin, err := readNULTerminated(payload, &offset)
	if err != nil {
		return "", nil, err
	}
	return plugin, bytes.TrimSuffix(payload[offset:], []byte{0}), nil
}

func closedError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || strings.Contains(strings.ToLower(err.Error()), "closed network connection")
}
