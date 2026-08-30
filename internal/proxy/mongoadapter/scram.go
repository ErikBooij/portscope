package mongoadapter

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"golang.org/x/text/secure/precis"
)

type scramAlgorithm struct {
	name string
	hash func() hash.Hash
	size int
}

func algorithm(name string) (scramAlgorithm, error) {
	switch name {
	case "SCRAM-SHA-256":
		return scramAlgorithm{name: name, hash: sha256.New, size: sha256.Size}, nil
	case "SCRAM-SHA-1":
		return scramAlgorithm{name: name, hash: sha1.New, size: sha1.Size}, nil
	default:
		return scramAlgorithm{}, fmt.Errorf("unsupported SCRAM mechanism %q", name)
	}
}

type scramClient struct {
	algorithm       scramAlgorithm
	username        string
	password        string
	nonce           string
	clientFirstBare string
	authMessage     string
	serverSignature []byte
}

func newSCRAMClient(mechanism, username, password string) (*scramClient, error) {
	selected, err := algorithm(mechanism)
	if err != nil {
		return nil, err
	}
	nonce, err := randomNonce()
	if err != nil {
		return nil, err
	}
	client := &scramClient{algorithm: selected, username: username, password: password, nonce: nonce}
	client.clientFirstBare = "n=" + escapeSCRAMName(username) + ",r=" + nonce
	return client, nil
}

func (client *scramClient) first() []byte {
	return []byte("n,," + client.clientFirstBare)
}

func (client *scramClient) final(serverFirst string) ([]byte, error) {
	attributes, err := parseSCRAMAttributes(serverFirst)
	if err != nil {
		return nil, err
	}
	serverNonce, saltText, iterationsText := attributes["r"], attributes["s"], attributes["i"]
	if !strings.HasPrefix(serverNonce, client.nonce) || len(serverNonce) == len(client.nonce) {
		return nil, errors.New("SCRAM server nonce does not extend the client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(saltText)
	if err != nil {
		return nil, errors.New("SCRAM server returned an invalid salt")
	}
	iterations, err := strconv.Atoi(iterationsText)
	if err != nil || iterations < 4096 || iterations > 10_000_000 {
		return nil, errors.New("SCRAM server returned an unsafe iteration count")
	}
	password, err := mongoSCRAMPassword(client.algorithm.name, client.username, client.password)
	if err != nil {
		return nil, err
	}
	saltedPassword := pbkdf2Key([]byte(password), salt, iterations, client.algorithm.size, client.algorithm.hash)
	clientFinalWithoutProof := "c=biws,r=" + serverNonce
	client.authMessage = client.clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	clientKey := hmacDigest(client.algorithm.hash, saltedPassword, []byte("Client Key"))
	storedKey := hashDigest(client.algorithm.hash, clientKey)
	clientSignature := hmacDigest(client.algorithm.hash, storedKey, []byte(client.authMessage))
	proof := xorBytes(clientKey, clientSignature)
	serverKey := hmacDigest(client.algorithm.hash, saltedPassword, []byte("Server Key"))
	client.serverSignature = hmacDigest(client.algorithm.hash, serverKey, []byte(client.authMessage))
	return []byte(clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

func (client *scramClient) verify(serverFinal string) error {
	attributes, err := parseSCRAMAttributes(serverFinal)
	if err != nil {
		return err
	}
	if message := attributes["e"]; message != "" {
		return fmt.Errorf("SCRAM server rejected authentication: %s", message)
	}
	actual, err := base64.StdEncoding.DecodeString(attributes["v"])
	if err != nil || subtle.ConstantTimeCompare(actual, client.serverSignature) != 1 {
		return errors.New("SCRAM server signature did not verify")
	}
	return nil
}

type scramServer struct {
	algorithm       scramAlgorithm
	expectedUser    string
	password        string
	clientFirstBare string
	serverFirst     string
	nonce           string
	salt            []byte
	iterations      int
}

func newSCRAMServer(mechanism, expectedUser, password string, clientFirst []byte) (*scramServer, []byte, error) {
	selected, err := algorithm(mechanism)
	if err != nil {
		return nil, nil, err
	}
	value := string(clientFirst)
	if !strings.HasPrefix(value, "n,,") && !strings.HasPrefix(value, "y,,") {
		return nil, nil, errors.New("SCRAM channel binding is not supported")
	}
	bare := value[3:]
	attributes, err := parseSCRAMAttributes(bare)
	if err != nil {
		return nil, nil, err
	}
	username, err := unescapeSCRAMName(attributes["n"])
	if err != nil || subtle.ConstantTimeCompare([]byte(username), []byte(expectedUser)) != 1 {
		return nil, nil, errors.New("invalid MongoDB listener credentials")
	}
	clientNonce := attributes["r"]
	if len(clientNonce) < 8 {
		return nil, nil, errors.New("SCRAM client nonce is too short")
	}
	serverNonce, err := randomNonce()
	if err != nil {
		return nil, nil, err
	}
	salt := make([]byte, 18)
	if _, err := rand.Read(salt); err != nil {
		return nil, nil, err
	}
	conversation := &scramServer{algorithm: selected, expectedUser: expectedUser, password: password, clientFirstBare: bare, nonce: clientNonce + serverNonce, salt: salt, iterations: 15000}
	conversation.serverFirst = "r=" + conversation.nonce + ",s=" + base64.StdEncoding.EncodeToString(salt) + ",i=" + strconv.Itoa(conversation.iterations)
	return conversation, []byte(conversation.serverFirst), nil
}

func (server *scramServer) finish(clientFinal []byte) ([]byte, error) {
	value := string(clientFinal)
	attributes, err := parseSCRAMAttributes(value)
	if err != nil {
		return nil, err
	}
	if attributes["c"] != "biws" || attributes["r"] != server.nonce {
		return nil, errors.New("SCRAM channel binding or nonce mismatch")
	}
	proofText := attributes["p"]
	proof, err := base64.StdEncoding.DecodeString(proofText)
	if err != nil || len(proof) != server.algorithm.size {
		return nil, errors.New("invalid SCRAM client proof")
	}
	proofIndex := strings.LastIndex(value, ",p=")
	if proofIndex < 0 {
		return nil, errors.New("SCRAM client proof is missing")
	}
	authMessage := server.clientFirstBare + "," + server.serverFirst + "," + value[:proofIndex]
	password, err := mongoSCRAMPassword(server.algorithm.name, server.expectedUser, server.password)
	if err != nil {
		return nil, err
	}
	saltedPassword := pbkdf2Key([]byte(password), server.salt, server.iterations, server.algorithm.size, server.algorithm.hash)
	clientKey := hmacDigest(server.algorithm.hash, saltedPassword, []byte("Client Key"))
	storedKey := hashDigest(server.algorithm.hash, clientKey)
	clientSignature := hmacDigest(server.algorithm.hash, storedKey, []byte(authMessage))
	recoveredKey := xorBytes(proof, clientSignature)
	if subtle.ConstantTimeCompare(hashDigest(server.algorithm.hash, recoveredKey), storedKey) != 1 {
		return nil, errors.New("invalid MongoDB listener credentials")
	}
	serverKey := hmacDigest(server.algorithm.hash, saltedPassword, []byte("Server Key"))
	signature := hmacDigest(server.algorithm.hash, serverKey, []byte(authMessage))
	return []byte("v=" + base64.StdEncoding.EncodeToString(signature)), nil
}

func mongoSCRAMPassword(mechanism, username, password string) (string, error) {
	if mechanism == "SCRAM-SHA-1" {
		digest := md5.Sum([]byte(username + ":mongo:" + password))
		return hex.EncodeToString(digest[:]), nil
	}
	prepared, err := precis.OpaqueString.String(password)
	if err != nil {
		return "", errors.New("password is not valid SCRAM-SHA-256 text")
	}
	return prepared, nil
}

func parseSCRAMAttributes(value string) (map[string]string, error) {
	result := make(map[string]string)
	for _, part := range strings.Split(value, ",") {
		if len(part) < 3 || part[1] != '=' {
			return nil, errors.New("invalid SCRAM attribute")
		}
		key := part[:1]
		if _, exists := result[key]; exists {
			return nil, errors.New("duplicate SCRAM attribute")
		}
		result[key] = part[2:]
	}
	return result, nil
}

func escapeSCRAMName(value string) string {
	return strings.NewReplacer("=", "=3D", ",", "=2C").Replace(value)
}

func unescapeSCRAMName(value string) (string, error) {
	for i := 0; i < len(value); i++ {
		if value[i] == '=' && (i+2 >= len(value) || (value[i:i+3] != "=2C" && value[i:i+3] != "=3D")) {
			return "", errors.New("invalid SCRAM username escape")
		}
	}
	return strings.NewReplacer("=2C", ",", "=3D", "=").Replace(value), nil
}

func randomNonce() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(value), nil
}

func hmacDigest(hashFn func() hash.Hash, key, data []byte) []byte {
	mac := hmac.New(hashFn, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

func hashDigest(hashFn func() hash.Hash, data []byte) []byte {
	digest := hashFn()
	_, _ = digest.Write(data)
	return digest.Sum(nil)
}

func xorBytes(left, right []byte) []byte {
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] ^ right[i]
	}
	return result
}

func pbkdf2Key(password, salt []byte, iterations, length int, hashFn func() hash.Hash) []byte {
	blocks := (length + hashFn().Size() - 1) / hashFn().Size()
	result := make([]byte, 0, blocks*hashFn().Size())
	for block := 1; block <= blocks; block++ {
		counter := []byte{byte(block >> 24), byte(block >> 16), byte(block >> 8), byte(block)}
		current := hmacDigest(hashFn, password, append(append([]byte(nil), salt...), counter...))
		combined := append([]byte(nil), current...)
		for iteration := 1; iteration < iterations; iteration++ {
			current = hmacDigest(hashFn, password, current)
			for i := range combined {
				combined[i] ^= current[i]
			}
		}
		result = append(result, combined...)
	}
	return result[:length]
}
