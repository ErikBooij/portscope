package postgresadapter

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	protocolVersion30 = 196608
	sslRequestCode    = 80877103
	gssRequestCode    = 80877104
	cancelRequestCode = 80877102
	maxStartupSize    = 64 * 1024
	maxMessageSize    = 64 * 1024 * 1024
	captureLimit      = 256 * 1024
)

type message struct {
	typ  byte
	body []byte
}

func readStartup(reader *bufio.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(header[:]))
	if length < 8 || length > maxStartupSize {
		return nil, fmt.Errorf("invalid PostgreSQL startup packet length %d", length)
	}
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writeStartup(writer io.Writer, payload []byte) error {
	if len(payload)+4 > maxStartupSize {
		return errors.New("PostgreSQL startup packet exceeds 64 KiB")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)+4))
	if err := writeAll(writer, header[:]); err != nil {
		return err
	}
	return writeAll(writer, payload)
}

func readMessage(reader *bufio.Reader) (message, error) {
	typ, err := reader.ReadByte()
	if err != nil {
		return message{}, err
	}
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return message{}, err
	}
	length := int(binary.BigEndian.Uint32(header[:]))
	if length < 4 || length > maxMessageSize {
		return message{}, fmt.Errorf("invalid PostgreSQL message length %d", length)
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(reader, body); err != nil {
		return message{}, err
	}
	return message{typ: typ, body: body}, nil
}

func writeMessage(writer io.Writer, item message) error {
	if len(item.body)+4 > maxMessageSize {
		return errors.New("PostgreSQL message exceeds 64 MiB")
	}
	header := []byte{item.typ, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[1:], uint32(len(item.body)+4))
	if err := writeAll(writer, header); err != nil {
		return err
	}
	return writeAll(writer, item.body)
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrUnexpectedEOF
		}
		data = data[written:]
	}
	return nil
}

func int32At(data []byte, offset int) (int32, error) {
	if offset < 0 || len(data)-offset < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	return int32(binary.BigEndian.Uint32(data[offset:])), nil
}

func int16At(data []byte, offset int) (int16, error) {
	if offset < 0 || len(data)-offset < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	return int16(binary.BigEndian.Uint16(data[offset:])), nil
}

func appendInt32(target []byte, value int32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(value))
	return append(target, encoded[:]...)
}

func appendInt16(target []byte, value int16) []byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(value))
	return append(target, encoded[:]...)
}

func readCString(data []byte, offset *int) (string, error) {
	start := *offset
	for *offset < len(data) && data[*offset] != 0 {
		*offset++
	}
	if *offset >= len(data) {
		return "", io.ErrUnexpectedEOF
	}
	value := string(data[start:*offset])
	*offset++
	return value, nil
}

func parseStartup(payload []byte) (map[string]string, error) {
	version, err := int32At(payload, 0)
	if err != nil || version>>16 != 3 {
		return nil, errors.New("only PostgreSQL protocol version 3 is supported")
	}
	parameters := make(map[string]string)
	offset := 4
	for offset < len(payload) {
		if payload[offset] == 0 {
			if offset != len(payload)-1 {
				return nil, errors.New("trailing bytes in PostgreSQL startup packet")
			}
			return parameters, nil
		}
		name, err := readCString(payload, &offset)
		if err != nil || name == "" {
			return nil, errors.New("invalid PostgreSQL startup parameter name")
		}
		value, err := readCString(payload, &offset)
		if err != nil {
			return nil, errors.New("invalid PostgreSQL startup parameter value")
		}
		if len(parameters) >= 64 {
			return nil, errors.New("too many PostgreSQL startup parameters")
		}
		parameters[name] = value
	}
	return nil, errors.New("unterminated PostgreSQL startup packet")
}

func negotiateProtocolVersion(payload []byte, parameters map[string]string) (message, bool) {
	version, err := int32At(payload, 0)
	if err != nil {
		return message{}, false
	}
	unsupported := make([]string, 0)
	for name := range parameters {
		if strings.HasPrefix(name, "_pq_.") {
			unsupported = append(unsupported, name)
		}
	}
	minor := version & 0xffff
	if minor == 0 && len(unsupported) == 0 {
		return message{}, false
	}
	body := appendInt32(nil, 0) // Highest protocol 3 minor version Portscope implements.
	body = appendInt32(body, int32(len(unsupported)))
	for _, name := range unsupported {
		body = append(body, name...)
		body = append(body, 0)
	}
	return message{typ: 'v', body: body}, true
}

func buildStartup(parameters map[string]string, username, database string) []byte {
	payload := appendInt32(nil, protocolVersion30)
	ordered := []string{"user", username, "database", database}
	for _, name := range []string{"application_name", "client_encoding", "DateStyle", "TimeZone", "options"} {
		if value := parameters[name]; value != "" {
			ordered = append(ordered, name, value)
		}
	}
	for _, value := range ordered {
		payload = append(payload, value...)
		payload = append(payload, 0)
	}
	return append(payload, 0)
}

func authenticationMessage(code int32, data []byte) message {
	return message{typ: 'R', body: append(appendInt32(nil, code), data...)}
}

func errorResponse(severity, code, text string) message {
	body := []byte{'S'}
	body = append(body, severity...)
	body = append(body, 0, 'C')
	body = append(body, code...)
	body = append(body, 0, 'M')
	body = append(body, text...)
	body = append(body, 0, 0)
	return message{typ: 'E', body: body}
}

func parseError(body []byte) (severity, code, text string) {
	offset := 0
	for offset < len(body) && body[offset] != 0 {
		field := body[offset]
		offset++
		value, err := readCString(body, &offset)
		if err != nil {
			break
		}
		switch field {
		case 'S', 'V':
			if severity == "" || field == 'V' {
				severity = value
			}
		case 'C':
			code = value
		case 'M':
			text = value
		}
	}
	return
}
