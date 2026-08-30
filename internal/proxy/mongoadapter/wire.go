package mongoadapter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	opReply      int32 = 1
	opQuery      int32 = 2004
	opCompressed int32 = 2012
	opMsg        int32 = 2013
	maxMessage         = 64 * 1024 * 1024
)

type wireMessage struct {
	raw        []byte
	requestID  int32
	responseTo int32
	opcode     int32
}

type commandMessage struct {
	name      string
	database  string
	document  bson.Raw
	sequences map[string][]bson.Raw
	flags     uint32
}

func readWireMessage(reader io.Reader) (wireMessage, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(reader, header); err != nil {
		return wireMessage{}, err
	}
	length := int64(int32(binary.LittleEndian.Uint32(header[:4])))
	if length < 16 || length > maxMessage {
		return wireMessage{}, fmt.Errorf("invalid MongoDB message length %d", length)
	}
	raw := make([]byte, int(length))
	copy(raw, header)
	if _, err := io.ReadFull(reader, raw[16:]); err != nil {
		return wireMessage{}, err
	}
	return wireMessage{raw: raw, requestID: int32(binary.LittleEndian.Uint32(raw[4:8])), responseTo: int32(binary.LittleEndian.Uint32(raw[8:12])), opcode: int32(binary.LittleEndian.Uint32(raw[12:16]))}, nil
}

func parseCommand(message wireMessage) (commandMessage, error) {
	switch message.opcode {
	case opMsg:
		return parseOPMsg(message.raw)
	case opQuery:
		return parseOPQuery(message.raw)
	case opCompressed:
		return commandMessage{name: "COMPRESSED"}, nil
	default:
		return commandMessage{name: fmt.Sprintf("OPCODE %d", message.opcode)}, nil
	}
}

func parseOPMsg(raw []byte) (commandMessage, error) {
	if len(raw) < 21 {
		return commandMessage{}, errors.New("short OP_MSG")
	}
	flags := binary.LittleEndian.Uint32(raw[16:20])
	end := len(raw)
	if flags&1 != 0 {
		if end < 25 {
			return commandMessage{}, errors.New("short checksummed OP_MSG")
		}
		end -= 4
	}
	result := commandMessage{flags: flags, sequences: make(map[string][]bson.Raw)}
	for position := 20; position < end; {
		kind := raw[position]
		position++
		switch kind {
		case 0:
			document, next, err := readRawDocument(raw, position, end)
			if err != nil {
				return commandMessage{}, err
			}
			if result.document == nil {
				result.document = document
			}
			position = next
		case 1:
			if position+4 > end {
				return commandMessage{}, errors.New("short OP_MSG document sequence")
			}
			sectionEnd := position + int(int32(binary.LittleEndian.Uint32(raw[position:position+4])))
			if sectionEnd <= position+4 || sectionEnd > end {
				return commandMessage{}, errors.New("invalid OP_MSG document sequence size")
			}
			nameStart := position + 4
			nameEnd := nameStart
			for nameEnd < sectionEnd && raw[nameEnd] != 0 {
				nameEnd++
			}
			if nameEnd == sectionEnd {
				return commandMessage{}, errors.New("unterminated OP_MSG sequence name")
			}
			name := string(raw[nameStart:nameEnd])
			for cursor := nameEnd + 1; cursor < sectionEnd; {
				document, next, err := readRawDocument(raw, cursor, sectionEnd)
				if err != nil {
					return commandMessage{}, err
				}
				result.sequences[name] = append(result.sequences[name], document)
				cursor = next
			}
			position = sectionEnd
		default:
			return commandMessage{}, fmt.Errorf("unsupported OP_MSG section kind %d", kind)
		}
	}
	if result.document == nil {
		return commandMessage{}, errors.New("OP_MSG has no body document")
	}
	result.name, result.database = commandIdentity(result.document)
	return result, nil
}

func parseOPQuery(raw []byte) (commandMessage, error) {
	if len(raw) < 29 {
		return commandMessage{}, errors.New("short OP_QUERY")
	}
	position := 20
	for position < len(raw) && raw[position] != 0 {
		position++
	}
	if position == len(raw) || position+9 > len(raw) {
		return commandMessage{}, errors.New("invalid OP_QUERY namespace")
	}
	namespace := string(raw[20:position])
	position += 1 + 8
	document, _, err := readRawDocument(raw, position, len(raw))
	if err != nil {
		return commandMessage{}, err
	}
	name, database := commandIdentity(document)
	if database == "" {
		for i := range namespace {
			if namespace[i] == '.' {
				database = namespace[:i]
				break
			}
		}
	}
	return commandMessage{name: name, database: database, document: document}, nil
}

func readRawDocument(raw []byte, position, end int) (bson.Raw, int, error) {
	if position+4 > end {
		return nil, position, errors.New("short BSON document")
	}
	length := int(int32(binary.LittleEndian.Uint32(raw[position : position+4])))
	if length < 5 || position+length > end {
		return nil, position, errors.New("invalid BSON document length")
	}
	document := bson.Raw(raw[position : position+length])
	if err := document.Validate(); err != nil {
		return nil, position, fmt.Errorf("invalid BSON document: %w", err)
	}
	return document, position + length, nil
}

func commandIdentity(document bson.Raw) (string, string) {
	elements, err := document.Elements()
	if err != nil || len(elements) == 0 {
		return "UNKNOWN", ""
	}
	name := elements[0].Key()
	if name == "$query" {
		if nested, ok := elements[0].Value().DocumentOK(); ok {
			elements, _ = nested.Elements()
			if len(elements) > 0 {
				name = elements[0].Key()
				document = nested
			}
		}
	}
	database, _ := document.Lookup("$db").StringValueOK()
	return name, database
}

func marshalDocument(document any) (bson.Raw, error) {
	raw, err := bson.Marshal(document)
	return bson.Raw(raw), err
}

func makeOPMsg(requestID, responseTo int32, document bson.Raw) wireMessage {
	raw := make([]byte, 21+len(document))
	binary.LittleEndian.PutUint32(raw[:4], uint32(len(raw)))
	binary.LittleEndian.PutUint32(raw[4:8], uint32(requestID))
	binary.LittleEndian.PutUint32(raw[8:12], uint32(responseTo))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(opMsg))
	raw[20] = 0
	copy(raw[21:], document)
	return wireMessage{raw: raw, requestID: requestID, responseTo: responseTo, opcode: opMsg}
}

func makeCommandReply(request wireMessage, requestID int32, document bson.Raw) wireMessage {
	if request.opcode != opQuery {
		return makeOPMsg(requestID, request.requestID, document)
	}
	raw := make([]byte, 36+len(document))
	binary.LittleEndian.PutUint32(raw[:4], uint32(len(raw)))
	binary.LittleEndian.PutUint32(raw[4:8], uint32(requestID))
	binary.LittleEndian.PutUint32(raw[8:12], uint32(request.requestID))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(opReply))
	binary.LittleEndian.PutUint32(raw[32:36], 1)
	copy(raw[36:], document)
	return wireMessage{raw: raw, requestID: requestID, responseTo: request.requestID, opcode: opReply}
}

func responseDocument(message wireMessage) (bson.Raw, error) {
	switch message.opcode {
	case opMsg:
		parsed, err := parseOPMsg(message.raw)
		return parsed.document, err
	case opReply:
		if len(message.raw) < 41 {
			return nil, errors.New("short OP_REPLY")
		}
		document, _, err := readRawDocument(message.raw, 36, len(message.raw))
		return document, err
	default:
		return nil, fmt.Errorf("unsupported MongoDB response opcode %d", message.opcode)
	}
}
