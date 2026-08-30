package rabbitadapter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

const (
	frameMethod    byte = 1
	frameHeader    byte = 2
	frameBody      byte = 3
	frameHeartbeat byte = 8
	frameEnd       byte = 0xce
	frameMin            = 4096
	hardFrameMax        = 128 * 1024 * 1024
)

type amqpFrame struct {
	typeID  byte
	channel uint16
	payload []byte
	raw     []byte
}

func readFrame(reader io.Reader, negotiated uint32) (amqpFrame, error) {
	header := make([]byte, 7)
	if _, err := io.ReadFull(reader, header); err != nil {
		return amqpFrame{}, err
	}
	size := binary.BigEndian.Uint32(header[3:7])
	limit := uint32(hardFrameMax)
	if negotiated != 0 && negotiated < limit {
		limit = negotiated
	}
	if uint64(size)+8 > uint64(limit) {
		return amqpFrame{}, fmt.Errorf("AMQP frame size %d exceeds negotiated maximum %d", uint64(size)+8, limit)
	}
	raw := make([]byte, int(size)+8)
	copy(raw, header)
	if _, err := io.ReadFull(reader, raw[7:]); err != nil {
		return amqpFrame{}, err
	}
	if raw[len(raw)-1] != frameEnd {
		return amqpFrame{}, errors.New("invalid AMQP frame terminator")
	}
	frame := amqpFrame{typeID: raw[0], channel: binary.BigEndian.Uint16(raw[1:3]), payload: raw[7 : len(raw)-1], raw: raw}
	if frame.typeID == frameHeartbeat && (frame.channel != 0 || len(frame.payload) != 0) {
		return amqpFrame{}, errors.New("invalid AMQP heartbeat frame")
	}
	return frame, nil
}

func makeFrame(typeID byte, channel uint16, payload []byte) amqpFrame {
	raw := make([]byte, len(payload)+8)
	raw[0] = typeID
	binary.BigEndian.PutUint16(raw[1:3], channel)
	binary.BigEndian.PutUint32(raw[3:7], uint32(len(payload)))
	copy(raw[7:], payload)
	raw[len(raw)-1] = frameEnd
	return amqpFrame{typeID: typeID, channel: channel, payload: payload, raw: raw}
}

func methodFrame(channel, classID, methodID uint16, arguments []byte) amqpFrame {
	payload := make([]byte, 4+len(arguments))
	binary.BigEndian.PutUint16(payload[:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], arguments)
	return makeFrame(frameMethod, channel, payload)
}

func methodID(frame amqpFrame) (uint16, uint16, bool) {
	if frame.typeID != frameMethod || len(frame.payload) < 4 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint16(frame.payload[:2]), binary.BigEndian.Uint16(frame.payload[2:4]), true
}

type fieldCursor struct {
	data []byte
	pos  int
}

func newCursor(data []byte) *fieldCursor { return &fieldCursor{data: data} }

func (cursor *fieldCursor) remaining() int { return len(cursor.data) - cursor.pos }

func (cursor *fieldCursor) take(length int) ([]byte, error) {
	if length < 0 || cursor.remaining() < length {
		return nil, io.ErrUnexpectedEOF
	}
	value := cursor.data[cursor.pos : cursor.pos+length]
	cursor.pos += length
	return value, nil
}

func (cursor *fieldCursor) octet() (byte, error) {
	value, err := cursor.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (cursor *fieldCursor) short() (uint16, error) {
	value, err := cursor.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value), nil
}

func (cursor *fieldCursor) long() (uint32, error) {
	value, err := cursor.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(value), nil
}

func (cursor *fieldCursor) longlong() (uint64, error) {
	value, err := cursor.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}

func (cursor *fieldCursor) shortstr() (string, error) {
	length, err := cursor.octet()
	if err != nil {
		return "", err
	}
	value, err := cursor.take(int(length))
	return string(value), err
}

func (cursor *fieldCursor) longstr() ([]byte, error) {
	length, err := cursor.long()
	if err != nil {
		return nil, err
	}
	return cursor.take(int(length))
}

func (cursor *fieldCursor) tableRaw() ([]byte, error) {
	start := cursor.pos
	length, err := cursor.long()
	if err != nil {
		return nil, err
	}
	if _, err := cursor.take(int(length)); err != nil {
		return nil, err
	}
	return cursor.data[start:cursor.pos], nil
}

func writeShortstr(buffer *bytes.Buffer, value string) error {
	if len(value) > 255 {
		return errors.New("AMQP short string exceeds 255 bytes")
	}
	buffer.WriteByte(byte(len(value)))
	_, _ = buffer.WriteString(value)
	return nil
}

func writeLongstr(buffer *bytes.Buffer, value []byte) error {
	if uint64(len(value)) > math.MaxUint32 {
		return errors.New("AMQP long string exceeds 4 GiB")
	}
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(value)))
	_, _ = buffer.Write(value)
	return nil
}

func decodeTable(raw []byte) (map[string]any, error) {
	cursor := newCursor(raw)
	length, err := cursor.long()
	if err != nil || int(length) != cursor.remaining() {
		return nil, errors.New("invalid AMQP field table")
	}
	return decodeTableEntries(cursor, cursor.pos+int(length), 0)
}

func decodeTableEntries(cursor *fieldCursor, end, depth int) (map[string]any, error) {
	if depth > 16 {
		return nil, errors.New("AMQP field table nesting exceeds 16")
	}
	result := make(map[string]any)
	for cursor.pos < end {
		if len(result) >= 1000 {
			return nil, errors.New("AMQP field table has too many entries")
		}
		name, err := cursor.shortstr()
		if err != nil {
			return nil, err
		}
		kind, err := cursor.octet()
		if err != nil {
			return nil, err
		}
		value, err := decodeFieldValue(cursor, kind, depth)
		if err != nil {
			return nil, err
		}
		result[name] = value
	}
	if cursor.pos != end {
		return nil, errors.New("AMQP field table length mismatch")
	}
	return result, nil
}

func decodeFieldValue(cursor *fieldCursor, kind byte, depth int) (any, error) {
	if depth > 16 {
		return nil, errors.New("AMQP field value nesting exceeds 16")
	}
	switch kind {
	case 't':
		value, err := cursor.octet()
		return value != 0, err
	case 'b':
		value, err := cursor.octet()
		return int8(value), err
	case 'B':
		return cursor.octet()
	case 'U':
		value, err := cursor.short()
		return int16(value), err
	case 'u':
		return cursor.short()
	case 'I':
		value, err := cursor.long()
		return int32(value), err
	case 'i':
		return cursor.long()
	case 'L':
		value, err := cursor.longlong()
		return int64(value), err
	case 'l':
		return cursor.longlong()
	case 'f':
		value, err := cursor.long()
		return math.Float32frombits(value), err
	case 'd':
		value, err := cursor.longlong()
		return math.Float64frombits(value), err
	case 'D':
		scale, err := cursor.octet()
		if err != nil {
			return nil, err
		}
		value, err := cursor.long()
		return map[string]any{"scale": scale, "value": int32(value)}, err
	case 's':
		return cursor.shortstr()
	case 'S', 'x':
		value, err := cursor.longstr()
		if kind == 'S' {
			return string(value), err
		}
		return fmt.Sprintf("<%d bytes>", len(value)), err
	case 'T':
		value, err := cursor.longlong()
		return time.Unix(int64(value), 0).UTC().Format(time.RFC3339), err
	case 'V':
		return nil, nil
	case 'F':
		length, err := cursor.long()
		if err != nil || int(length) > cursor.remaining() {
			return nil, errors.New("invalid nested AMQP table")
		}
		return decodeTableEntries(cursor, cursor.pos+int(length), depth+1)
	case 'A':
		length, err := cursor.long()
		if err != nil || int(length) > cursor.remaining() {
			return nil, errors.New("invalid AMQP array")
		}
		end := cursor.pos + int(length)
		values := make([]any, 0)
		for cursor.pos < end {
			itemKind, err := cursor.octet()
			if err != nil {
				return nil, err
			}
			item, err := decodeFieldValue(cursor, itemKind, depth+1)
			if err != nil {
				return nil, err
			}
			values = append(values, item)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unsupported AMQP field type %q", kind)
	}
}
