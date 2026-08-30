package mysqladapter

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	maxPacketPayload = 1<<24 - 1
	captureLimit     = 256 * 1024
)

type packet struct {
	sequence byte
	payload  []byte
}

type logicalPacket struct {
	sequence     byte
	payload      []byte
	size         int64
	truncated    bool
	nextSequence byte
}

func readPacket(reader *bufio.Reader) (packet, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return packet{}, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return packet{}, err
	}
	return packet{sequence: header[3], payload: payload}, nil
}

func writePacket(writer io.Writer, item packet) error {
	if len(item.payload) > maxPacketPayload {
		return errors.New("MySQL physical packet exceeds 16 MiB")
	}
	header := [4]byte{byte(len(item.payload)), byte(len(item.payload) >> 8), byte(len(item.payload) >> 16), item.sequence}
	if err := writeAll(writer, header[:]); err != nil {
		return err
	}
	return writeAll(writer, item.payload)
}

// readLogical forwards every physical packet unchanged while retaining only a
// bounded prefix for inspection. MySQL logical packets may span any number of
// 16 MiB physical packets.
func readLogical(reader *bufio.Reader, destination io.Writer, expected byte) (logicalPacket, error) {
	result := logicalPacket{sequence: expected}
	for {
		item, err := readPacket(reader)
		if err != nil {
			return result, err
		}
		if item.sequence != expected {
			return result, fmt.Errorf("MySQL sequence id %d, expected %d", item.sequence, expected)
		}
		if destination != nil {
			if err := writePacket(destination, item); err != nil {
				return result, err
			}
		}
		result.size += int64(len(item.payload))
		remaining := captureLimit - len(result.payload)
		if remaining > 0 {
			result.payload = append(result.payload, item.payload[:min(remaining, len(item.payload))]...)
		}
		if len(item.payload) > remaining {
			result.truncated = true
		}
		expected++
		if len(item.payload) < maxPacketPayload {
			result.nextSequence = expected
			return result, nil
		}
	}
}

func writeLogical(writer io.Writer, sequence byte, payload []byte) (byte, error) {
	for len(payload) >= maxPacketPayload {
		if err := writePacket(writer, packet{sequence: sequence, payload: payload[:maxPacketPayload]}); err != nil {
			return sequence, err
		}
		sequence++
		payload = payload[maxPacketPayload:]
	}
	if err := writePacket(writer, packet{sequence: sequence, payload: payload}); err != nil {
		return sequence, err
	}
	return sequence + 1, nil
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

func uint24(data []byte) int {
	return int(data[0]) | int(data[1])<<8 | int(data[2])<<16
}

func appendUint24(target []byte, value int) []byte {
	return append(target, byte(value), byte(value>>8), byte(value>>16))
}

func readLenenc(data []byte, offset *int) (uint64, bool, error) {
	if *offset >= len(data) {
		return 0, false, io.ErrUnexpectedEOF
	}
	prefix := data[*offset]
	*offset++
	switch prefix {
	case 0xfb:
		return 0, true, nil
	case 0xfc:
		if len(data)-*offset < 2 {
			return 0, false, io.ErrUnexpectedEOF
		}
		value := uint64(binary.LittleEndian.Uint16(data[*offset:]))
		*offset += 2
		return value, false, nil
	case 0xfd:
		if len(data)-*offset < 3 {
			return 0, false, io.ErrUnexpectedEOF
		}
		value := uint64(uint24(data[*offset:]))
		*offset += 3
		return value, false, nil
	case 0xfe:
		if len(data)-*offset < 8 {
			return 0, false, io.ErrUnexpectedEOF
		}
		value := binary.LittleEndian.Uint64(data[*offset:])
		*offset += 8
		return value, false, nil
	case 0xff:
		return 0, false, errors.New("0xff is not a length-encoded integer")
	default:
		return uint64(prefix), false, nil
	}
}

func readLenencBytes(data []byte, offset *int) ([]byte, bool, error) {
	length, null, err := readLenenc(data, offset)
	if err != nil || null {
		return nil, null, err
	}
	if length > uint64(len(data)-*offset) {
		return nil, false, io.ErrUnexpectedEOF
	}
	start := *offset
	*offset += int(length)
	return data[start:*offset], false, nil
}

func readNULTerminated(data []byte, offset *int) (string, error) {
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
