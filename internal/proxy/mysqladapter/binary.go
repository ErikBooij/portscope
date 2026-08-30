package mysqladapter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"
)

const (
	mysqlTypeDecimal    byte = 0x00
	mysqlTypeTiny       byte = 0x01
	mysqlTypeShort      byte = 0x02
	mysqlTypeLong       byte = 0x03
	mysqlTypeFloat      byte = 0x04
	mysqlTypeDouble     byte = 0x05
	mysqlTypeNull       byte = 0x06
	mysqlTypeTimestamp  byte = 0x07
	mysqlTypeLongLong   byte = 0x08
	mysqlTypeInt24      byte = 0x09
	mysqlTypeDate       byte = 0x0a
	mysqlTypeTime       byte = 0x0b
	mysqlTypeDateTime   byte = 0x0c
	mysqlTypeYear       byte = 0x0d
	mysqlTypeNewDate    byte = 0x0e
	mysqlTypeVarChar    byte = 0x0f
	mysqlTypeBit        byte = 0x10
	mysqlTypeJSON       byte = 0xf5
	mysqlTypeNewDecimal byte = 0xf6
	mysqlTypeEnum       byte = 0xf7
	mysqlTypeSet        byte = 0xf8
	mysqlTypeTinyBlob   byte = 0xf9
	mysqlTypeMediumBlob byte = 0xfa
	mysqlTypeLongBlob   byte = 0xfb
	mysqlTypeBlob       byte = 0xfc
	mysqlTypeVarString  byte = 0xfd
	mysqlTypeString     byte = 0xfe
	mysqlTypeGeometry   byte = 0xff

	columnUnsignedFlag      uint16 = 0x0020
	binaryValuePreviewLimit        = 4 * 1024
)

type binaryType struct {
	code     byte
	unsigned bool
}

type columnDefinition struct {
	name     string
	typeInfo binaryType
}

func parseColumnDefinition(payload []byte, fallback int) (columnDefinition, error) {
	offset := 0
	name := ""
	for index := 0; index < 6; index++ {
		value, _, err := readLenencBytes(payload, &offset)
		if err != nil {
			return columnDefinition{name: "column_" + strconv.Itoa(fallback+1)}, err
		}
		if index == 4 {
			name = string(value)
		}
	}
	_, null, err := readLenenc(payload, &offset)
	if err != nil || null || len(payload)-offset < 12 {
		return columnDefinition{name: "column_" + strconv.Itoa(fallback+1)}, errors.New("invalid MySQL column definition")
	}
	typeCode := payload[offset+6]
	flags := binary.LittleEndian.Uint16(payload[offset+7 : offset+9])
	if name == "" {
		name = "column_" + strconv.Itoa(fallback+1)
	}
	return columnDefinition{name: name, typeInfo: binaryType{code: typeCode, unsigned: flags&columnUnsignedFlag != 0}}, nil
}

func decodeExecuteParameters(payload []byte, statement *preparedStatement) ([]any, error) {
	if statement == nil || len(payload) < 10 || payload[0] != comStmtExecute {
		return nil, errors.New("invalid COM_STMT_EXECUTE packet")
	}
	count := statement.parameters
	if count == 0 {
		return []any{}, nil
	}
	offset := 10
	bitmapLength := (count + 7) / 8
	if len(payload)-offset < bitmapLength+1 {
		return nil, errors.New("truncated prepared parameter bitmap")
	}
	nulls := payload[offset : offset+bitmapLength]
	offset += bitmapLength
	newBindings := payload[offset] != 0
	offset++
	if newBindings {
		if len(payload)-offset < count*2 {
			return nil, errors.New("truncated prepared parameter types")
		}
		statement.parameterTypes = make([]binaryType, count)
		for index := range count {
			statement.parameterTypes[index] = binaryType{code: payload[offset], unsigned: payload[offset+1]&0x80 != 0}
			offset += 2
		}
	}
	if len(statement.parameterTypes) != count {
		return nil, errors.New("prepared parameter types were not supplied")
	}
	values := make([]any, 0, count)
	for index := range count {
		if nulls[index/8]&(1<<uint(index%8)) != 0 || statement.parameterTypes[index].code == mysqlTypeNull {
			values = append(values, nil)
			continue
		}
		if size := statement.longData[uint16(index)]; size > 0 {
			values = append(values, map[string]any{"longDataBytes": size})
			continue
		}
		value, err := decodeBinaryValue(payload, &offset, statement.parameterTypes[index])
		if err != nil {
			return nil, fmt.Errorf("parameter %d: %w", index, err)
		}
		values = append(values, value)
	}
	if offset != len(payload) {
		return nil, errors.New("prepared parameter payload has trailing data")
	}
	return values, nil
}

func decodeBinaryRow(payload []byte, columns []columnDefinition) ([]any, error) {
	if len(payload) == 0 || payload[0] != 0x00 {
		return nil, errors.New("invalid MySQL binary row header")
	}
	bitmapLength := (len(columns) + 9) / 8
	if len(payload) < 1+bitmapLength {
		return nil, errors.New("truncated MySQL binary row bitmap")
	}
	nulls := payload[1 : 1+bitmapLength]
	offset := 1 + bitmapLength
	values := make([]any, 0, len(columns))
	for index := range columns {
		bit := index + 2
		if nulls[bit/8]&(1<<uint(bit%8)) != 0 || columns[index].typeInfo.code == mysqlTypeNull {
			values = append(values, nil)
			continue
		}
		value, err := decodeBinaryValue(payload, &offset, columns[index].typeInfo)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", columns[index].name, err)
		}
		values = append(values, value)
	}
	if offset != len(payload) {
		return nil, errors.New("MySQL binary row has trailing data")
	}
	return values, nil
}

func decodeBinaryValue(data []byte, offset *int, valueType binaryType) (any, error) {
	switch valueType.code {
	case mysqlTypeNull:
		return nil, nil
	case mysqlTypeTiny:
		value, err := fixedInteger(data, offset, 1)
		return integerValue(value, 8, valueType.unsigned), err
	case mysqlTypeShort, mysqlTypeYear:
		value, err := fixedInteger(data, offset, 2)
		return integerValue(value, 16, valueType.unsigned), err
	case mysqlTypeLong, mysqlTypeInt24:
		value, err := fixedInteger(data, offset, 4)
		return integerValue(value, 32, valueType.unsigned), err
	case mysqlTypeLongLong:
		value, err := fixedInteger(data, offset, 8)
		return integerValue(value, 64, valueType.unsigned), err
	case mysqlTypeFloat:
		value, err := fixedInteger(data, offset, 4)
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(uint32(value)), nil
	case mysqlTypeDouble:
		value, err := fixedInteger(data, offset, 8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(value), nil
	case mysqlTypeDate, mysqlTypeNewDate, mysqlTypeDateTime, mysqlTypeTimestamp:
		return decodeBinaryDateTime(data, offset, valueType.code)
	case mysqlTypeTime:
		return decodeBinaryTime(data, offset)
	case mysqlTypeDecimal, mysqlTypeNewDecimal, mysqlTypeVarChar, mysqlTypeVarString, mysqlTypeString,
		mysqlTypeEnum, mysqlTypeSet, mysqlTypeJSON, mysqlTypeTinyBlob, mysqlTypeMediumBlob, mysqlTypeLongBlob,
		mysqlTypeBlob, mysqlTypeGeometry, mysqlTypeBit:
		value, null, err := readLenencBytes(data, offset)
		if err != nil {
			return nil, err
		}
		if null {
			return nil, nil
		}
		return capturedBinaryValue(value), nil
	default:
		return nil, fmt.Errorf("unsupported MySQL binary type 0x%02x", valueType.code)
	}
}

func capturedBinaryValue(value []byte) any {
	if len(value) <= binaryValuePreviewLimit {
		if utf8.Valid(value) {
			return string(value)
		}
		return "0x" + fmt.Sprintf("%x", value)
	}
	preview := value[:binaryValuePreviewLimit]
	if utf8.Valid(preview) {
		return map[string]any{"preview": string(preview), "size": len(value), "truncated": true}
	}
	return map[string]any{"previewHex": fmt.Sprintf("%x", preview[:min(512, len(preview))]), "size": len(value), "truncated": true}
}

func hasTruncatedBinaryValue(values []any) bool {
	for _, value := range values {
		if metadata, ok := value.(map[string]any); ok && metadata["truncated"] == true {
			return true
		}
	}
	return false
}

func fixedInteger(data []byte, offset *int, size int) (uint64, error) {
	if size < 1 || size > 8 || len(data)-*offset < size {
		return 0, errors.New("truncated fixed-width value")
	}
	var buffer [8]byte
	copy(buffer[:], data[*offset:*offset+size])
	*offset += size
	return binary.LittleEndian.Uint64(buffer[:]), nil
}

func integerValue(value uint64, bits int, unsigned bool) any {
	if unsigned {
		return value
	}
	switch bits {
	case 8:
		return int64(int8(value))
	case 16:
		return int64(int16(value))
	case 32:
		return int64(int32(value))
	default:
		return int64(value)
	}
}

func decodeBinaryDateTime(data []byte, offset *int, typeCode byte) (any, error) {
	if *offset >= len(data) {
		return nil, errors.New("truncated temporal value")
	}
	length := int(data[*offset])
	(*offset)++
	if length != 0 && length != 4 && length != 7 && length != 11 || len(data)-*offset < length {
		return nil, errors.New("invalid temporal value length")
	}
	if length == 0 {
		if typeCode == mysqlTypeDate || typeCode == mysqlTypeNewDate {
			return "0000-00-00", nil
		}
		return "0000-00-00 00:00:00", nil
	}
	year := binary.LittleEndian.Uint16(data[*offset:])
	month, day := data[*offset+2], data[*offset+3]
	*offset += 4
	date := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	if length == 4 || typeCode == mysqlTypeDate || typeCode == mysqlTypeNewDate {
		return date, nil
	}
	hour, minute, second := data[*offset], data[*offset+1], data[*offset+2]
	*offset += 3
	result := fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
	if length == 11 {
		microseconds := binary.LittleEndian.Uint32(data[*offset:])
		*offset += 4
		result += fmt.Sprintf(".%06d", microseconds)
	}
	return result, nil
}

func decodeBinaryTime(data []byte, offset *int) (any, error) {
	if *offset >= len(data) {
		return nil, errors.New("truncated time value")
	}
	length := int(data[*offset])
	(*offset)++
	if length != 0 && length != 8 && length != 12 || len(data)-*offset < length {
		return nil, errors.New("invalid time value length")
	}
	if length == 0 {
		return "00:00:00", nil
	}
	negative := data[*offset] != 0
	days := binary.LittleEndian.Uint32(data[*offset+1:])
	hour, minute, second := data[*offset+5], data[*offset+6], data[*offset+7]
	*offset += 8
	prefix := ""
	if negative {
		prefix = "-"
	}
	result := fmt.Sprintf("%s%d:%02d:%02d", prefix, uint64(days)*24+uint64(hour), minute, second)
	if length == 12 {
		microseconds := binary.LittleEndian.Uint32(data[*offset:])
		*offset += 4
		result += fmt.Sprintf(".%06d", microseconds)
	}
	return result, nil
}
