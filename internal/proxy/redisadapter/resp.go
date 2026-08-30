package redisadapter

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxFrame      = 64 << 20
	maxCollection = 1 << 20
)

type value struct {
	kind  byte
	text  string
	items []value
}
type frame struct {
	raw   []byte
	value value
}

func readFrame(reader *bufio.Reader) (frame, error) {
	var raw bytes.Buffer
	parsed, err := readValue(reader, &raw, 0)
	return frame{raw: raw.Bytes(), value: parsed}, err
}
func readValue(reader *bufio.Reader, raw *bytes.Buffer, depth int) (value, error) {
	if depth > 64 {
		return value{}, errors.New("RESP nesting exceeds 64 levels")
	}
	prefix, err := reader.ReadByte()
	if err != nil {
		return value{}, err
	}
	raw.WriteByte(prefix)
	line, err := readLine(reader, raw)
	if err != nil {
		return value{}, err
	}
	switch prefix {
	case '+', '-', ':', ',', '(', '#', '_':
		return value{kind: prefix, text: line}, nil
	case '$', '!', '=':
		if line == "?" && prefix == '$' {
			return readStreamedString(reader, raw)
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return value{}, fmt.Errorf("invalid RESP bulk length %q", line)
		}
		if length < 0 {
			return value{kind: prefix, text: "null"}, nil
		}
		if length > maxFrame {
			return value{}, errors.New("RESP frame exceeds 64 MiB")
		}
		data := make([]byte, length+2)
		if _, err = io.ReadFull(reader, data); err != nil {
			return value{}, err
		}
		raw.Write(data)
		if !bytes.HasSuffix(data, []byte("\r\n")) {
			return value{}, errors.New("invalid RESP bulk terminator")
		}
		return value{kind: prefix, text: string(data[:length])}, nil
	case '*', '~', '>':
		if line == "?" {
			return readStreamedItems(reader, raw, depth, prefix)
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return value{}, fmt.Errorf("invalid RESP collection length %q", line)
		}
		if count < 0 {
			return value{kind: prefix, text: "null"}, nil
		}
		if count > maxCollection {
			return value{}, errors.New("RESP collection exceeds 1048576 elements")
		}
		return readItems(reader, raw, depth, count, prefix)
	case '%', '|':
		if line == "?" {
			return readStreamedItems(reader, raw, depth, prefix)
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return value{}, fmt.Errorf("invalid RESP map length %q", line)
		}
		if count < 0 {
			return value{kind: prefix, text: "null"}, nil
		}
		if count > maxCollection/2 {
			return value{}, errors.New("RESP map exceeds 524288 pairs")
		}
		return readItems(reader, raw, depth, count*2, prefix)
	case '.':
		if line != "" {
			return value{}, errors.New("invalid RESP streamed aggregate terminator")
		}
		return value{kind: '.'}, nil
	default:
		return value{}, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func readStreamedString(reader *bufio.Reader, raw *bytes.Buffer) (value, error) {
	var text strings.Builder
	for {
		prefix, err := reader.ReadByte()
		if err != nil {
			return value{}, err
		}
		raw.WriteByte(prefix)
		if prefix != ';' {
			return value{}, fmt.Errorf("invalid RESP streamed string chunk prefix %q", prefix)
		}
		line, err := readLine(reader, raw)
		if err != nil {
			return value{}, err
		}
		length, err := strconv.Atoi(line)
		if err != nil || length < 0 {
			return value{}, fmt.Errorf("invalid RESP streamed string chunk length %q", line)
		}
		if length == 0 {
			return value{kind: '$', text: text.String()}, nil
		}
		if length > maxFrame || raw.Len()+length+2 > maxFrame {
			return value{}, errors.New("RESP frame exceeds 64 MiB")
		}
		data := make([]byte, length+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return value{}, err
		}
		if !bytes.HasSuffix(data, []byte("\r\n")) {
			return value{}, errors.New("invalid RESP streamed string chunk terminator")
		}
		raw.Write(data)
		text.Write(data[:length])
	}
}

func readStreamedItems(reader *bufio.Reader, raw *bytes.Buffer, depth int, kind byte) (value, error) {
	items := make([]value, 0)
	for {
		item, err := readValue(reader, raw, depth+1)
		if err != nil {
			return value{}, err
		}
		if item.kind == '.' {
			if (kind == '%' || kind == '|') && len(items)%2 != 0 {
				return value{}, errors.New("RESP streamed map ended with an unmatched key")
			}
			return value{kind: kind, items: items}, nil
		}
		items = append(items, item)
		if len(items) > maxCollection || raw.Len() > maxFrame {
			return value{}, errors.New("RESP streamed collection exceeds its limit")
		}
	}
}
func readItems(reader *bufio.Reader, raw *bytes.Buffer, depth, count int, kind byte) (value, error) {
	items := make([]value, 0, count)
	for range count {
		item, err := readValue(reader, raw, depth+1)
		if err != nil {
			return value{}, err
		}
		items = append(items, item)
		if raw.Len() > maxFrame {
			return value{}, errors.New("RESP frame exceeds 64 MiB")
		}
	}
	return value{kind: kind, items: items}, nil
}
func readLine(reader *bufio.Reader, raw *bytes.Buffer) (string, error) {
	var line bytes.Buffer
	for {
		fragment, err := reader.ReadSlice('\n')
		if raw.Len()+line.Len()+len(fragment) > maxFrame {
			return "", errors.New("RESP frame exceeds 64 MiB")
		}
		line.Write(fragment)
		if err == nil {
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
	}
	data := line.Bytes()
	raw.Write(data)
	if !bytes.HasSuffix(data, []byte("\r\n")) {
		return "", errors.New("invalid RESP line terminator")
	}
	return string(data[:len(data)-2]), nil
}
func (v value) scalar() string { return v.text }
func (v value) render(limit int) string {
	var out strings.Builder
	renderValue(&out, v)
	text := out.String()
	if len(text) > limit {
		return text[:limit] + "…"
	}
	return text
}
func renderValue(out *strings.Builder, v value) {
	if len(v.items) > 0 {
		open, close := "[", "]"
		if v.kind == '%' || v.kind == '|' {
			open, close = "{", "}"
		}
		out.WriteString(open)
		for i, item := range v.items {
			if i > 0 {
				out.WriteString(", ")
			}
			renderValue(out, item)
		}
		out.WriteString(close)
		return
	}
	if v.kind == '$' || v.kind == '=' {
		out.WriteString(strconv.Quote(v.text))
		return
	}
	if v.kind == '-' || v.kind == '!' {
		out.WriteString("ERR ")
	}
	out.WriteString(v.text)
}
