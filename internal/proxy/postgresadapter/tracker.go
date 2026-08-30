package postgresadapter

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type statement struct {
	query string
	types []uint32
}

type portal struct {
	statement string
	query     string
	params    []any
	size      int64
	truncated bool
}

type column struct {
	Name   string `json:"name"`
	OID    uint32 `json:"typeOid"`
	Format string `json:"format"`
}

type pending struct {
	simple       bool
	started      time.Time
	operation    string
	request      observation.Payload
	rows         [][]any
	columns      []column
	rowCount     int
	columnCount  int
	commandTags  []string
	responseSize int64
	captureBytes int
	truncated    bool
	errorText    string
	errorCode    string
}

const (
	maxCapturedValues = 1024
	maxCapturedValue  = 4096
	maxCapturedRaw    = captureLimit / 2
)

type tracker struct {
	mu         sync.Mutex
	upstream   config.Upstream
	sink       observation.Sink
	connection string
	tlsDown    bool
	tlsUp      bool
	statements map[string]statement
	portals    map[string]portal
	pending    []*pending
	deferred   *pendingError
}

type pendingError struct {
	code string
	text string
}

func newTracker(upstream config.Upstream, sink observation.Sink, connection string, tlsDown, tlsUp bool) *tracker {
	return &tracker{upstream: upstream, sink: sink, connection: connection, tlsDown: tlsDown, tlsUp: tlsUp, statements: make(map[string]statement), portals: make(map[string]portal)}
}

func (state *tracker) frontend(item message) {
	state.mu.Lock()
	defer state.mu.Unlock()
	switch item.typ {
	case 'Q':
		query := strings.TrimSuffix(string(item.body), "\x00")
		state.pending = append(state.pending, &pending{simple: true, started: time.Now(), operation: sqlOperation(query), request: observation.Payload{Kind: "sql", Summary: sqlOperation(query), Text: boundedText(query), Size: int64(len(item.body) + 5), Truncated: len(query) > captureLimit}})
	case 'P':
		name, query, types, ok := parseParse(item.body)
		if ok {
			state.statements[name] = statement{query: query, types: types}
		}
	case 'B':
		portalName, statementName, parameters, truncated, ok := parseBind(item.body)
		if ok {
			prepared := state.statements[statementName]
			state.portals[portalName] = portal{statement: statementName, query: prepared.query, params: parameters, size: int64(len(item.body) + 5), truncated: truncated}
		}
	case 'E':
		offset := 0
		portalName, err := readCString(item.body, &offset)
		if err != nil {
			return
		}
		bound := state.portals[portalName]
		query := bound.query
		request := observation.Payload{Kind: "sql", Summary: sqlOperation(query), Text: boundedText(query), Size: bound.size + int64(len(item.body)+5), Truncated: len(query) > captureLimit}
		if len(bound.params) > 0 {
			encoded, _ := json.Marshal(map[string]any{"statement": bound.statement, "portal": portalName, "query": query, "parameters": bound.params, "captureTruncated": bound.truncated})
			if len(encoded) > captureLimit {
				encoded, _ = json.Marshal(map[string]any{"statement": bound.statement, "portal": portalName, "query": boundedText(query), "parameterCount": len(bound.params), "captureTruncated": true})
				bound.truncated = true
			}
			request.Kind, request.JSON = "json", encoded
		}
		request.Truncated = request.Truncated || bound.truncated
		queued := &pending{started: time.Now(), operation: sqlOperation(query), request: request}
		if state.deferred != nil {
			queued.errorCode, queued.errorText = state.deferred.code, state.deferred.text
			state.deferred = nil
		}
		state.pending = append(state.pending, queued)
	case 'd':
		if len(state.pending) > 0 {
			state.pending[len(state.pending)-1].request.Size += int64(len(item.body) + 5)
		}
	case 'C':
		if len(item.body) > 1 {
			offset := 1
			name, err := readCString(item.body, &offset)
			if err == nil {
				if item.body[0] == 'S' {
					delete(state.statements, name)
				} else if item.body[0] == 'P' {
					delete(state.portals, name)
				}
			}
		}
	}
}

func (state *tracker) backend(item message) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if item.typ == 'A' {
		state.recordNotification(item)
		return
	}
	if item.typ == 'E' && len(state.pending) == 0 {
		_, code, text := parseError(item.body)
		state.deferred = &pendingError{code: code, text: text}
		return
	}
	if len(state.pending) == 0 {
		return
	}
	current := state.pending[0]
	current.responseSize += int64(len(item.body) + 5)
	switch item.typ {
	case 'T':
		if columns, count, truncated, ok := parseRowDescription(item.body); ok {
			current.columns = columns
			current.columnCount = count
			current.truncated = current.truncated || truncated
		}
	case 'D':
		current.rowCount++
		if len(current.rows) >= 100 {
			current.truncated = true
			return
		}
		if row, captured, truncated, ok := parseDataRow(item.body, current.columns, maxCapturedRaw-current.captureBytes); ok {
			current.rows = append(current.rows, row)
			current.captureBytes += captured
			current.truncated = current.truncated || truncated
		}
	case 'C':
		tag := strings.TrimSuffix(string(item.body), "\x00")
		current.commandTags = append(current.commandTags, tag)
		if !current.simple {
			state.finishCurrent("ok")
		}
	case 's', 'I':
		if !current.simple {
			state.finishCurrent("ok")
		}
	case 'E':
		_, code, text := parseError(item.body)
		current.errorCode, current.errorText = code, text
		state.finishCurrent("error")
	case 'Z':
		if current.simple || current.errorText != "" {
			outcome := "ok"
			if current.errorText != "" {
				outcome = "error"
			}
			state.finishCurrent(outcome)
		}
		state.deferred = nil
	}
}

func (state *tracker) finishCurrent(outcome string) {
	current := state.pending[0]
	state.pending = state.pending[1:]
	response := observation.Payload{Kind: "postgres", Size: current.responseSize, Truncated: current.truncated}
	if current.errorText != "" {
		response.Summary, response.Text = current.errorText, current.errorText
	} else if len(current.columns) > 0 || len(current.rows) > 0 {
		encoded, _ := json.Marshal(map[string]any{"columns": current.columns, "rows": current.rows, "commandTags": current.commandTags})
		if len(encoded) > captureLimit {
			encoded, _ = json.Marshal(map[string]any{"rowCount": current.rowCount, "columnCount": current.columnCount, "commandTags": current.commandTags, "captureTruncated": true})
			current.truncated = true
		}
		response.Kind, response.JSON = "json", encoded
		response.Summary = fmt.Sprintf("%d rows · %d columns", current.rowCount, current.columnCount)
	} else if len(current.commandTags) > 0 {
		response.Summary = strings.Join(current.commandTags, " · ")
		response.Text = response.Summary
	} else {
		response.Summary = "complete"
	}
	response.Truncated = current.truncated
	attributes := map[string]string{
		"downstreamTLS": strconv.FormatBool(state.tlsDown),
		"upstreamTLS":   strconv.FormatBool(state.tlsUp),
		"rows":          strconv.Itoa(current.rowCount),
	}
	if current.errorCode != "" {
		attributes["sqlstate"] = current.errorCode
	}
	state.sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: state.upstream.ID, Protocol: "postgres", Connection: state.connection,
		Operation: current.operation, StartedAt: current.started, DurationUS: time.Since(current.started).Microseconds(), Outcome: outcome,
		Error: current.errorText, Request: current.request, Response: response, Attributes: attributes,
	})
}

func (state *tracker) recordNotification(item message) {
	offset := 4
	channel, err := readCString(item.body, &offset)
	if err != nil {
		return
	}
	payload, _ := readCString(item.body, &offset)
	state.sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: state.upstream.ID, Protocol: "postgres", Connection: state.connection,
		Operation: "NOTIFY", StartedAt: time.Now(), Outcome: "ok",
		Request: observation.Payload{Kind: "postgres", Summary: channel}, Response: observation.Payload{Kind: "text", Summary: payload, Text: payload, Size: int64(len(item.body) + 5)},
	})
}

func parseParse(body []byte) (string, string, []uint32, bool) {
	offset := 0
	name, err := readCString(body, &offset)
	if err != nil {
		return "", "", nil, false
	}
	query, err := readCString(body, &offset)
	if err != nil {
		return "", "", nil, false
	}
	count, err := int16At(body, offset)
	offset += 2
	if err != nil || count < 0 || int(count) > (len(body)-offset)/4 {
		return "", "", nil, false
	}
	types := make([]uint32, count)
	for index := range types {
		value, _ := int32At(body, offset)
		offset += 4
		types[index] = uint32(value)
	}
	return name, query, types, offset == len(body)
}

func parseBind(body []byte) (string, string, []any, bool, bool) {
	offset := 0
	portalName, err := readCString(body, &offset)
	if err != nil {
		return "", "", nil, false, false
	}
	statementName, err := readCString(body, &offset)
	if err != nil {
		return "", "", nil, false, false
	}
	formatCount, err := int16At(body, offset)
	offset += 2
	if err != nil || formatCount < 0 || int(formatCount) > (len(body)-offset)/2 {
		return "", "", nil, false, false
	}
	formats := make([]int16, formatCount)
	for index := range formats {
		formats[index], _ = int16At(body, offset)
		offset += 2
	}
	parameterCount, err := int16At(body, offset)
	offset += 2
	if err != nil || parameterCount < 0 || parameterCount > 32767 {
		return "", "", nil, false, false
	}
	parameters := make([]any, 0, min(int(parameterCount), maxCapturedValues))
	captured := 0
	truncated := false
	for index := 0; index < int(parameterCount); index++ {
		length, err := int32At(body, offset)
		offset += 4
		if err != nil || length < -1 || (length >= 0 && int(length) > len(body)-offset) {
			return "", "", nil, false, false
		}
		if length == -1 {
			if len(parameters) < maxCapturedValues {
				parameters = append(parameters, nil)
			} else {
				truncated = true
			}
			continue
		}
		value := body[offset : offset+int(length)]
		offset += int(length)
		format := int16(0)
		if len(formats) == 1 {
			format = formats[0]
		} else if len(formats) == int(parameterCount) {
			format = formats[index]
		}
		if len(parameters) < maxCapturedValues && captured < maxCapturedRaw {
			parameters = append(parameters, capturedValue(value, format))
			captured += min(len(value), maxCapturedValue)
			truncated = truncated || len(value) > maxCapturedValue
		} else {
			truncated = true
		}
	}
	return portalName, statementName, parameters, truncated, true
}

func parseRowDescription(body []byte) ([]column, int, bool, bool) {
	count, err := int16At(body, 0)
	if err != nil || count < 0 {
		return nil, 0, false, false
	}
	offset := 2
	columns := make([]column, 0, min(int(count), maxCapturedValues))
	captured := 0
	truncated := false
	for range int(count) {
		name, err := readCString(body, &offset)
		if err != nil || len(body)-offset < 18 {
			return nil, 0, false, false
		}
		oid, _ := int32At(body, offset+6)
		format, _ := int16At(body, offset+16)
		offset += 18
		formatName := "text"
		if format == 1 {
			formatName = "binary"
		}
		if len(columns) < maxCapturedValues && captured < maxCapturedRaw {
			boundedName := name
			if len(boundedName) > maxCapturedValue {
				boundedName = boundedName[:maxCapturedValue]
				truncated = true
			}
			columns = append(columns, column{Name: boundedName, OID: uint32(oid), Format: formatName})
			captured += len(boundedName)
		} else {
			truncated = true
		}
	}
	return columns, int(count), truncated, offset == len(body)
}

func parseDataRow(body []byte, columns []column, budget int) ([]any, int, bool, bool) {
	count, err := int16At(body, 0)
	if err != nil || count < 0 {
		return nil, 0, false, false
	}
	offset := 2
	values := make([]any, 0, min(int(count), maxCapturedValues))
	captured := 0
	truncated := false
	for index := 0; index < int(count); index++ {
		length, err := int32At(body, offset)
		offset += 4
		if err != nil || length < -1 || (length >= 0 && int(length) > len(body)-offset) {
			return nil, 0, false, false
		}
		if length == -1 {
			if len(values) < maxCapturedValues {
				values = append(values, nil)
			} else {
				truncated = true
			}
			continue
		}
		format := int16(0)
		if index < len(columns) && columns[index].Format == "binary" {
			format = 1
		}
		if len(values) < maxCapturedValues && captured < budget {
			value := body[offset : offset+int(length)]
			values = append(values, capturedValue(value, format))
			captured += min(len(value), maxCapturedValue)
			truncated = truncated || len(value) > maxCapturedValue
		} else {
			truncated = true
		}
		offset += int(length)
	}
	return values, captured, truncated, offset == len(body)
}

func capturedValue(value []byte, format int16) any {
	if len(value) > maxCapturedValue {
		preview := value[:maxCapturedValue]
		if format == 0 {
			return map[string]any{"preview": string(preview), "size": len(value), "truncated": true}
		}
		return map[string]any{"hexPreview": hex.EncodeToString(preview), "size": len(value), "truncated": true, "format": "binary"}
	}
	if format == 0 {
		return string(value)
	}
	return map[string]any{"hex": hex.EncodeToString(value), "size": len(value), "format": "binary"}
}

func sqlOperation(query string) string {
	trimmed := strings.TrimLeft(strings.TrimSpace(query), ";")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return "QUERY EMPTY"
	}
	end := len(trimmed)
	for index, char := range trimmed {
		if char == ' ' || char == '\t' || char == '\r' || char == '\n' || char == '(' || char == ';' {
			end = index
			break
		}
	}
	return "QUERY " + strings.ToUpper(trimmed[:end])
}

func boundedText(value string) string {
	if len(value) <= captureLimit {
		return value
	}
	return value[:captureLimit]
}
