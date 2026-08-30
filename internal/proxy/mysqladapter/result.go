package mysqladapter

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/erikbooij/portscope/internal/observation"
)

const (
	comQuit         = 0x01
	comInitDB       = 0x02
	comQuery        = 0x03
	comFieldList    = 0x04
	comStatistics   = 0x09
	comPing         = 0x0e
	comChangeUser   = 0x11
	comStmtPrepare  = 0x16
	comStmtExecute  = 0x17
	comStmtLongData = 0x18
	comStmtClose    = 0x19
	comStmtReset    = 0x1a
	comSetOption    = 0x1b
	comStmtFetch    = 0x1c
	comResetConn    = 0x1f
)

type commandInfo struct {
	code        byte
	operation   string
	query       string
	statementID uint32
	request     observation.Payload
	noResponse  bool
}

type responseInfo struct {
	payload     observation.Payload
	outcome     string
	errorText   string
	status      uint16
	statementID uint32
	parameters  int
	columns     int
	rows        int
}

type responseStream struct {
	reader       *bufio.Reader
	destination  net.Conn
	sequence     byte
	capabilities uint32
	size         int64
	truncated    bool
}

func parseCommand(item logicalPacket, statements map[uint32]string) commandInfo {
	request := observation.Payload{Kind: "mysql", Size: item.size, Truncated: item.truncated}
	if len(item.payload) == 0 {
		request.Summary = "empty command"
		return commandInfo{operation: "UNKNOWN", request: request}
	}
	code := item.payload[0]
	result := commandInfo{code: code, operation: commandName(code), request: request}
	switch code {
	case comQuery, comStmtPrepare:
		result.query = string(item.payload[1:])
		result.operation += " " + sqlVerb(result.query)
		result.request.Kind = "sql"
		result.request.Summary = result.operation
		result.request.Text = result.query
	case comInitDB:
		result.operation = "USE"
		result.request.Kind = "text"
		result.request.Text = string(item.payload[1:])
		result.request.Summary = "USE " + string(item.payload[1:])
	case comStmtExecute, comStmtClose, comStmtReset, comStmtFetch, comStmtLongData:
		if len(item.payload) >= 5 {
			result.statementID = binary.LittleEndian.Uint32(item.payload[1:5])
			if query := statements[result.statementID]; query != "" {
				result.operation += " " + sqlVerb(query)
				result.request.Summary = query
			}
		}
		result.request.Text = hexPreview(item.payload, 512)
		result.noResponse = code == comStmtClose || code == comStmtLongData
	case comQuit:
		result.noResponse = true
		result.request.Summary = result.operation
	default:
		result.request.Summary = result.operation
		if len(item.payload) > 1 {
			result.request.Text = hexPreview(item.payload[1:], 512)
		}
	}
	return result
}

func commandName(code byte) string {
	switch code {
	case comQuit:
		return "QUIT"
	case comInitDB:
		return "INIT DB"
	case comQuery:
		return "QUERY"
	case comFieldList:
		return "FIELD LIST"
	case comStatistics:
		return "STATISTICS"
	case comPing:
		return "PING"
	case comChangeUser:
		return "CHANGE USER"
	case comStmtPrepare:
		return "PREPARE"
	case comStmtExecute:
		return "EXECUTE"
	case comStmtLongData:
		return "STMT LONG DATA"
	case comStmtClose:
		return "STMT CLOSE"
	case comStmtReset:
		return "STMT RESET"
	case comSetOption:
		return "SET OPTION"
	case comStmtFetch:
		return "STMT FETCH"
	case comResetConn:
		return "RESET CONNECTION"
	default:
		return fmt.Sprintf("COMMAND 0x%02X", code)
	}
}

func sqlVerb(query string) string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return "EMPTY"
	}
	for index, char := range trimmed {
		if char == ' ' || char == '\t' || char == '\r' || char == '\n' || char == '(' {
			return strings.ToUpper(trimmed[:index])
		}
	}
	return strings.ToUpper(trimmed)
}

func (stream *responseStream) next() (logicalPacket, error) {
	item, err := readLogical(stream.reader, stream.destination, stream.sequence)
	if err != nil {
		return item, err
	}
	stream.sequence = item.nextSequence
	stream.size += item.size
	stream.truncated = stream.truncated || item.truncated
	return item, nil
}

func readCommandResponse(reader *bufio.Reader, client net.Conn, sequence byte, capabilities uint32, command commandInfo) (responseInfo, error) {
	stream := &responseStream{reader: reader, destination: client, sequence: sequence, capabilities: capabilities}
	if command.code == comStmtPrepare {
		return readPrepareResponse(stream)
	}
	if command.code == comFieldList || command.code == comStmtFetch {
		return readUntilTerminator(stream, command.code == comStmtFetch)
	}
	first, err := stream.next()
	if err != nil {
		return responseInfo{}, err
	}
	return readGeneralResponse(stream, first, command)
}

func readGeneralResponse(stream *responseStream, first logicalPacket, command commandInfo) (responseInfo, error) {
	if len(first.payload) == 0 {
		return responseInfo{}, errors.New("empty MySQL response packet")
	}
	switch first.payload[0] {
	case 0xff:
		err := parseMySQLError(first.payload)
		return responseInfo{payload: observation.Payload{Kind: "mysql", Summary: err.Error(), Text: err.Error(), Size: stream.size, Truncated: stream.truncated}, outcome: "error", errorText: err.Error()}, nil
	case 0x00:
		status, affected, lastID, parseErr := parseOK(first.payload, stream.capabilities)
		if parseErr != nil {
			return responseInfo{}, parseErr
		}
		result := responseInfo{payload: observation.Payload{Kind: "mysql", Summary: fmt.Sprintf("OK · %d affected", affected), Text: fmt.Sprintf("affected rows: %d\nlast insert id: %d", affected, lastID), Size: stream.size, Truncated: stream.truncated}, outcome: "ok", status: status}
		if status&serverMoreResultsExists != 0 {
			return readMoreResults(stream, result, command)
		}
		return result, nil
	case 0xfb:
		return responseInfo{}, errors.New("MySQL upstream requested LOCAL INFILE after the proxy disabled that capability")
	}
	if command.code == comStatistics {
		return responseInfo{payload: observation.Payload{Kind: "text", Summary: "server statistics", Text: string(first.payload), Size: stream.size, Truncated: stream.truncated}, outcome: "ok"}, nil
	}
	return readResultset(stream, first, command.code == comQuery)
}

func readResultset(stream *responseStream, first logicalPacket, textRows bool) (responseInfo, error) {
	offset := 0
	columnCount, null, err := readLenenc(first.payload, &offset)
	if err != nil || null || columnCount > 1<<20 {
		return responseInfo{}, errors.New("invalid MySQL resultset column count")
	}
	columns := make([]string, 0, int(columnCount))
	for range int(columnCount) {
		definition, readErr := stream.next()
		if readErr != nil {
			return responseInfo{}, readErr
		}
		columns = append(columns, columnName(definition.payload, len(columns)))
	}
	if stream.capabilities&clientDeprecateEOF == 0 {
		terminator, readErr := stream.next()
		if readErr != nil {
			return responseInfo{}, readErr
		}
		if !isEOFPacket(terminator.payload) {
			return responseInfo{}, errors.New("missing MySQL column-definition terminator")
		}
	}
	rows := make([][]any, 0)
	rowCount := 0
	var status uint16
	for {
		row, readErr := stream.next()
		if readErr != nil {
			return responseInfo{}, readErr
		}
		if len(row.payload) > 0 && row.payload[0] == 0xff {
			err := parseMySQLError(row.payload)
			return responseInfo{payload: observation.Payload{Kind: "mysql", Summary: err.Error(), Text: err.Error(), Size: stream.size, Truncated: stream.truncated}, outcome: "error", errorText: err.Error(), columns: int(columnCount), rows: rowCount}, nil
		}
		if isEOFPacket(row.payload) {
			status, _ = terminatorStatus(row.payload, stream.capabilities)
			break
		}
		rowCount++
		if textRows && len(rows) < 100 {
			if values, decodeErr := decodeTextRow(row.payload, int(columnCount)); decodeErr == nil {
				rows = append(rows, values)
			}
		}
	}
	payload := observation.Payload{Kind: "mysql", Summary: fmt.Sprintf("RESULTSET · %d columns · %d rows", columnCount, rowCount), Size: stream.size, Truncated: stream.truncated}
	if textRows && len(rows) > 0 {
		encoded, _ := json.Marshal(map[string]any{"columns": columns, "rows": rows})
		payload.Kind = "json"
		payload.JSON = encoded
		if len(rows) < rowCount {
			payload.Truncated = true
		}
	}
	result := responseInfo{payload: payload, outcome: "ok", status: status, columns: int(columnCount), rows: rowCount}
	if status&serverMoreResultsExists != 0 {
		return readMoreResults(stream, result, commandInfo{code: comQuery})
	}
	return result, nil
}

func readMoreResults(stream *responseStream, accumulated responseInfo, command commandInfo) (responseInfo, error) {
	resultsets := 1
	for accumulated.status&serverMoreResultsExists != 0 {
		first, err := stream.next()
		if err != nil {
			return responseInfo{}, err
		}
		next, err := readGeneralResponse(stream, first, command)
		if err != nil {
			return responseInfo{}, err
		}
		resultsets++
		accumulated.status = next.status
		accumulated.rows += next.rows
		accumulated.columns += next.columns
		if next.outcome == "error" {
			accumulated.outcome, accumulated.errorText = next.outcome, next.errorText
		}
	}
	accumulated.payload = observation.Payload{Kind: "mysql", Summary: fmt.Sprintf("%d RESULTS · %d rows", resultsets, accumulated.rows), Size: stream.size, Truncated: stream.truncated}
	return accumulated, nil
}

func readPrepareResponse(stream *responseStream) (responseInfo, error) {
	first, err := stream.next()
	if err != nil {
		return responseInfo{}, err
	}
	if len(first.payload) == 0 {
		return responseInfo{}, errors.New("empty MySQL prepare response")
	}
	if first.payload[0] == 0xff {
		parsed := parseMySQLError(first.payload)
		return responseInfo{payload: observation.Payload{Kind: "mysql", Summary: parsed.Error(), Text: parsed.Error(), Size: stream.size}, outcome: "error", errorText: parsed.Error()}, nil
	}
	if first.payload[0] != 0x00 || len(first.payload) < 12 {
		return responseInfo{}, errors.New("invalid MySQL prepare response")
	}
	statementID := binary.LittleEndian.Uint32(first.payload[1:5])
	columns := int(binary.LittleEndian.Uint16(first.payload[5:7]))
	parameters := int(binary.LittleEndian.Uint16(first.payload[7:9]))
	for range parameters {
		if _, err := stream.next(); err != nil {
			return responseInfo{}, err
		}
	}
	if parameters > 0 && stream.capabilities&clientDeprecateEOF == 0 {
		if _, err := stream.next(); err != nil {
			return responseInfo{}, err
		}
	}
	for range columns {
		if _, err := stream.next(); err != nil {
			return responseInfo{}, err
		}
	}
	if columns > 0 && stream.capabilities&clientDeprecateEOF == 0 {
		if _, err := stream.next(); err != nil {
			return responseInfo{}, err
		}
	}
	summary := fmt.Sprintf("PREPARED · id %d · %d params · %d columns", statementID, parameters, columns)
	return responseInfo{payload: observation.Payload{Kind: "mysql", Summary: summary, Text: summary, Size: stream.size, Truncated: stream.truncated}, outcome: "ok", statementID: statementID, parameters: parameters, columns: columns}, nil
}

func readUntilTerminator(stream *responseStream, binaryRows bool) (responseInfo, error) {
	count := 0
	for {
		item, err := stream.next()
		if err != nil {
			return responseInfo{}, err
		}
		if len(item.payload) > 0 && item.payload[0] == 0xff {
			parsed := parseMySQLError(item.payload)
			return responseInfo{payload: observation.Payload{Kind: "mysql", Summary: parsed.Error(), Text: parsed.Error(), Size: stream.size}, outcome: "error", errorText: parsed.Error()}, nil
		}
		if isEOFPacket(item.payload) {
			break
		}
		count++
	}
	label := "definitions"
	if binaryRows {
		label = "rows"
	}
	return responseInfo{payload: observation.Payload{Kind: "mysql", Summary: fmt.Sprintf("%d %s", count, label), Size: stream.size, Truncated: stream.truncated}, outcome: "ok", rows: count}, nil
}

func parseOK(payload []byte, capabilities uint32) (status uint16, affected, lastID uint64, err error) {
	if len(payload) < 1 || (payload[0] != 0x00 && payload[0] != 0xfe) {
		return 0, 0, 0, errors.New("not a MySQL OK packet")
	}
	offset := 1
	affected, _, err = readLenenc(payload, &offset)
	if err != nil {
		return
	}
	lastID, _, err = readLenenc(payload, &offset)
	if err != nil {
		return
	}
	if capabilities&clientProtocol41 != 0 {
		if len(payload)-offset < 4 {
			err = io.ErrUnexpectedEOF
			return
		}
		status = binary.LittleEndian.Uint16(payload[offset:])
	} else if capabilities&clientTransactions != 0 {
		if len(payload)-offset < 2 {
			err = io.ErrUnexpectedEOF
			return
		}
		status = binary.LittleEndian.Uint16(payload[offset:])
	}
	return
}

func terminatorStatus(payload []byte, capabilities uint32) (uint16, error) {
	if len(payload) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if payload[0] == 0xfe && len(payload) < 9 && capabilities&clientDeprecateEOF == 0 {
		if len(payload) < 5 {
			return 0, io.ErrUnexpectedEOF
		}
		return binary.LittleEndian.Uint16(payload[3:5]), nil
	}
	status, _, _, err := parseOK(payload, capabilities)
	return status, err
}

func isEOFPacket(payload []byte) bool {
	return len(payload) > 0 && payload[0] == 0xfe && len(payload) < 9
}

func columnName(payload []byte, fallback int) string {
	offset := 0
	for index := 0; index < 6; index++ {
		value, _, err := readLenencBytes(payload, &offset)
		if err != nil {
			return "column_" + strconv.Itoa(fallback+1)
		}
		if index == 4 && len(value) > 0 {
			return string(value)
		}
	}
	return "column_" + strconv.Itoa(fallback+1)
}

func decodeTextRow(payload []byte, columns int) ([]any, error) {
	values := make([]any, 0, columns)
	offset := 0
	for range columns {
		value, null, err := readLenencBytes(payload, &offset)
		if err != nil {
			return nil, err
		}
		if null {
			values = append(values, nil)
		} else {
			values = append(values, string(value))
		}
	}
	if offset != len(payload) {
		return nil, errors.New("MySQL text row has trailing data")
	}
	return values, nil
}

func hexPreview(data []byte, limit int) string {
	truncated := len(data) > limit
	if truncated {
		data = data[:limit]
	}
	result := hex.EncodeToString(data)
	if truncated {
		result += "…"
	}
	return result
}
