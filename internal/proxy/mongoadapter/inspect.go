package mongoadapter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const mongoCaptureLimit = 256 * 1024

type pendingRequest struct {
	message commandMessage
	wire    wireMessage
	started time.Time
	context map[string]string
}

func recordMongo(sink observation.Sink, upstream config.Upstream, connection string, request pendingRequest, response wireMessage, responseDocument bson.Raw, forcedError error) {
	command := request.message
	operation, collection := mongoOperation(command)
	attributes := map[string]string{
		"command":   command.name,
		"requestId": strconv.FormatInt(int64(request.wire.requestID), 10),
		"opcode":    mongoOpcodeName(request.wire.opcode),
	}
	for key, value := range request.context {
		attributes[key] = value
	}
	if command.database != "" {
		attributes["database"] = command.database
	}
	if collection != "" {
		attributes["collection"] = collection
	}
	if response.responseTo != 0 {
		attributes["responseTo"] = strconv.FormatInt(int64(response.responseTo), 10)
	}
	outcome := "ok"
	errorText := ""
	if forcedError != nil {
		outcome, errorText = "error", forcedError.Error()
	} else if responseDocument != nil {
		if err := commandError(responseDocument); err != nil {
			outcome, errorText = "error", err.Error()
		}
		responseAttributes(responseDocument, attributes)
	}
	requestPayload := mongoPayload(command.document, command.sequences, int64(len(request.wire.raw)), requestSummary(command, collection), mongoAuthCommand(command.name))
	responsePayload := mongoPayload(responseDocument, nil, int64(len(response.raw)), responseSummary(responseDocument, errorText), mongoAuthCommand(command.name))
	sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "mongodb", Connection: connection,
		Operation: operation, StartedAt: request.started, DurationUS: time.Since(request.started).Microseconds(), Outcome: outcome, Error: errorText,
		Request: requestPayload, Response: responsePayload, Attributes: attributes,
	})
}

func mongoPayload(document bson.Raw, sequences map[string][]bson.Raw, wireSize int64, summary string, redact bool) observation.Payload {
	result := observation.Payload{Kind: "json", Summary: summary, Size: wireSize}
	if document == nil {
		result.Kind = "text"
		return result
	}
	redact = redact || document.Lookup("speculativeAuthenticate").Type != 0
	var encoded []byte
	if redact {
		encoded, _ = json.Marshal(map[string]any{"authentication": "[redacted]"})
	} else {
		var value bson.D
		if err := bson.Unmarshal(document, &value); err == nil {
			for name, documents := range sequences {
				items := make(bson.A, 0, len(documents))
				for _, raw := range documents {
					var item bson.D
					if bson.Unmarshal(raw, &item) == nil {
						items = append(items, item)
					}
				}
				value = append(value, bson.E{Key: name, Value: items})
			}
			encoded, _ = bson.MarshalExtJSON(value, false, false)
		}
	}
	if len(encoded) == 0 {
		result.Kind = "bytes"
		result.Text = fmt.Sprintf("<%d MongoDB wire bytes>", wireSize)
		return result
	}
	if len(encoded) > mongoCaptureLimit {
		result.Kind = "text"
		result.Text = string(encoded[:mongoCaptureLimit])
		result.Truncated = true
		return result
	}
	result.JSON = encoded
	return result
}

func mongoOperation(command commandMessage) (string, string) {
	name := strings.ToUpper(command.name)
	if mongoAuthCommand(command.name) {
		return "AUTH", ""
	}
	collection := ""
	if command.document != nil {
		if value, ok := command.document.Lookup(command.name).StringValueOK(); ok {
			collection = value
		} else if strings.EqualFold(command.name, "getMore") {
			collection, _ = command.document.Lookup("collection").StringValueOK()
		}
	}
	if command.database != "" && collection != "" {
		return name + " " + command.database + "." + collection, collection
	}
	if command.database != "" && !isHandshake(command.name) {
		return name + " " + command.database, collection
	}
	return name, collection
}

func requestSummary(command commandMessage, collection string) string {
	parts := make([]string, 0, 2)
	if command.database != "" {
		parts = append(parts, command.database)
	}
	if collection != "" {
		if len(parts) == 1 {
			parts[0] += "." + collection
		} else {
			parts = append(parts, collection)
		}
	}
	for _, documents := range command.sequences {
		parts = append(parts, fmt.Sprintf("%d documents", len(documents)))
	}
	return strings.Join(parts, " · ")
}

func responseSummary(document bson.Raw, errorText string) string {
	if errorText != "" {
		return errorText
	}
	if document == nil {
		return "no response"
	}
	if cursor, ok := document.Lookup("cursor").DocumentOK(); ok {
		for _, field := range []string{"firstBatch", "nextBatch"} {
			if array, ok := cursor.Lookup(field).ArrayOK(); ok {
				values, _ := array.Values()
				return fmt.Sprintf("%d documents", len(values))
			}
		}
	}
	if count, ok := rawNumber(document.Lookup("n")); ok {
		return fmt.Sprintf("%d affected", int64(count))
	}
	return "ok"
}

func responseAttributes(document bson.Raw, attributes map[string]string) {
	for _, field := range []string{"n", "nModified"} {
		if value, ok := rawNumber(document.Lookup(field)); ok {
			attributes[field] = strconv.FormatInt(int64(value), 10)
		}
	}
	if writeErrors, ok := document.Lookup("writeErrors").ArrayOK(); ok {
		values, _ := writeErrors.Values()
		attributes["writeErrors"] = strconv.Itoa(len(values))
	}
	if cursor, ok := document.Lookup("cursor").DocumentOK(); ok {
		if id, ok := cursor.Lookup("id").Int64OK(); ok {
			attributes["cursorId"] = strconv.FormatInt(id, 10)
		}
		for _, field := range []string{"firstBatch", "nextBatch"} {
			if array, ok := cursor.Lookup(field).ArrayOK(); ok {
				values, _ := array.Values()
				attributes["documents"] = strconv.Itoa(len(values))
			}
		}
	}
}

func mongoAuthCommand(name string) bool {
	return strings.EqualFold(name, "saslStart") || strings.EqualFold(name, "saslContinue") || strings.EqualFold(name, "authenticate")
}

func isHandshake(name string) bool {
	return strings.EqualFold(name, "hello") || strings.EqualFold(name, "isMaster") || strings.EqualFold(name, "ismaster")
}

func mongoOpcodeName(opcode int32) string {
	switch opcode {
	case opMsg:
		return "OP_MSG"
	case opQuery:
		return "OP_QUERY"
	case opReply:
		return "OP_REPLY"
	case opCompressed:
		return "OP_COMPRESSED"
	default:
		return strconv.FormatInt(int64(opcode), 10)
	}
}
