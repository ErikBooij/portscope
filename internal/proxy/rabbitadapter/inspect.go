package rabbitadapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

const amqpCaptureLimit = 256 * 1024

type amqpDirection byte

const (
	clientToServer amqpDirection = iota
	serverToClient
)

type methodInfo struct {
	classID, methodID             uint16
	name                          string
	operation                     string
	arguments                     map[string]any
	carriesContent                bool
	expectedClass, expectedMethod uint16
}

type methodEvent struct {
	frame   amqpFrame
	info    methodInfo
	started time.Time
}

type contentKey struct {
	direction amqpDirection
	channel   uint16
}

type contentState struct {
	event       methodEvent
	request     *methodEvent
	bodySize    uint64
	received    uint64
	capture     []byte
	properties  []observation.Pair
	contentType string
	frameBytes  int64
}

type amqpInspector struct {
	mu         sync.Mutex
	upstream   config.Upstream
	sink       observation.Sink
	connection string
	context    map[string]string
	pending    map[uint16][]methodEvent
	content    map[contentKey]*contentState
}

func newAMQPInspector(upstream config.Upstream, sink observation.Sink, connection string, negotiated negotiatedConnection) *amqpInspector {
	context := map[string]string{
		"downstreamTLS": strconv.FormatBool(negotiated.downstreamTLS), "upstreamTLS": strconv.FormatBool(negotiated.upstreamTLS),
		"heartbeatSeconds": strconv.Itoa(int(negotiated.heartbeat)),
	}
	if negotiated.serverVersion != "" {
		context["serverVersion"] = negotiated.serverVersion
	}
	return &amqpInspector{upstream: upstream, sink: sink, connection: connection, context: context, pending: make(map[uint16][]methodEvent), content: make(map[contentKey]*contentState)}
}

func (inspector *amqpInspector) observe(direction amqpDirection, frame amqpFrame) {
	if frame.typeID == frameHeartbeat {
		return
	}
	inspector.mu.Lock()
	defer inspector.mu.Unlock()
	key := contentKey{direction: direction, channel: frame.channel}
	switch frame.typeID {
	case frameMethod:
		info := inspectMethod(frame)
		event := methodEvent{frame: frame, info: info, started: time.Now()}
		if info.carriesContent {
			if incomplete := inspector.content[key]; incomplete != nil {
				inspector.finishContent(incomplete, errors.New("AMQP content method arrived before the previous content completed"))
			}
			state := &contentState{event: event, frameBytes: int64(len(frame.raw))}
			if direction == serverToClient && info.classID == 60 && info.methodID == 71 {
				if request, found := inspector.takePending(frame.channel, info); found {
					state.request = &request
					state.event.started = request.started
				}
			}
			inspector.content[key] = state
			return
		}
		if direction == clientToServer && info.expectedClass != 0 && !boolArgument(info.arguments, "noWait") {
			inspector.pending[frame.channel] = append(inspector.pending[frame.channel], event)
			return
		}
		if direction == serverToClient {
			if request, found := inspector.takePending(frame.channel, info); found {
				inspector.recordRPC(request, event, nil)
				return
			}
		}
		inspector.recordMethod(direction, event, nil)
	case frameHeader:
		state := inspector.content[key]
		if state == nil {
			inspector.recordFrameError(direction, frame, errors.New("unexpected AMQP content header"))
			return
		}
		bodySize, contentType, properties, err := inspectContentHeader(frame)
		if err != nil {
			delete(inspector.content, key)
			inspector.finishContent(state, err)
			return
		}
		state.bodySize, state.contentType, state.properties = bodySize, contentType, properties
		state.frameBytes += int64(len(frame.raw))
		if bodySize == 0 {
			delete(inspector.content, key)
			inspector.finishContent(state, nil)
		}
	case frameBody:
		state := inspector.content[key]
		if state == nil || state.bodySize == 0 {
			inspector.recordFrameError(direction, frame, errors.New("unexpected AMQP content body"))
			return
		}
		state.received += uint64(len(frame.payload))
		state.frameBytes += int64(len(frame.raw))
		if len(state.capture) < amqpCaptureLimit {
			remaining := amqpCaptureLimit - len(state.capture)
			state.capture = append(state.capture, frame.payload[:min(remaining, len(frame.payload))]...)
		}
		if state.received >= state.bodySize {
			delete(inspector.content, key)
			var err error
			if state.received != state.bodySize {
				err = fmt.Errorf("AMQP content received %d bytes, expected %d", state.received, state.bodySize)
			}
			inspector.finishContent(state, err)
		}
	default:
		inspector.recordFrameError(direction, frame, fmt.Errorf("unknown AMQP frame type %d", frame.typeID))
	}
}

func (inspector *amqpInspector) takePending(channel uint16, response methodInfo) (methodEvent, bool) {
	queue := inspector.pending[channel]
	for index, request := range queue {
		if request.info.expectedClass == response.classID && (request.info.expectedMethod == response.methodID || request.info.expectedMethod == 0 && response.classID == 60 && (response.methodID == 71 || response.methodID == 72)) {
			inspector.pending[channel] = append(queue[:index], queue[index+1:]...)
			return request, true
		}
	}
	return methodEvent{}, false
}

func (inspector *amqpInspector) finishContent(state *contentState, contentErr error) {
	body := amqpBodyPayload(state.capture, state.bodySize, state.contentType, state.properties)
	attributes := inspector.attributes(state.event.frame.channel, state.event.info)
	attributes["bodyBytes"] = strconv.FormatUint(state.bodySize, 10)
	attributes["frameBytes"] = strconv.FormatInt(state.frameBytes, 10)
	if state.contentType != "" {
		attributes["contentType"] = state.contentType
	}
	outcome, errorText := "ok", ""
	if contentErr != nil {
		outcome, errorText = "error", contentErr.Error()
	}
	if state.event.info.classID == 60 && state.event.info.methodID == 50 {
		outcome = "error"
		errorText = fmt.Sprintf("message returned: %v %v", state.event.info.arguments["replyCode"], state.event.info.arguments["replyText"])
	}
	interaction := observation.Interaction{ID: observation.NewID(), UpstreamID: inspector.upstream.ID, Protocol: "rabbitmq", Connection: inspector.connection, Operation: state.event.info.operation, StartedAt: state.event.started, DurationUS: time.Since(state.event.started).Microseconds(), Outcome: outcome, Error: errorText, Attributes: attributes}
	if state.request != nil {
		interaction.Request = methodPayload(*state.request)
		interaction.Response = body
		interaction.Operation = state.request.info.operation
	} else if state.event.info.methodID == 40 && state.event.info.classID == 60 {
		interaction.Request = body
		interaction.Response = observation.Payload{Kind: "text", Summary: "published"}
	} else {
		interaction.Request = observation.Payload{Kind: "text", Summary: "broker event"}
		interaction.Response = body
	}
	inspector.sink.Record(interaction)
}

func (inspector *amqpInspector) recordRPC(request, response methodEvent, forced error) {
	outcome, errorText := "ok", ""
	if forced != nil {
		outcome, errorText = "error", forced.Error()
	}
	inspector.sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: inspector.upstream.ID, Protocol: "rabbitmq", Connection: inspector.connection,
		Operation: request.info.operation, StartedAt: request.started, DurationUS: time.Since(request.started).Microseconds(), Outcome: outcome, Error: errorText,
		Request: methodPayload(request), Response: methodPayload(response), Attributes: inspector.attributes(request.frame.channel, request.info),
	})
}

func (inspector *amqpInspector) recordMethod(direction amqpDirection, event methodEvent, forced error) {
	outcome, errorText := "ok", ""
	if forced != nil {
		outcome, errorText = "error", forced.Error()
	}
	if event.info.classID == 10 && event.info.methodID == 50 || event.info.classID == 20 && event.info.methodID == 40 {
		outcome = "error"
		errorText = fmt.Sprintf("%v %v", event.info.arguments["replyCode"], event.info.arguments["replyText"])
	}
	payload := methodPayload(event)
	interaction := observation.Interaction{ID: observation.NewID(), UpstreamID: inspector.upstream.ID, Protocol: "rabbitmq", Connection: inspector.connection, Operation: event.info.operation, StartedAt: event.started, DurationUS: time.Since(event.started).Microseconds(), Outcome: outcome, Error: errorText, Attributes: inspector.attributes(event.frame.channel, event.info)}
	if direction == clientToServer {
		interaction.Request, interaction.Response = payload, observation.Payload{Kind: "text", Summary: "sent"}
	} else {
		interaction.Request, interaction.Response = observation.Payload{Kind: "text", Summary: "broker event"}, payload
	}
	inspector.sink.Record(interaction)
}

func (inspector *amqpInspector) recordFrameError(direction amqpDirection, frame amqpFrame, err error) {
	event := methodEvent{frame: frame, info: methodInfo{name: "frame", operation: "FRAME"}, started: time.Now()}
	inspector.recordMethod(direction, event, err)
}

func (inspector *amqpInspector) observeBlocked(frame amqpFrame, err error) {
	inspector.mu.Lock()
	defer inspector.mu.Unlock()
	event := methodEvent{frame: frame, info: inspectMethod(frame), started: time.Now()}
	inspector.recordMethod(clientToServer, event, err)
}

func (inspector *amqpInspector) failPending(err error) {
	inspector.mu.Lock()
	defer inspector.mu.Unlock()
	for channel, queue := range inspector.pending {
		for _, request := range queue {
			inspector.recordRPC(request, methodEvent{}, err)
		}
		delete(inspector.pending, channel)
	}
	for key, content := range inspector.content {
		inspector.finishContent(content, err)
		delete(inspector.content, key)
	}
}

func (inspector *amqpInspector) attributes(channel uint16, info methodInfo) map[string]string {
	attributes := make(map[string]string, len(inspector.context)+len(info.arguments)+3)
	for key, value := range inspector.context {
		attributes[key] = value
	}
	attributes["channel"] = strconv.Itoa(int(channel))
	attributes["method"] = info.name
	for _, key := range []string{"exchange", "routingKey", "queue", "consumerTag", "deliveryTag", "messageCount"} {
		if value, found := info.arguments[key]; found {
			attributes[key] = fmt.Sprint(value)
		}
	}
	return attributes
}

func methodPayload(event methodEvent) observation.Payload {
	if event.info.name == "connection.update-secret" || event.info.name == "connection.start-ok" {
		return observation.Payload{Kind: "json", Summary: event.info.name + " · credentials [redacted]", JSON: []byte(`{"authentication":"[redacted]"}`), Size: int64(len(event.frame.raw))}
	}
	encoded, _ := json.Marshal(event.info.arguments)
	return observation.Payload{Kind: "json", Summary: event.info.name, JSON: encoded, Size: int64(len(event.frame.raw))}
}

func amqpBodyPayload(capture []byte, size uint64, contentType string, headers []observation.Pair) observation.Payload {
	result := observation.Payload{Kind: "text", Summary: fmt.Sprintf("%d-byte message", size), Text: string(capture), Size: int64(size), Truncated: size > uint64(len(capture)), Headers: headers}
	if strings.Contains(strings.ToLower(contentType), "json") && json.Valid(capture) && !result.Truncated {
		result.Kind, result.JSON, result.Text = "json", append([]byte(nil), capture...), ""
	} else if !utf8.Valid(capture) || strings.IndexByte(string(capture), 0) >= 0 {
		result.Kind, result.Text = "bytes", fmt.Sprintf("<%d bytes>", size)
	}
	return result
}

func inspectContentHeader(frame amqpFrame) (uint64, string, []observation.Pair, error) {
	if len(frame.payload) < 14 {
		return 0, "", nil, errors.New("short AMQP content header")
	}
	cursor := newCursor(frame.payload)
	classID, _ := cursor.short()
	weight, _ := cursor.short()
	bodySize, _ := cursor.longlong()
	if classID != 60 || weight != 0 {
		return 0, "", nil, errors.New("unsupported AMQP content header class")
	}
	flags, err := cursor.short()
	if err != nil {
		return 0, "", nil, err
	}
	if flags&1 != 0 {
		return 0, "", nil, errors.New("unsupported AMQP content property flag continuation")
	}
	properties := make([]observation.Pair, 0, 14)
	contentType := ""
	readString := func(name string) error {
		value, err := cursor.shortstr()
		if err == nil {
			properties = append(properties, observation.Pair{Name: name, Value: value})
		}
		return err
	}
	for bit, field := range []string{"content-type", "content-encoding", "headers", "delivery-mode", "priority", "correlation-id", "reply-to", "expiration", "message-id", "timestamp", "type", "user-id", "app-id", "cluster-id"} {
		mask := uint16(1 << (15 - bit))
		if flags&mask == 0 {
			continue
		}
		switch field {
		case "content-type", "content-encoding", "correlation-id", "reply-to", "expiration", "message-id", "type", "user-id", "app-id", "cluster-id":
			if err := readString(field); err != nil {
				return 0, "", nil, err
			}
			if field == "content-type" {
				contentType = properties[len(properties)-1].Value
			}
		case "headers":
			raw, err := cursor.tableRaw()
			if err != nil {
				return 0, "", nil, err
			}
			table, tableErr := decodeTable(raw)
			if tableErr != nil {
				properties = append(properties, observation.Pair{Name: "headers", Value: "<undecodable>"})
			} else {
				for name, value := range table {
					properties = append(properties, observation.Pair{Name: "header." + name, Value: fmt.Sprint(value)})
				}
			}
		case "delivery-mode", "priority":
			value, err := cursor.octet()
			if err != nil {
				return 0, "", nil, err
			}
			properties = append(properties, observation.Pair{Name: field, Value: strconv.Itoa(int(value))})
		case "timestamp":
			value, err := cursor.longlong()
			if err != nil {
				return 0, "", nil, err
			}
			properties = append(properties, observation.Pair{Name: field, Value: time.Unix(int64(value), 0).UTC().Format(time.RFC3339)})
		}
	}
	return bodySize, contentType, properties, nil
}

func boolArgument(arguments map[string]any, name string) bool {
	value, _ := arguments[name].(bool)
	return value
}

func inspectMethod(frame amqpFrame) methodInfo {
	classID, methodID, ok := methodID(frame)
	if !ok {
		return methodInfo{name: "unknown", operation: "UNKNOWN", arguments: map[string]any{"bytes": len(frame.payload)}}
	}
	info := methodInfo{classID: classID, methodID: methodID, name: methodName(classID, methodID), arguments: make(map[string]any)}
	info.operation = strings.ToUpper(strings.ReplaceAll(info.name, ".", " "))
	parseMethodArguments(&info, newCursor(frame.payload[4:]))
	decorateMethod(&info)
	return info
}

func decorateMethod(info *methodInfo) {
	if info.classID == 60 && (info.methodID == 40 || info.methodID == 50 || info.methodID == 60 || info.methodID == 71) {
		info.carriesContent = true
	}
	switch {
	case info.classID == 60 && info.methodID == 40:
		info.operation = fmt.Sprintf("PUBLISH %v → %v", info.arguments["exchange"], info.arguments["routingKey"])
	case info.classID == 60 && info.methodID == 50:
		info.operation = fmt.Sprintf("RETURN %v", info.arguments["replyCode"])
	case info.classID == 60 && info.methodID == 60:
		info.operation = "DELIVER " + fmt.Sprint(info.arguments["consumerTag"])
	case info.classID == 60 && info.methodID == 71:
		info.operation = "GET RESULT"
	case info.classID == 40 && info.methodID == 10:
		info.operation = "DECLARE EXCHANGE " + fmt.Sprint(info.arguments["exchange"])
	case info.classID == 50 && info.methodID == 10:
		info.operation = "DECLARE QUEUE " + fmt.Sprint(info.arguments["queue"])
	case info.classID == 60 && info.methodID == 20:
		info.operation = "CONSUME " + fmt.Sprint(info.arguments["queue"])
	case info.classID == 60 && info.methodID == 70:
		info.operation = "GET " + fmt.Sprint(info.arguments["queue"])
	}
	setExpectedResponse(info)
}

func setExpectedResponse(info *methodInfo) {
	pairs := map[[2]uint16][2]uint16{
		{10, 50}: {10, 51}, {20, 10}: {20, 11}, {20, 20}: {20, 21}, {20, 40}: {20, 41},
		{40, 10}: {40, 11}, {40, 20}: {40, 21}, {40, 30}: {40, 31}, {40, 40}: {40, 51},
		{50, 10}: {50, 11}, {50, 20}: {50, 21}, {50, 30}: {50, 31}, {50, 40}: {50, 41}, {50, 50}: {50, 51},
		{60, 10}: {60, 11}, {60, 20}: {60, 21}, {60, 30}: {60, 31}, {60, 70}: {60, 0}, {60, 110}: {60, 111},
		{85, 10}: {85, 11}, {90, 10}: {90, 11}, {90, 20}: {90, 21}, {90, 30}: {90, 31},
	}
	if expected, found := pairs[[2]uint16{info.classID, info.methodID}]; found {
		info.expectedClass, info.expectedMethod = expected[0], expected[1]
	}
}

func parseMethodArguments(info *methodInfo, cursor *fieldCursor) {
	shortstr := func(name string) {
		if value, err := cursor.shortstr(); err == nil {
			info.arguments[name] = value
		}
	}
	short := func(name string) {
		if value, err := cursor.short(); err == nil {
			info.arguments[name] = value
		}
	}
	long := func(name string) {
		if value, err := cursor.long(); err == nil {
			info.arguments[name] = value
		}
	}
	longlong := func(name string) {
		if value, err := cursor.longlong(); err == nil {
			info.arguments[name] = value
		}
	}
	bits := func(names ...string) {
		if value, err := cursor.octet(); err == nil {
			for index, name := range names {
				info.arguments[name] = value&(1<<index) != 0
			}
		}
	}
	table := func() { _, _ = cursor.tableRaw() }
	switch info.classID {
	case 10:
		switch info.methodID {
		case 50:
			short("replyCode")
			shortstr("replyText")
			short("failedClass")
			short("failedMethod")
		case 60:
			shortstr("reason")
		case 70:
			info.arguments["secret"] = "[redacted]"
		}
	case 20:
		switch info.methodID {
		case 10:
			shortstr("outOfBand")
		case 20, 21:
			bits("active")
		case 40:
			short("replyCode")
			shortstr("replyText")
			short("failedClass")
			short("failedMethod")
		}
	case 40:
		switch info.methodID {
		case 10:
			short("reserved")
			shortstr("exchange")
			shortstr("type")
			bits("passive", "durable", "autoDelete", "internal", "noWait")
			table()
		case 11, 21, 31, 51:
		case 20:
			short("reserved")
			shortstr("exchange")
			bits("ifUnused", "noWait")
		case 30, 40:
			short("reserved")
			shortstr("destination")
			shortstr("source")
			shortstr("routingKey")
			bits("noWait")
			table()
		}
	case 50:
		switch info.methodID {
		case 10:
			short("reserved")
			shortstr("queue")
			bits("passive", "durable", "exclusive", "autoDelete", "noWait")
			table()
		case 11:
			shortstr("queue")
			long("messageCount")
			long("consumerCount")
		case 20:
			short("reserved")
			shortstr("queue")
			shortstr("exchange")
			shortstr("routingKey")
			bits("noWait")
			table()
		case 30:
			short("reserved")
			shortstr("queue")
			bits("noWait")
		case 31, 41:
			long("messageCount")
		case 40:
			short("reserved")
			shortstr("queue")
			bits("ifUnused", "ifEmpty", "noWait")
		case 50:
			short("reserved")
			shortstr("queue")
			shortstr("exchange")
			shortstr("routingKey")
			table()
		}
	case 60:
		switch info.methodID {
		case 10:
			long("prefetchSize")
			short("prefetchCount")
			bits("global")
		case 20:
			short("reserved")
			shortstr("queue")
			shortstr("consumerTag")
			bits("noLocal", "noAck", "exclusive", "noWait")
			table()
		case 21, 31:
			shortstr("consumerTag")
		case 30:
			shortstr("consumerTag")
			bits("noWait")
		case 40:
			short("reserved")
			shortstr("exchange")
			shortstr("routingKey")
			bits("mandatory", "immediate")
		case 50:
			short("replyCode")
			shortstr("replyText")
			shortstr("exchange")
			shortstr("routingKey")
		case 60:
			shortstr("consumerTag")
			longlong("deliveryTag")
			bits("redelivered")
			shortstr("exchange")
			shortstr("routingKey")
		case 70:
			short("reserved")
			shortstr("queue")
			bits("noAck")
		case 71:
			longlong("deliveryTag")
			bits("redelivered")
			shortstr("exchange")
			shortstr("routingKey")
			long("messageCount")
		case 80:
			longlong("deliveryTag")
			bits("multiple")
		case 90:
			longlong("deliveryTag")
			bits("requeue")
		case 100, 110:
			bits("requeue")
		case 120:
			longlong("deliveryTag")
			bits("multiple", "requeue")
		}
	case 85:
		if info.methodID == 10 {
			bits("noWait")
		}
	}
}

func methodName(classID, methodID uint16) string {
	names := map[[2]uint16]string{
		{10, 50}: "connection.close", {10, 51}: "connection.close-ok", {10, 60}: "connection.blocked", {10, 61}: "connection.unblocked", {10, 70}: "connection.update-secret", {10, 71}: "connection.update-secret-ok",
		{20, 10}: "channel.open", {20, 11}: "channel.open-ok", {20, 20}: "channel.flow", {20, 21}: "channel.flow-ok", {20, 40}: "channel.close", {20, 41}: "channel.close-ok",
		{40, 10}: "exchange.declare", {40, 11}: "exchange.declare-ok", {40, 20}: "exchange.delete", {40, 21}: "exchange.delete-ok", {40, 30}: "exchange.bind", {40, 31}: "exchange.bind-ok", {40, 40}: "exchange.unbind", {40, 51}: "exchange.unbind-ok",
		{50, 10}: "queue.declare", {50, 11}: "queue.declare-ok", {50, 20}: "queue.bind", {50, 21}: "queue.bind-ok", {50, 30}: "queue.purge", {50, 31}: "queue.purge-ok", {50, 40}: "queue.delete", {50, 41}: "queue.delete-ok", {50, 50}: "queue.unbind", {50, 51}: "queue.unbind-ok",
		{60, 10}: "basic.qos", {60, 11}: "basic.qos-ok", {60, 20}: "basic.consume", {60, 21}: "basic.consume-ok", {60, 30}: "basic.cancel", {60, 31}: "basic.cancel-ok", {60, 40}: "basic.publish", {60, 50}: "basic.return", {60, 60}: "basic.deliver", {60, 70}: "basic.get", {60, 71}: "basic.get-ok", {60, 72}: "basic.get-empty", {60, 80}: "basic.ack", {60, 90}: "basic.reject", {60, 100}: "basic.recover-async", {60, 110}: "basic.recover", {60, 111}: "basic.recover-ok", {60, 120}: "basic.nack",
		{85, 10}: "confirm.select", {85, 11}: "confirm.select-ok", {90, 10}: "tx.select", {90, 11}: "tx.select-ok", {90, 20}: "tx.commit", {90, 21}: "tx.commit-ok", {90, 30}: "tx.rollback", {90, 31}: "tx.rollback-ok",
	}
	if name, found := names[[2]uint16{classID, methodID}]; found {
		return name
	}
	return fmt.Sprintf("method.%d.%d", classID, methodID)
}
