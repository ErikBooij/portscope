package httpadapter

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	maxGRPCDescriptorBytes      = 64 * 1024 * 1024
	maxGRPCSnapshots            = 100
	maxGRPCMessageObservations  = 1000
	maxGRPCFrameErrorCharacters = 4096
)

type grpcProfile struct {
	files      *protoregistry.Files
	descriptor string
}

func loadGRPCProfile(options *config.GRPCOptions) (*grpcProfile, error) {
	if options == nil {
		return nil, errors.New("gRPC settings are required")
	}
	profile := &grpcProfile{descriptor: options.DescriptorSetFile}
	if options.DescriptorSetFile == "" {
		return profile, nil
	}
	file, err := os.Open(options.DescriptorSetFile)
	if err != nil {
		return nil, fmt.Errorf("open gRPC descriptor set: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxGRPCDescriptorBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read gRPC descriptor set: %w", err)
	}
	if len(data) > maxGRPCDescriptorBytes {
		return nil, fmt.Errorf("gRPC descriptor set exceeds %d bytes", maxGRPCDescriptorBytes)
	}
	var descriptorSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(data, &descriptorSet); err != nil {
		return nil, fmt.Errorf("decode gRPC descriptor set: %w", err)
	}
	files, err := protodesc.NewFiles(&descriptorSet)
	if err != nil {
		return nil, fmt.Errorf("link gRPC descriptor set: %w", err)
	}
	profile.files = files
	return profile, nil
}

func (profile *grpcProfile) method(path string) protoreflect.MethodDescriptor {
	if profile == nil || profile.files == nil {
		return nil
	}
	serviceName, methodName := grpcNames(path)
	descriptor, err := profile.files.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil
	}
	return service.Methods().ByName(protoreflect.Name(methodName))
}

type grpcCallCapture struct {
	upstream   config.Upstream
	sink       observation.Sink
	profile    *grpcProfile
	id         string
	connection string
	operation  string
	service    string
	method     string
	started    time.Time
	descriptor protoreflect.MethodDescriptor
	request    *grpcMessageStream
	response   *grpcMessageStream
}

func newGRPCCallCapture(upstream config.Upstream, sink observation.Sink, profile *grpcProfile, id string, request *http.Request, started time.Time) *grpcCallCapture {
	service, method := grpcNames(request.URL.Path)
	call := &grpcCallCapture{upstream: upstream, sink: sink, profile: profile, id: id, connection: request.RemoteAddr, operation: strings.TrimPrefix(request.URL.Path, "/"), service: service, method: method, started: started}
	if call.operation == "" {
		call.operation = "UNKNOWN RPC"
	}
	call.descriptor = profile.method(request.URL.Path)
	requestType, responseType := protoreflect.MessageDescriptor(nil), protoreflect.MessageDescriptor(nil)
	if call.descriptor != nil {
		requestType, responseType = call.descriptor.Input(), call.descriptor.Output()
	}
	format := "proto"
	if strings.Contains(strings.ToLower(request.Header.Get("Content-Type")), "+json") {
		format = "json"
	}
	call.request = newGRPCMessageStream(call, "client_to_upstream", request.Header.Get("Grpc-Encoding"), format, requestType)
	call.response = newGRPCMessageStream(call, "upstream_to_client", "", format, responseType)
	return call
}

func grpcNames(path string) (string, string) {
	trimmed := strings.Trim(path, "/")
	position := strings.LastIndexByte(trimmed, '/')
	if position < 0 {
		return trimmed, ""
	}
	return trimmed[:position], trimmed[position+1:]
}

type grpcMessageSnapshot struct {
	payload    observation.Payload
	compressed bool
	decodeErr  string
}

type grpcMessageStream struct {
	mu          sync.Mutex
	call        *grpcCallCapture
	direction   string
	encoding    string
	format      string
	descriptor  protoreflect.MessageDescriptor
	header      [5]byte
	headerBytes int
	active      bool
	compressed  bool
	length      uint32
	remaining   uint64
	capture     []byte
	started     time.Time
	count       int
	emitted     int
	snapshots   []grpcMessageSnapshot
	truncated   bool
	errors      []string
}

func newGRPCMessageStream(call *grpcCallCapture, direction, encoding, format string, descriptor protoreflect.MessageDescriptor) *grpcMessageStream {
	return &grpcMessageStream{call: call, direction: direction, encoding: strings.ToLower(strings.TrimSpace(encoding)), format: format, descriptor: descriptor}
}

func (stream *grpcMessageStream) setEncoding(encoding string) {
	stream.mu.Lock()
	if stream.count == 0 && stream.headerBytes == 0 && !stream.active {
		stream.encoding = strings.ToLower(strings.TrimSpace(encoding))
	}
	stream.mu.Unlock()
}

func (stream *grpcMessageStream) feed(data []byte) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for len(data) > 0 {
		if !stream.active {
			if stream.headerBytes == 0 {
				stream.started = time.Now()
			}
			count := copy(stream.header[stream.headerBytes:], data)
			stream.headerBytes += count
			data = data[count:]
			if stream.headerBytes < len(stream.header) {
				return
			}
			flag := stream.header[0]
			stream.compressed = flag == 1
			if flag != 0 && flag != 1 {
				stream.addError(fmt.Sprintf("invalid gRPC compressed flag %d", flag))
			}
			stream.length = binary.BigEndian.Uint32(stream.header[1:])
			stream.remaining = uint64(stream.length)
			stream.capture = stream.capture[:0]
			stream.headerBytes = 0
			stream.active = true
			if stream.remaining == 0 {
				stream.completeMessage()
			}
			continue
		}
		consume := min(uint64(len(data)), stream.remaining)
		if len(stream.capture) < captureLimit {
			capture := min(int(consume), captureLimit-len(stream.capture))
			stream.capture = append(stream.capture, data[:capture]...)
		}
		data = data[int(consume):]
		stream.remaining -= consume
		if stream.remaining == 0 {
			stream.completeMessage()
		}
	}
}

func (stream *grpcMessageStream) completeMessage() {
	stream.count++
	truncated := uint64(len(stream.capture)) != uint64(stream.length)
	stream.truncated = stream.truncated || truncated
	payload, decodeErr := stream.messagePayload(stream.capture, stream.length, stream.compressed, truncated)
	snapshot := grpcMessageSnapshot{payload: payload, compressed: stream.compressed, decodeErr: decodeErr}
	if len(stream.snapshots) < maxGRPCSnapshots {
		stream.snapshots = append(stream.snapshots, snapshot)
	} else {
		stream.truncated = true
	}
	if stream.emitted < maxGRPCMessageObservations {
		stream.emitted++
		stream.recordMessage(snapshot, stream.count, stream.started)
	}
	stream.active = false
	stream.remaining = 0
	stream.length = 0
	stream.capture = nil
}

func (stream *grpcMessageStream) messagePayload(capture []byte, length uint32, compressed, truncated bool) (observation.Payload, string) {
	data := append([]byte(nil), capture...)
	decodeErr := ""
	if compressed {
		switch stream.encoding {
		case "gzip":
			if !truncated {
				decoded, decodedTruncated, err := decompressGRPCMessage(data)
				if err != nil {
					decodeErr = err.Error()
				} else {
					data, truncated = decoded, decodedTruncated
				}
			}
		case "", "identity":
			decodeErr = "compressed gRPC message has no compression encoding"
		default:
			decodeErr = "unsupported gRPC compression encoding " + stream.encoding
		}
	}
	payload := observation.Payload{Kind: "grpc", Summary: fmt.Sprintf("%d-byte %s message", length, grpcTypeName(stream.descriptor)), Size: int64(length), Truncated: truncated}
	if decodeErr != "" || truncated {
		payload.Text = fmt.Sprintf("<%d-byte protobuf message>", length)
		return payload, decodeErr
	}
	if stream.format == "json" {
		if json.Valid(data) {
			payload.Kind, payload.JSON = "json", data
			return payload, ""
		}
		return payload, "invalid application/grpc+json message"
	}
	if stream.descriptor == nil {
		payload.Text = fmt.Sprintf("<%d-byte protobuf message>", length)
		return payload, ""
	}
	message := dynamicpb.NewMessage(stream.descriptor)
	if err := proto.Unmarshal(data, message); err != nil {
		payload.Text = fmt.Sprintf("<%d-byte protobuf message>", length)
		return payload, "protobuf decode: " + err.Error()
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		payload.Text = fmt.Sprintf("<%d-byte protobuf message>", length)
		return payload, "protobuf JSON: " + err.Error()
	}
	payload.Kind, payload.JSON = "json", encoded
	return payload, ""
}

func decompressGRPCMessage(data []byte) ([]byte, bool, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, false, fmt.Errorf("gRPC gzip: %w", err)
	}
	decoded, readErr := io.ReadAll(io.LimitReader(reader, captureLimit+1))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, false, fmt.Errorf("gRPC gzip: %w", readErr)
	}
	if closeErr != nil {
		return nil, false, fmt.Errorf("gRPC gzip: %w", closeErr)
	}
	if len(decoded) > captureLimit {
		return decoded[:captureLimit], true, nil
	}
	return decoded, false, nil
}

func grpcTypeName(descriptor protoreflect.MessageDescriptor) string {
	if descriptor == nil {
		return "protobuf"
	}
	return string(descriptor.FullName())
}

func (stream *grpcMessageStream) recordMessage(snapshot grpcMessageSnapshot, index int, started time.Time) {
	attributes := stream.call.attributes()
	attributes["callId"] = stream.call.id
	attributes["direction"] = stream.direction
	attributes["messageIndex"] = strconv.Itoa(index)
	attributes["compressed"] = strconv.FormatBool(snapshot.compressed)
	if stream.encoding != "" {
		attributes["encoding"] = stream.encoding
	}
	if stream.descriptor != nil {
		attributes["messageType"] = string(stream.descriptor.FullName())
	}
	if snapshot.decodeErr != "" {
		attributes["decodeError"] = snapshot.decodeErr
	}
	arrow := "→"
	request, response := snapshot.payload, observation.Payload{Kind: "text", Summary: "forwarded"}
	if stream.direction == "upstream_to_client" {
		arrow = "←"
		request, response = observation.Payload{Kind: "text", Summary: "upstream message"}, snapshot.payload
	}
	stream.call.sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: stream.call.upstream.ID, Protocol: "grpc", Connection: stream.call.connection, Operation: fmt.Sprintf("%s %s · message %d", arrow, stream.call.operation, index), StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: "ok", Request: request, Response: response, Attributes: attributes})
}

func (stream *grpcMessageStream) addError(message string) {
	if len(message) > maxGRPCFrameErrorCharacters {
		message = message[:maxGRPCFrameErrorCharacters]
	}
	if len(stream.errors) < 10 {
		stream.errors = append(stream.errors, message)
	}
}

func (stream *grpcMessageStream) finish() {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.active {
		stream.addError(fmt.Sprintf("incomplete gRPC message: %d of %d bytes missing", stream.remaining, stream.length))
	} else if stream.headerBytes != 0 {
		stream.addError(fmt.Sprintf("incomplete gRPC message header: %d of 5 bytes", stream.headerBytes))
	}
}

type grpcStreamSummary struct {
	count     int
	emitted   int
	snapshots []grpcMessageSnapshot
	truncated bool
	errors    []string
}

func (stream *grpcMessageStream) summary() grpcStreamSummary {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return grpcStreamSummary{count: stream.count, emitted: stream.emitted, snapshots: append([]grpcMessageSnapshot(nil), stream.snapshots...), truncated: stream.truncated, errors: append([]string(nil), stream.errors...)}
}

func (call *grpcCallCapture) finish(metadata *exchange, request *http.Request, requestBody *captureBuffer, response *captureWriter) {
	call.request.finish()
	call.response.finish()
	requestSummary, responseSummary := call.request.summary(), call.response.summary()
	statusValue := grpcHeader(metadata.trailers, response.Header(), "Grpc-Status")
	statusCode, statusName := grpcStatus(statusValue, response.status)
	message := grpcMessage(grpcHeader(metadata.trailers, response.Header(), "Grpc-Message"))
	outcome, errorText := "ok", ""
	if metadata.err != nil {
		outcome, errorText = "error", metadata.err.Error()
	} else if statusCode != 0 {
		outcome, errorText = "error", statusName
		if message != "" {
			errorText += ": " + message
		}
	} else if statusValue == "" {
		statusCode, statusName = 2, "UNKNOWN"
		outcome, errorText = "error", "gRPC response omitted grpc-status"
	}
	if len(requestSummary.errors)+len(responseSummary.errors) > 0 && errorText == "" {
		outcome, errorText = "error", strings.Join(append(requestSummary.errors, responseSummary.errors...), "; ")
	}
	attributes := call.attributes()
	attributes["grpcStatus"] = strconv.Itoa(statusCode)
	attributes["grpcCode"] = statusName
	attributes["httpStatus"] = strconv.Itoa(response.status)
	attributes["requestMessages"] = strconv.Itoa(requestSummary.count)
	attributes["responseMessages"] = strconv.Itoa(responseSummary.count)
	attributes["downstreamProtocol"] = "HTTP/2"
	if message != "" {
		attributes["grpcMessage"] = message
	}
	if timeout := request.Header.Get("Grpc-Timeout"); timeout != "" {
		attributes["timeout"] = timeout
	}
	if metadata.upstreamProtocol != "" {
		attributes["upstreamProtocol"] = metadata.upstreamProtocol
	}
	if metadata.upstreamTLS != "" {
		attributes["upstreamTLS"] = metadata.upstreamTLS
	}
	if details := grpcStatusDetails(grpcHeader(metadata.trailers, response.Header(), "Grpc-Status-Details-Bin")); details != "" {
		attributes["statusDetails"] = details
	}
	if requestSummary.emitted < requestSummary.count || responseSummary.emitted < responseSummary.count {
		attributes["messageObservationsCapped"] = "true"
	}
	sensitive := sensitiveHeaders(call.upstream)
	requestPayload := grpcAggregatePayload(payload(request.Header, requestBody, request.Header.Get("Content-Type"), "", sensitive), requestSummary, "request")
	responsePayload := grpcAggregatePayload(payload(response.Header(), response.body, response.Header().Get("Content-Type"), "", sensitive), responseSummary, "response")
	responsePayload.Summary = statusName + " · " + responsePayload.Summary
	call.sink.Record(observation.Interaction{ID: call.id, UpstreamID: call.upstream.ID, Protocol: "grpc", Connection: call.connection, Operation: call.operation, StartedAt: call.started, DurationUS: time.Since(call.started).Microseconds(), Outcome: outcome, Error: errorText, Request: requestPayload, Response: responsePayload, Attributes: attributes})
}

func grpcAggregatePayload(base observation.Payload, summary grpcStreamSummary, direction string) observation.Payload {
	base.Kind = "grpc"
	base.Text, base.JSON = "", nil
	base.Summary = fmt.Sprintf("%d %s message%s", summary.count, direction, plural(summary.count))
	base.Truncated = base.Truncated || summary.truncated || len(summary.snapshots) < summary.count
	if summary.count == 0 || len(summary.snapshots) != summary.count {
		return base
	}
	values := make([]json.RawMessage, 0, len(summary.snapshots))
	for _, snapshot := range summary.snapshots {
		if len(snapshot.payload.JSON) == 0 {
			return base
		}
		values = append(values, snapshot.payload.JSON)
	}
	var encoded []byte
	var err error
	if len(values) == 1 {
		encoded = append([]byte(nil), values[0]...)
	} else {
		encoded, err = json.Marshal(values)
	}
	if err == nil && len(encoded) <= captureLimit {
		base.Kind, base.JSON = "json", encoded
	}
	return base
}

func plural(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

func (call *grpcCallCapture) attributes() map[string]string {
	attributes := map[string]string{"service": call.service, "method": call.method, "callType": call.callType()}
	if call.profile != nil && call.profile.descriptor != "" {
		attributes["descriptorSet"] = "configured"
	}
	return attributes
}

func (call *grpcCallCapture) callType() string {
	if call.descriptor == nil {
		return "unknown"
	}
	switch {
	case call.descriptor.IsStreamingClient() && call.descriptor.IsStreamingServer():
		return "bidirectional streaming"
	case call.descriptor.IsStreamingClient():
		return "client streaming"
	case call.descriptor.IsStreamingServer():
		return "server streaming"
	default:
		return "unary"
	}
}

func grpcHeader(trailers, headers http.Header, name string) string {
	if value := headerValueCaseInsensitive(trailers, name); value != "" {
		return value
	}
	if value := headerValueCaseInsensitive(headers, http.TrailerPrefix+name); value != "" {
		return value
	}
	return headerValueCaseInsensitive(headers, name)
}

func headerValueCaseInsensitive(headers http.Header, name string) string {
	if headers == nil {
		return ""
	}
	return headers.Get(name)
}

func grpcMessage(value string) string {
	decoded, err := url.PathUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

func grpcStatus(value string, httpStatus int) (int, string) {
	if value != "" {
		code, err := strconv.Atoi(value)
		if err == nil && code >= 0 && code <= 16 {
			return code, grpcCodeName(code)
		}
		return 13, "INTERNAL"
	}
	if httpStatus != http.StatusOK {
		return grpcCodeFromHTTP(httpStatus)
	}
	return 0, "OK"
}

func grpcCodeFromHTTP(status int) (int, string) {
	switch status {
	case http.StatusBadRequest:
		return 13, "INTERNAL"
	case http.StatusUnauthorized:
		return 16, "UNAUTHENTICATED"
	case http.StatusForbidden:
		return 7, "PERMISSION_DENIED"
	case http.StatusNotFound:
		return 12, "UNIMPLEMENTED"
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return 14, "UNAVAILABLE"
	default:
		return 2, "UNKNOWN"
	}
}

func grpcCodeName(code int) string {
	names := [...]string{"OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED"}
	if code >= 0 && code < len(names) {
		return names[code]
	}
	return "UNKNOWN"
}

func grpcStatusDetails(encoded string) string {
	if encoded == "" {
		return ""
	}
	data, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(encoded, "="))
	if err != nil {
		return "<invalid grpc-status-details-bin>"
	}
	var status statuspb.Status
	if err := proto.Unmarshal(data, &status); err != nil {
		return "<undecodable grpc-status-details-bin>"
	}
	details := make([]string, 0, len(status.Details))
	for _, detail := range status.Details {
		if detail == nil {
			continue
		}
		details = append(details, anyTypeName(detail))
	}
	if len(details) == 0 {
		return status.Message
	}
	return status.Message + " [" + strings.Join(details, ", ") + "]"
}

func anyTypeName(value *anypb.Any) string {
	name := value.TypeUrl
	if position := strings.LastIndexByte(name, '/'); position >= 0 {
		name = name[position+1:]
	}
	return name
}
