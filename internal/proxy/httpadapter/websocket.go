package httpadapter

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type webSocketFrame struct {
	started time.Time
	fin     bool
	rsv     byte
	opcode  byte
	masked  bool
	length  int64
	body    *captureBuffer
}

type webSocketCopyResult struct {
	direction string
	err       error
}

func isWebSocketUpgrade(request *http.Request) bool {
	return headerContainsToken(request.Header, "Connection", "upgrade") && strings.EqualFold(strings.TrimSpace(request.Header.Get("Upgrade")), "websocket")
}

func proxyWebSocket(upstream config.Upstream, target *url.URL, transport *http.Transport, sink observation.Sink, writer http.ResponseWriter, request *http.Request) {
	metadata := request.Context().Value(exchangeKey{}).(*exchange)
	if target.Scheme == "h2c" {
		http.Error(writer, "WebSocket upgrades require an http:// or https:// upstream target", http.StatusNotImplemented)
		return
	}
	started := time.Now()
	outgoing := request.Clone(request.Context())
	outgoing.RequestURI = ""
	outgoing.URL.Scheme = target.Scheme
	outgoing.URL.Host = target.Host
	outgoing.URL.Path = joinPath(target.Path, request.URL.Path)
	outgoing.URL.RawPath = ""
	if target.RawQuery != "" {
		if outgoing.URL.RawQuery != "" {
			outgoing.URL.RawQuery = target.RawQuery + "&" + outgoing.URL.RawQuery
		} else {
			outgoing.URL.RawQuery = target.RawQuery
		}
	}
	if upstream.HTTP == nil || !upstream.HTTP.PreserveHost {
		outgoing.Host = target.Host
	}
	removeWebSocketHopHeaders(outgoing.Header)
	if upstream.HTTP != nil {
		applyHeaderRules(outgoing.Header, upstream.HTTP.RequestHeaders)
	}
	request.Header = outgoing.Header.Clone()

	response, err := transport.RoundTrip(outgoing)
	if err != nil {
		metadata.err = err
		http.Error(writer, "upstream unavailable", http.StatusBadGateway)
		return
	}
	metadata.upstreamProtocol = response.Proto
	if response.TLS != nil {
		metadata.upstreamTLS = tlsVersion(response.TLS.Version)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		defer response.Body.Close()
		removeHopHeaders(response.Header)
		if upstream.HTTP != nil {
			applyHeaderRules(response.Header, upstream.HTTP.ResponseHeaders)
		}
		copyHeaders(writer.Header(), response.Header)
		writer.WriteHeader(response.StatusCode)
		if err := copyResponse(writer, response.Body); err != nil {
			metadata.err = err
		}
		return
	}
	if !strings.EqualFold(strings.TrimSpace(response.Header.Get("Upgrade")), "websocket") || !headerContainsToken(response.Header, "Connection", "upgrade") {
		_ = response.Body.Close()
		metadata.err = errors.New("upstream returned an invalid WebSocket upgrade response")
		http.Error(writer, "invalid upstream WebSocket response", http.StatusBadGateway)
		return
	}
	removeWebSocketHopHeaders(response.Header)
	backend, ok := response.Body.(io.ReadWriteCloser)
	if !ok {
		_ = response.Body.Close()
		metadata.err = errors.New("WebSocket upgrade response body is not writable")
		http.Error(writer, "invalid upstream WebSocket connection", http.StatusBadGateway)
		return
	}

	client, buffered, err := http.NewResponseController(writer).Hijack()
	if err != nil {
		_ = backend.Close()
		metadata.err = fmt.Errorf("hijack WebSocket client: %w", err)
		http.Error(writer, "WebSocket upgrade unavailable", http.StatusInternalServerError)
		return
	}
	metadata.skipObservation = true
	defer client.Close()
	defer backend.Close()

	copyHeaders(writer.Header(), response.Header)
	if upstream.HTTP != nil {
		applyHeaderRules(writer.Header(), upstream.HTTP.ResponseHeaders)
	}
	response.Header = writer.Header()
	response.Body = nil
	if err := response.Write(buffered); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}

	connectionID := observation.NewID()
	recordWebSocketHandshake(upstream, sink, request, response, connectionID, started, metadata)
	closeOnCancel := make(chan struct{})
	go func() {
		select {
		case <-request.Context().Done():
			_ = backend.Close()
			_ = client.Close()
		case <-closeOnCancel:
		}
	}()
	defer close(closeOnCancel)

	results := make(chan webSocketCopyResult, 2)
	go copyWebSocketFrames(backend, buffered, "client_to_upstream", upstream, sink, connectionID, request.URL.RequestURI(), results)
	go copyWebSocketFrames(client, backend, "upstream_to_client", upstream, sink, connectionID, request.URL.RequestURI(), results)
	first := <-results
	_ = backend.Close()
	_ = client.Close()
	second := <-results
	for _, result := range []webSocketCopyResult{first, second} {
		if result.err != nil && !errors.Is(result.err, io.EOF) && !errors.Is(result.err, net.ErrClosed) && !strings.Contains(strings.ToLower(result.err.Error()), "closed network connection") {
			recordWebSocketError(upstream, sink, connectionID, request.URL.RequestURI(), result.direction, result.err)
		}
	}
}

func copyWebSocketFrames(destination io.Writer, source io.Reader, direction string, upstream config.Upstream, sink observation.Sink, connectionID, path string, results chan<- webSocketCopyResult) {
	for {
		frame, err := forwardWebSocketFrame(destination, source)
		if err != nil {
			results <- webSocketCopyResult{direction: direction, err: err}
			return
		}
		recordWebSocketFrame(upstream, sink, connectionID, path, direction, frame)
	}
}

func forwardWebSocketFrame(destination io.Writer, source io.Reader) (webSocketFrame, error) {
	var first [2]byte
	if _, err := io.ReadFull(source, first[:]); err != nil {
		return webSocketFrame{}, err
	}
	frame := webSocketFrame{started: time.Now(), fin: first[0]&0x80 != 0, rsv: (first[0] >> 4) & 0x7, opcode: first[0] & 0xf, masked: first[1]&0x80 != 0, body: &captureBuffer{limit: captureLimit}}
	header := append([]byte(nil), first[:]...)
	lengthCode := first[1] & 0x7f
	switch lengthCode {
	case 126:
		var extended [2]byte
		if _, err := io.ReadFull(source, extended[:]); err != nil {
			return frame, err
		}
		header = append(header, extended[:]...)
		frame.length = int64(binary.BigEndian.Uint16(extended[:]))
		if frame.length < 126 {
			return frame, errors.New("non-minimal WebSocket frame length")
		}
	case 127:
		var extended [8]byte
		if _, err := io.ReadFull(source, extended[:]); err != nil {
			return frame, err
		}
		header = append(header, extended[:]...)
		unsigned := binary.BigEndian.Uint64(extended[:])
		if unsigned > uint64(^uint64(0)>>1) || unsigned < 65536 {
			return frame, errors.New("invalid WebSocket frame length")
		}
		frame.length = int64(unsigned)
	default:
		frame.length = int64(lengthCode)
	}
	var mask [4]byte
	if frame.masked {
		if _, err := io.ReadFull(source, mask[:]); err != nil {
			return frame, err
		}
		header = append(header, mask[:]...)
	}
	if err := writeWebSocketBytes(destination, header); err != nil {
		return frame, err
	}
	remaining := frame.length
	offset := int64(0)
	buffer := make([]byte, 32*1024)
	for remaining > 0 {
		chunk := int64(len(buffer))
		if remaining < chunk {
			chunk = remaining
		}
		data := buffer[:chunk]
		if _, err := io.ReadFull(source, data); err != nil {
			return frame, err
		}
		if frame.masked {
			decoded := make([]byte, len(data))
			for index, value := range data {
				decoded[index] = value ^ mask[(offset+int64(index))%4]
			}
			_, _ = frame.body.Write(decoded)
		} else {
			_, _ = frame.body.Write(data)
		}
		if err := writeWebSocketBytes(destination, data); err != nil {
			return frame, err
		}
		offset += chunk
		remaining -= chunk
	}
	return frame, nil
}

func recordWebSocketHandshake(upstream config.Upstream, sink observation.Sink, request *http.Request, response *http.Response, connectionID string, started time.Time, metadata *exchange) {
	sensitive := sensitiveHeaders(upstream)
	emptyRequest := &captureBuffer{limit: captureLimit}
	emptyResponse := &captureBuffer{limit: captureLimit}
	attributes := map[string]string{
		"direction": "handshake", "path": request.URL.Path, "status": "101", "downstreamProtocol": request.Proto, "upstreamProtocol": response.Proto,
	}
	if protocol := response.Header.Get("Sec-WebSocket-Protocol"); protocol != "" {
		attributes["subprotocol"] = protocol
	}
	if extensions := response.Header.Get("Sec-WebSocket-Extensions"); extensions != "" {
		attributes["extensions"] = extensions
	}
	if metadata.upstreamTLS != "" {
		attributes["upstreamTLS"] = metadata.upstreamTLS
	}
	sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "websocket", Connection: connectionID,
		Operation: "WS CONNECT " + request.URL.RequestURI(), StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: "ok",
		Request:  payload(request.Header, emptyRequest, "", request.Method+" "+request.URL.RequestURI(), sensitive),
		Response: payload(response.Header, emptyResponse, "", "101 Switching Protocols", sensitive), Attributes: attributes,
	})
}

func recordWebSocketFrame(upstream config.Upstream, sink observation.Sink, connectionID, path, direction string, frame webSocketFrame) {
	payload := webSocketPayload(frame)
	arrow := "→"
	requestPayload := payload
	responsePayload := observation.Payload{Kind: "websocket", Summary: "forwarded upstream"}
	if direction == "upstream_to_client" {
		arrow = "←"
		requestPayload = observation.Payload{Kind: "websocket", Summary: "received from upstream"}
		responsePayload = payload
	}
	attributes := map[string]string{
		"direction": direction, "opcode": webSocketOpcode(frame.opcode), "fin": strconv.FormatBool(frame.fin), "masked": strconv.FormatBool(frame.masked), "path": path,
	}
	if frame.rsv != 0 {
		attributes["reservedBits"] = fmt.Sprintf("0x%x", frame.rsv)
	}
	sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "websocket", Connection: connectionID,
		Operation: "WS " + arrow + " " + webSocketOpcode(frame.opcode) + " " + path, StartedAt: frame.started, DurationUS: time.Since(frame.started).Microseconds(), Outcome: "ok",
		Request: requestPayload, Response: responsePayload, Attributes: attributes,
	})
}

func recordWebSocketError(upstream config.Upstream, sink observation.Sink, connectionID, path, direction string, err error) {
	sink.Record(observation.Interaction{
		ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "websocket", Connection: connectionID,
		Operation: "WS PROTOCOL " + path, StartedAt: time.Now(), Outcome: "error", Error: err.Error(),
		Request: observation.Payload{Kind: "websocket", Summary: "frame forwarding failed"}, Response: observation.Payload{Kind: "websocket", Summary: "connection closed"},
		Attributes: map[string]string{"direction": direction, "path": path},
	})
}

func webSocketPayload(frame webSocketFrame) observation.Payload {
	data := frame.body.Bytes()
	result := observation.Payload{Kind: "bytes", Size: frame.length, Truncated: frame.body.truncated, Text: fmt.Sprintf("<%d bytes>", frame.length), Summary: webSocketOpcode(frame.opcode)}
	if frame.rsv == 0 && (frame.opcode == 1 || frame.opcode == 0 || frame.opcode >= 8) && utf8.Valid(data) {
		result.Kind = "text"
		result.Text = string(data)
		if frame.opcode == 1 && json.Valid(data) {
			result.Kind = "json"
			result.JSON = append([]byte(nil), data...)
			result.Text = ""
		}
	}
	if frame.opcode == 8 && len(data) >= 2 {
		code := binary.BigEndian.Uint16(data[:2])
		reason := ""
		if utf8.Valid(data[2:]) {
			reason = string(data[2:])
		}
		result.Kind = "text"
		result.Summary = fmt.Sprintf("CLOSE %d", code)
		result.Text = strings.TrimSpace(fmt.Sprintf("%d %s", code, reason))
	}
	return result
}

func webSocketOpcode(opcode byte) string {
	switch opcode {
	case 0:
		return "CONTINUATION"
	case 1:
		return "TEXT"
	case 2:
		return "BINARY"
	case 8:
		return "CLOSE"
	case 9:
		return "PING"
	case 10:
		return "PONG"
	default:
		return fmt.Sprintf("OPCODE_%X", opcode)
	}
}

func headerContainsToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for _, candidate := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), token) {
				return true
			}
		}
	}
	return false
}

func removeWebSocketHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			name := strings.TrimSpace(token)
			if name != "" && !strings.EqualFold(name, "upgrade") {
				header.Del(name)
			}
		}
	}
	for _, name := range []string{"Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding"} {
		header.Del(name)
	}
	header.Set("Connection", "Upgrade")
	header.Set("Upgrade", "websocket")
}

func writeWebSocketBytes(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		count, err := writer.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrUnexpectedEOF
		}
		data = data[count:]
	}
	return nil
}
