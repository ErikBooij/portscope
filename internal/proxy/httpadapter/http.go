package httpadapter

import (
	"bytes"
	"context"
	"crypto/tls"
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

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy/tlsutil"
)

const captureLimit = 256 * 1024

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Run(ctx context.Context, upstream config.Upstream, sink observation.Sink, ready func(string)) error {
	listener, err := net.Listen("tcp", upstream.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", upstream.ListenAddr, err)
	}
	defer listener.Close()
	server := &http.Server{
		Handler:           a.handler(upstream, sink),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
		Protocols:         new(http.Protocols),
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetHTTP2(true)
	serveListener := listener
	if upstream.ListenerTLS != nil && upstream.ListenerTLS.Enabled {
		tlsConfig, tlsErr := tlsutil.Server(*upstream.ListenerTLS)
		if tlsErr != nil {
			return tlsErr
		}
		server.TLSConfig = tlsConfig
		serveListener = tls.NewListener(listener, tlsConfig)
	} else {
		server.Protocols.SetUnencryptedHTTP2(true)
	}
	ready(listener.Addr().String())
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.Serve(serveListener)
	if err == http.ErrServerClosed {
		return ctx.Err()
	}
	return err
}

func (a *Adapter) handler(upstream config.Upstream, sink observation.Sink) http.Handler {
	if upstream.Target == "internal://echo" {
		return observed(upstream, sink, http.HandlerFunc(echo))
	}
	target, _ := url.Parse(upstream.Target)
	transport, transportErr := transportFor(upstream, target)
	if transportErr != nil {
		return observed(upstream, sink, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			request.Context().Value(exchangeKey{}).(*exchange).err = transportErr
			http.Error(writer, "upstream TLS configuration invalid", http.StatusBadGateway)
		}))
	}
	websocketTransport, websocketTransportErr := websocketTransportFor(upstream, target)
	return observed(upstream, sink, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if isWebSocketUpgrade(request) {
			if websocketTransportErr != nil {
				request.Context().Value(exchangeKey{}).(*exchange).err = websocketTransportErr
				http.Error(writer, "upstream TLS configuration invalid", http.StatusBadGateway)
				return
			}
			proxyWebSocket(upstream, target, websocketTransport, sink, writer, request)
			return
		}
		if request.Method == http.MethodConnect {
			http.Error(writer, "CONNECT tunnels are not supported", http.StatusNotImplemented)
			return
		}
		_ = http.NewResponseController(writer).EnableFullDuplex()
		outgoing := request.Clone(request.Context())
		outgoing.RequestURI = ""
		outgoing.URL.Scheme = target.Scheme
		if outgoing.URL.Scheme == "h2c" {
			outgoing.URL.Scheme = "http"
		}
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
		removeHopHeaders(outgoing.Header)
		if upstream.HTTP != nil {
			applyHeaderRules(outgoing.Header, upstream.HTTP.RequestHeaders)
			// Record the actual post-policy headers sent on the wire.
			request.Header = outgoing.Header.Clone()
		}
		response, err := transport.RoundTrip(outgoing)
		if err != nil {
			request.Context().Value(exchangeKey{}).(*exchange).err = err
			http.Error(writer, "upstream unavailable", http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		metadata := request.Context().Value(exchangeKey{}).(*exchange)
		metadata.upstreamProtocol = response.Proto
		if response.TLS != nil {
			metadata.upstreamTLS = tlsVersion(response.TLS.Version)
		}
		removeHopHeaders(response.Header)
		if upstream.HTTP != nil {
			applyHeaderRules(response.Header, upstream.HTTP.ResponseHeaders)
		}
		copyHeaders(writer.Header(), response.Header)
		for name := range response.Trailer {
			writer.Header().Add("Trailer", name)
		}
		writer.WriteHeader(response.StatusCode)
		if err := copyResponse(writer, response.Body); err != nil {
			metadata.err = err
		}
		for name, values := range response.Trailer {
			writer.Header()[http.TrailerPrefix+name] = values
		}
	}))
}

func websocketTransportFor(upstream config.Upstream, target *url.URL) (*http.Transport, error) {
	transport, err := transportFor(upstream, target)
	if err != nil {
		return nil, err
	}
	transport.ForceAttemptHTTP2 = false
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	return transport, nil
}

func transportFor(upstream config.Upstream, target *url.URL) (*http.Transport, error) {
	options := config.ClientTLSOptions{}
	if upstream.HTTP != nil {
		options = upstream.HTTP.UpstreamTLS
	}
	serverName := target.Hostname()
	tlsConfig, err := tlsutil.Client(options, serverName)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       tlsConfig,
		Protocols:             new(http.Protocols),
	}
	if target.Scheme == "h2c" {
		transport.Protocols.SetUnencryptedHTTP2(true)
	} else {
		transport.Protocols.SetHTTP1(true)
		transport.Protocols.SetHTTP2(true)
	}
	return transport, nil
}

type exchangeKey struct{}
type exchange struct {
	err              error
	upstreamProtocol string
	upstreamTLS      string
	skipObservation  bool
}

func observed(upstream config.Upstream, sink observation.Sink, next http.Handler) http.Handler {
	sensitive := sensitiveHeaders(upstream)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		requestCapture := &captureBuffer{limit: captureLimit}
		if request.Body != nil {
			request.Body = &teeReadCloser{Reader: io.TeeReader(request.Body, requestCapture), Closer: request.Body}
		}
		responseCapture := &captureWriter{ResponseWriter: writer, body: &captureBuffer{limit: captureLimit}, status: 200}
		metadata := &exchange{}
		request = request.WithContext(context.WithValue(request.Context(), exchangeKey{}, metadata))
		next.ServeHTTP(responseCapture, request)
		if metadata.skipObservation {
			return
		}
		outcome := "ok"
		errorText := ""
		if metadata.err != nil {
			outcome = "error"
			errorText = metadata.err.Error()
		} else if responseCapture.status >= 400 {
			outcome = "error"
		}
		attributes := map[string]string{
			"method": request.Method, "path": request.URL.Path, "status": strconv.Itoa(responseCapture.status), "host": request.Host,
			"downstreamProtocol": request.Proto,
		}
		if metadata.upstreamProtocol != "" {
			attributes["upstreamProtocol"] = metadata.upstreamProtocol
		}
		if metadata.upstreamTLS != "" {
			attributes["upstreamTLS"] = metadata.upstreamTLS
		}
		sink.Record(observation.Interaction{ID: observation.NewID(), UpstreamID: upstream.ID, Protocol: "http", Connection: request.RemoteAddr, Operation: request.Method + " " + request.URL.RequestURI(), StartedAt: started, DurationUS: time.Since(started).Microseconds(), Outcome: outcome, Error: errorText,
			Request:    payload(request.Header, requestCapture, request.Header.Get("Content-Type"), request.Method+" "+request.URL.RequestURI(), sensitive),
			Response:   payload(responseCapture.Header(), responseCapture.body, responseCapture.Header().Get("Content-Type"), strconv.Itoa(responseCapture.status)+" "+http.StatusText(responseCapture.status), sensitive),
			Attributes: attributes})
	})
}

type captureBuffer struct {
	bytes.Buffer
	limit     int
	total     int64
	truncated bool
}

func (b *captureBuffer) Write(data []byte) (int, error) {
	b.total += int64(len(data))
	remaining := b.limit - b.Len()
	if remaining > 0 {
		_, _ = b.Buffer.Write(data[:min(remaining, len(data))])
	}
	if len(data) > remaining {
		b.truncated = true
	}
	return len(data), nil
}

type teeReadCloser struct {
	io.Reader
	io.Closer
}
type captureWriter struct {
	http.ResponseWriter
	body        *captureBuffer
	status      int
	wroteHeader bool
}

func (w *captureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *captureWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *captureWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.body.Write(data)
	return w.ResponseWriter.Write(data)
}

func payload(headers http.Header, body *captureBuffer, contentType, summary string, sensitive map[string]struct{}) observation.Payload {
	result := observation.Payload{Kind: "text", Summary: summary, Text: string(body.Bytes()), Size: body.total, Truncated: body.truncated, Headers: redactHeaders(headers, sensitive)}
	if strings.Contains(contentType, "json") && json.Valid(body.Bytes()) {
		result.Kind = "json"
		result.JSON = append([]byte(nil), body.Bytes()...)
		result.Text = ""
	} else if !looksText(body.Bytes()) {
		result.Kind = "bytes"
		result.Text = fmt.Sprintf("<%d bytes>", body.total)
	}
	return result
}
func redactHeaders(headers http.Header, sensitive map[string]struct{}) []observation.Pair {
	result := make([]observation.Pair, 0, len(headers))
	for name, values := range headers {
		value := strings.Join(values, ", ")
		if _, found := sensitive[strings.ToLower(name)]; found {
			value = "[redacted]"
		}
		result = append(result, observation.Pair{Name: name, Value: value})
	}
	return result
}
func sensitiveHeaders(upstream config.Upstream) map[string]struct{} {
	result := map[string]struct{}{"authorization": {}, "proxy-authorization": {}, "cookie": {}, "set-cookie": {}, "x-api-key": {}}
	if upstream.HTTP != nil {
		for _, rule := range append(append([]config.HeaderRule{}, upstream.HTTP.RequestHeaders...), upstream.HTTP.ResponseHeaders...) {
			if rule.Sensitive {
				result[strings.ToLower(rule.Name)] = struct{}{}
			}
		}
	}
	return result
}
func applyHeaderRules(headers http.Header, rules []config.HeaderRule) {
	for _, rule := range rules {
		switch rule.Action {
		case "set":
			headers.Set(rule.Name, rule.Value)
		case "add":
			headers.Add(rule.Name, rule.Value)
		case "remove":
			headers.Del(rule.Name)
		}
	}
}
func looksText(data []byte) bool {
	for _, value := range data {
		if value == 0 {
			return false
		}
	}
	return true
}
func copyHeaders(target, source http.Header) {
	for name, values := range source {
		for _, value := range values {
			target.Add(name, value)
		}
	}
}
func removeHopHeaders(headers http.Header) {
	connectionTokens := strings.Split(headers.Get("Connection"), ",")
	for _, name := range connectionTokens {
		headers.Del(strings.TrimSpace(name))
	}
	teTrailers := false
	for _, value := range headers.Values("Te") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "trailers") {
				teTrailers = true
			}
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		headers.Del(name)
	}
	if teTrailers {
		headers.Set("Te", "trailers")
	}
}

func copyResponse(writer http.ResponseWriter, reader io.Reader) error {
	buffer := make([]byte, 32*1024)
	controller := http.NewResponseController(writer)
	for {
		count, readErr := reader.Read(buffer)
		if count > 0 {
			if _, writeErr := writer.Write(buffer[:count]); writeErr != nil {
				return writeErr
			}
			_ = controller.Flush()
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}
func joinPath(base, path string) string {
	if base == "" {
		return path
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
}
func tlsVersion(version uint16) string {
	switch version {
	case tls.VersionTLS13:
		return "TLS 1.3"
	case tls.VersionTLS12:
		return "TLS 1.2"
	default:
		return fmt.Sprintf("TLS 0x%x", version)
	}
}
func echo(writer http.ResponseWriter, request *http.Request) {
	if request.Body != nil {
		_, _ = io.Copy(io.Discard, request.Body)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("X-Portscope-Demo", "true")
	if strings.HasSuffix(request.URL.Path, "/404") {
		writer.WriteHeader(http.StatusNotFound)
	}
	_ = json.NewEncoder(writer).Encode(map[string]any{"method": request.Method, "path": request.URL.Path, "query": request.URL.Query(), "message": "This response passed through a real Portscope HTTP proxy."})
}
