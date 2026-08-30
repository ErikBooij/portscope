package httpadapter

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestGRPCProxyDecodesCompressedUnaryAndLiveStreamingMessages(t *testing.T) {
	injected := make(chan string, 2)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if values := metadata.ValueFromIncomingContext(ctx, "x-inspection"); len(values) > 0 {
			injected <- values[0]
		}
		return handler(ctx, request)
	}))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("stream", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	go server.Serve(listener)
	defer server.Stop()

	descriptorPath := healthDescriptorSet(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 50)
	upstream := config.Upstream{
		ID: "grpc", Name: "Health", Protocol: "grpc", ListenAddr: "127.0.0.1:0", Target: "h2c://" + listener.Addr().String(), Enabled: true,
		HTTP: &config.HTTPOptions{RequestHeaders: []config.HeaderRule{{Action: "set", Name: "X-Inspection", Value: "portscope"}}},
		GRPC: &config.GRPCOptions{DescriptorSetFile: descriptorPath},
	}
	go func() { _ = New().Run(ctx, upstream, testSink{events}, func(address string) { ready <- address }) }()
	connection, err := grpc.NewClient(<-ready, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.UseCompressor(grpcgzip.Name)))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := healthpb.NewHealthClient(connection)
	response, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health check = %#v, %v", response, err)
	}
	select {
	case value := <-injected:
		if value != "portscope" {
			t.Fatalf("injected metadata = %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("gRPC metadata header policy did not reach upstream")
	}

	unary := waitForGRPCOperation(t, events, "grpc.health.v1.Health/Check")
	if unary.Protocol != "grpc" || unary.Outcome != "ok" || unary.Attributes["grpcCode"] != "OK" || unary.Attributes["callType"] != "unary" || unary.Attributes["requestMessages"] != "1" || unary.Attributes["responseMessages"] != "1" {
		t.Fatalf("unary observation = %#v", unary)
	}
	if string(unary.Request.JSON) != `{}` || !strings.Contains(string(unary.Response.JSON), `"status":"SERVING"`) {
		t.Fatalf("decoded unary payloads = %s / %s", unary.Request.JSON, unary.Response.JSON)
	}
	if pairValue(unary.Request.Headers, "X-Inspection") != "portscope" {
		t.Fatalf("post-policy request headers missing: %#v", unary.Request.Headers)
	}
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing health service error = %v", err)
	}
	failed := waitForGRPCOperation(t, events, "grpc.health.v1.Health/Check")
	if failed.Outcome != "error" || failed.Attributes["grpcCode"] != "NOT_FOUND" || failed.Attributes["grpcStatus"] != "5" || !strings.Contains(failed.Error, "unknown service") {
		t.Fatalf("failed unary observation = %#v", failed)
	}

	streamContext, stopStream := context.WithCancel(ctx)
	stream, err := client.Watch(streamContext, &healthpb.HealthCheckRequest{Service: "stream"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil || first.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("first watch response = %#v, %v", first, err)
	}
	healthServer.SetServingStatus("stream", healthpb.HealthCheckResponse_NOT_SERVING)
	second, err := stream.Recv()
	if err != nil || second.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("second watch response = %#v, %v", second, err)
	}
	live := waitForGRPCOperation(t, events, "← grpc.health.v1.Health/Watch · message 2")
	if live.Attributes["callType"] != "server streaming" || !strings.Contains(string(live.Response.JSON), `"status":"NOT_SERVING"`) {
		t.Fatalf("live streaming observation = %#v", live)
	}
	stopStream()
}

func TestGRPCFrameParserHandlesSplitGzipAndBoundsHugeMessages(t *testing.T) {
	call := &grpcCallCapture{upstream: config.Upstream{ID: "grpc"}, sink: testSink{make(chan observation.Interaction, 10)}, profile: &grpcProfile{}, id: "call", connection: "client", operation: "test.Service/Method", service: "test.Service", method: "Method", started: time.Now()}
	stream := newGRPCMessageStream(call, "client_to_upstream", "gzip", "json", nil)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write([]byte(`{"value":42}`))
	_ = writer.Close()
	frame := grpcTestFrame(true, compressed.Bytes())
	for _, value := range frame {
		stream.feed([]byte{value})
	}
	stream.finish()
	summary := stream.summary()
	if summary.count != 1 || len(summary.snapshots) != 1 || string(summary.snapshots[0].payload.JSON) != `{"value":42}` || len(summary.errors) != 0 {
		t.Fatalf("split compressed frame = %#v", summary)
	}

	huge := newGRPCMessageStream(call, "client_to_upstream", "", "proto", nil)
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[1:], ^uint32(0))
	huge.feed(append(header, []byte("bounded")...))
	huge.finish()
	hugeSummary := huge.summary()
	if len(huge.capture) > captureLimit || len(hugeSummary.errors) == 0 || !strings.Contains(hugeSummary.errors[0], "incomplete") {
		t.Fatalf("huge frame was not bounded: %#v capture=%d", hugeSummary, len(huge.capture))
	}
}

func TestGRPCUsesVerifiedHTTP2TLSOnBothLegs(t *testing.T) {
	certificatePath, keyPath, roots := testServerCertificate(t)
	pair, err := tls.LoadX509KeyPair(certificatePath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}})))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	go server.Serve(listener)
	defer server.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 10)
	upstream := config.Upstream{ID: "secure-grpc", Name: "Secure health", Protocol: "grpc", ListenAddr: "127.0.0.1:0", Target: "https://" + listener.Addr().String(), Enabled: true, ListenerTLS: &config.ListenerTLSOptions{Enabled: true, CertFile: certificatePath, KeyFile: keyPath}, HTTP: &config.HTTPOptions{UpstreamTLS: config.ClientTLSOptions{Enabled: true, CAFile: certificatePath, ServerName: "127.0.0.1"}}, GRPC: &config.GRPCOptions{}}
	go func() { _ = New().Run(ctx, upstream, testSink{events}, func(address string) { ready <- address }) }()
	connection, err := grpc.NewClient(<-ready, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"})))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	call := waitForGRPCOperation(t, events, "grpc.health.v1.Health/Check")
	if call.Attributes["upstreamProtocol"] != "HTTP/2.0" || call.Attributes["upstreamTLS"] == "" || call.Attributes["downstreamProtocol"] != "HTTP/2" {
		t.Fatalf("secure gRPC transport attributes = %#v", call.Attributes)
	}
}

func TestGRPCProfileRejectsOversizedDescriptorSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "huge.protoset")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(maxGRPCDescriptorBytes, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := loadGRPCProfile(&config.GRPCOptions{DescriptorSetFile: path}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized descriptor set error = %v", err)
	}
}

func grpcTestFrame(compressed bool, payload []byte) []byte {
	frame := make([]byte, len(payload)+5)
	if compressed {
		frame[0] = 1
	}
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	copy(frame[5:], payload)
	return frame
}

func healthDescriptorSet(t *testing.T) string {
	t.Helper()
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{protodesc.ToFileDescriptorProto(healthpb.File_grpc_health_v1_health_proto)}}
	data, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "health.protoset")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitForGRPCOperation(t *testing.T, events <-chan observation.Interaction, operation string) observation.Interaction {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case item := <-events:
			if item.Operation == operation {
				return item
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", operation)
		}
	}
}

func pairValue(values []observation.Pair, name string) string {
	for _, value := range values {
		if strings.EqualFold(value.Name, name) {
			return value.Value
		}
	}
	return ""
}
