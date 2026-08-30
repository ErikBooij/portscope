//go:build rabbitmatrix

package rabbitadapter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRealRabbitMQCompatibility(t *testing.T) {
	address := os.Getenv("RABBIT_MATRIX_ADDR")
	if address == "" {
		t.Skip("RABBIT_MATRIX_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 200)
	upstream := config.Upstream{
		ID: "rabbit-real", Name: "RabbitMQ " + os.Getenv("RABBIT_MATRIX_VERSION"), Protocol: "rabbitmq", ListenAddr: "127.0.0.1:0", Target: address, Enabled: true,
		RabbitMQ: &config.RabbitMQOptions{ListenerUsername: "portscope_listener", ListenerPassword: "listener-secret", ListenerVHost: "listener", UpstreamUsername: "portscope_upstream", UpstreamPassword: "upstream-secret", UpstreamVHost: "/portscope"},
	}
	go func() { _ = New().Run(ctx, upstream, testSink{events}, func(address string) { ready <- address }) }()
	proxyAddress := <-ready
	connection, err := amqp.DialConfig(amqpURL(proxyAddress, "portscope_listener", "listener-secret", "listener"), amqp.Config{Heartbeat: time.Second, Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		select {
		case event := <-events:
			t.Fatalf("dial: %v; proxy observation: %#v", err, event)
		default:
			t.Fatal(err)
		}
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	const exchange = "portscope.matrix"
	if err := channel.ExchangeDeclare(exchange, "direct", false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}
	queue, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.QueueBind(queue.Name, "books", exchange, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := channel.Confirm(false); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"kind":"book","rank":1}`)
	confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, exchange, "books", false, false, amqp.Publishing{ContentType: "application/json", MessageId: "portscope-matrix", Headers: amqp.Table{"suite": "compatibility"}, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if confirmation == nil || !confirmation.Wait() {
		t.Fatal("RabbitMQ did not confirm publish")
	}
	delivery, ok, err := channel.Get(queue.Name, false)
	if err != nil || !ok || string(delivery.Body) != string(body) {
		t.Fatalf("get returned %q, %t, %v", delivery.Body, ok, err)
	}
	if err := delivery.Ack(false); err != nil {
		t.Fatal(err)
	}
	txChannel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer txChannel.Close()
	if err := txChannel.Tx(); err != nil {
		t.Fatal(err)
	}
	if err := txChannel.TxRollback(); err != nil {
		t.Fatal(err)
	}
	seen := waitForAMQPOperations(t, events, "CONNECT", "DECLARE EXCHANGE "+exchange, "PUBLISH "+exchange+" → books", "GET "+queue.Name, "TX ROLLBACK")
	if seen["CONNECT"].Attributes["serverVersion"] == "" {
		t.Fatalf("broker version was not captured: %#v", seen["CONNECT"])
	}
	publish := seen["PUBLISH "+exchange+" → books"]
	if publish.Request.Kind != "json" || headerValue(publish.Request.Headers, "header.suite") != "compatibility" {
		t.Fatalf("publish capture = %#v", publish)
	}
	get := seen["GET "+queue.Name]
	if get.Response.Kind != "json" || get.Response.Truncated {
		t.Fatalf("get capture = %#v", get)
	}
}
