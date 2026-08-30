# RabbitMQ compatibility contract

Portscope implements the RabbitMQ AMQP 0-9-1 wire protocol. The real-server matrix covers RabbitMQ 3.13.7, 4.0.9, 4.1.8, 4.2.9, and 4.3.5 with the official RabbitMQ Go client. It declares exchanges and queues, binds routing keys, publishes JSON with headers under publisher-confirm mode, gets and acknowledges the message, and executes transaction selection and rollback on a separate channel.

## Connection ownership

Portscope terminates `connection.start`, PLAIN `connection.start-ok`, tune negotiation, and `connection.open`. The application-facing username, password, and virtual host are validated locally. Only after that succeeds does Portscope send the independently configured broker username/password and open the configured broker virtual host. Application credentials therefore never become broker credentials, and a failed listener login cannot trigger upstream authentication.

After `connection.open-ok`, Portscope forwards complete AMQP frames byte-for-byte. Connection and channel state, publisher confirms, consumer tags, delivery tags, transactions, acknowledgements, flow control, returned messages, and broker extensions remain owned by RabbitMQ and the client. Portscope blocks a post-open `connection.start-ok` or `connection.update-secret` attempt because either would try to replace the identity Portscope owns.

## Inspection

The inspector understands standard connection, channel, exchange, queue, Basic, confirm, and transaction methods. It tracks pending synchronous methods per channel and correlates their `*-ok` response without serializing unrelated channels. Basic publish, return, deliver, and get-ok content is assembled from its method, content header, and one or more body frames. Common Basic properties and application headers are captured; JSON content is rendered structurally. Captures stop at 256 KiB while forwarding continues.

Frames are bounded by the negotiated frame maximum and a 128 MiB absolute ceiling. Frame terminators, heartbeat channels, content lengths, table lengths, entry counts, and nested field values are validated. Incomplete operations become error observations when the connection closes.

## TLS and authentication

Listener and broker TLS are independent direct-TLS connections. Both enforce TLS 1.2 or newer and support custom roots, SNI, client certificates, and listener-side mutual TLS through the shared TLS module. The test suite verifies certificate validation on both legs simultaneously.

Portscope currently terminates the PLAIN SASL mechanism. AMQPLAIN, EXTERNAL, SCRAM, OAuth, and challenge-based mechanisms are outside this contract. The listener intentionally advertises only PLAIN; the broker must offer PLAIN for the configured upstream identity.

## Scope

This adapter is for AMQP 0-9-1. AMQP 1.0, RabbitMQ Streams, MQTT, STOMP, and Web STOMP are distinct protocols and are not accepted on this listener. Heartbeats are forwarded and validated but not persisted as interactions. Unknown well-formed AMQP methods and field values continue forwarding; they remain generic observations rather than being guessed.
