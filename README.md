# REST SMPP Herald

A high-performance SMS gateway that bridges RESTful HTTP APIs with the SMPP (Short Message Peer-to-Peer) protocol, written in Rust.

## Why This Project Exists

Integrating with telecom infrastructure via SMPP is complex. The protocol is binary, stateful, and requires persistent TCP connections with features like windowing, sequence numbering, and heartbeats. Most application developers just want to send and receive SMS messages through a simple HTTP API.

REST SMPP Herald eliminates that complexity. It handles the low-level SMPP protocol details — connection management, reconnection, bind negotiation, PDU encoding/decoding, TLV fields — and exposes a clean, modern REST API. Applications can send SMS, receive delivery reports, and manage gateway configuration without dealing with SMPP directly.

This project is also designed for environments where you need to run your own SMS gateway infrastructure rather than relying on third-party cloud APIs, giving you full control over message routing, storage, and delivery.

## What It Does

REST SMPP Herald operates in two modes:

### Client Mode

Connects to one or more external SMSC (Short Message Service Center) servers and provides a REST API for applications to interact with them.

- **Send SMS** — single messages or bulk batches via simple HTTP POST
- **Receive SMS** — poll for inbound messages or get them pushed via webhooks
- **Track delivery** — query message status and delivery reports
- **Multiple SMSC connections** — connect to several SMSCs simultaneously with configurable load balancing (round-robin or failover) and weighted routing
- **Automatic reconnection** — recovers from network failures and SMSC restarts
- **Rate limiting** — configurable leaky-bucket rate limits to comply with SMSC throttling policies
- **API key authentication** — secure REST API access with API keys
- **Phone number management** — register and manage sender IDs and phone numbers
- **Country-based routing** — route messages based on destination country
- **Message ID generation** — pluggable ID generators (UUID, sequential, Redis-backed)
- **Message storage** — in-memory or Redis-backed storage for inbound and outbound messages
- **Scheduled delivery** — queue messages for future delivery
- **Prometheus metrics** — built-in `/metrics` endpoint for monitoring
- **Alarm notifications** — configurable alerts for connection failures and errors

### Server Mode

Acts as an SMSC server, accepting connections from SMPP clients. Useful for testing, message proxying, or building custom SMSC logic.

- **Multi-client support** — accept connections from multiple SMPP clients simultaneously
- **User authentication** — via local YAML file or external HTTP authentication service
- **IP whitelisting** — restrict connections by source IP address
- **Message forwarding** — forward received messages to HTTP handler endpoints with configurable load balancing

## Configuration

Configuration files can be JSON or YAML.

### Client Configuration

```yaml
rest_addresses:
  - "0.0.0.0:8080"

connections:
  - address: "127.0.0.1:2775"
    system_id: "my_system"
    password: "secret"
    system_type: ""
    addr_ton: 0
    addr_npi: 0
    address_range: ""
    interface_version: 52
    weight: 1

max_inbound_messages: 10000

inbound_storage:
  type: memory
outbound_storage:
  type: memory
```

### Server Configuration

```yaml
listen_addresses:
  - "0.0.0.0:2775"

interface_version: 52
users_file: "users.yaml"

handler_urls:
  - "http://127.0.0.1:5000/handle"
```

### Users File

```yaml
users:
  - system_id: "client1"
    password: "secret1"
  - system_id: "client2"
    password: "secret2"
```

## Quick Start

```bash
# Build
cargo build --release

# Run as SMPP client (connects to an SMSC, exposes REST API)
rest-smpp-herald client --config-file client-config.yaml

# Run as SMPP server (accepts SMPP client connections)
rest-smpp-herald server --config-file server-config.yaml
```

### Logging Options

```bash
# JSON logs to file with ISO 8601 timestamps
rest-smpp-herald client --config-file client-config.yaml \
  --log-output /var/log/smpp.log \
  --log-format json \
  --log-timestamp iso8601
```

### Send an SMS

```bash
curl -X POST http://localhost:8080/v1/sms/send \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "source_addr": "12345",
    "destination_addr": "67890",
    "short_message": "Hello, World!"
  }'
```

## REST API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/sms/send` | Send a single SMS |
| POST | `/v1/sms/send/bulk` | Send bulk SMS messages |
| GET | `/v1/sms/messages/{id}` | Get message status |
| DELETE | `/v1/sms/messages/{id}` | Cancel a message |
| GET | `/v1/sms/batches/{id}` | Get batch status |
| GET | `/v1/sms/inbound` | List inbound messages |
| GET | `/v1/sms/inbound/{id}` | Get a specific inbound message |
| GET | `/v1/gateway/status` | Gateway connection status |
| GET/POST | `/v1/gateway/smpp/connections` | List/create SMPP connections |
| PUT/DELETE | `/v1/gateway/smpp/connections/{id}` | Update/delete SMPP connection |
| POST | `/v1/gateway/smpp/connections/{id}/rebind` | Rebind an SMPP connection |
| GET/POST | `/v1/gateway/smpp/live-connections` | List/add live SMSC connections |
| GET/POST | `/v1/gateway/sender-ids` | List/create sender IDs |
| GET/POST | `/v1/gateway/numbers` | List/create phone numbers |
| PUT/DELETE | `/v1/gateway/numbers/{id}` | Update/delete phone number |
| GET/POST | `/v1/gateway/api-keys` | List/create API keys |
| PUT/DELETE | `/v1/gateway/api-keys/{id}` | Update/delete API key |
| GET/PUT | `/v1/gateway/rate-limits` | Get/update rate limits |
| GET | `/v1/utils/countries` | List supported countries |
| POST | `/v1/utils/validate-phone` | Validate a phone number |
| POST | `/v1/utils/message-parts` | Calculate message parts |
| POST | `/v1/webhooks/inbound` | Create inbound webhook |
| GET/PUT/DELETE | `/v1/webhooks/{id}` | Manage webhooks |
| POST | `/v1/webhooks/{id}/test` | Test a webhook |
| GET | `/metrics` | Prometheus metrics |

See [api-spec/](api-spec/) for the full OpenAPI specification.

## Architecture

```
┌─────────────┐        ┌─────────────────────────┐        ┌──────────┐
│             │  HTTP  │                         │  SMPP  │          │
│ Application ├───────►│   REST SMPP Herald      ├───────►│  SMSC 1  │
│             │  REST  │                         │        │          │
└─────────────┘        │  ┌─────────────────┐    │        └──────────┘
                       │  │ REST API (Axum) │    │
                       │  └────────┬────────┘    │        ┌──────────┐
                       │           │             ├───────►│  SMSC 2  │
                       │  ┌────────▼────────┐    │        │          │
                       │  │ Message Router  │    │        └──────────┘
                       │  └────────┬────────┘    │
                       │           │             │        ┌──────────┐
                       │  ┌────────▼────────┐    ├───────►│  SMSC N  │
                       │  │ SMPP Codec      │    │        │          │
                       │  └─────────────────┘    │        └──────────┘
                       └─────────────────────────┘
```

## License

See LICENSE file for details.
