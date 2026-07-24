# REST SMPP Herald

REST SMPP Herald is a Rust gateway that bridges HTTP JSON requests and SMPP PDUs.

It can run in two roles:

- Client mode: connect to one or more external SMSCs and expose REST APIs.
- Server mode: accept SMPP client binds and optionally expose REST APIs that forward SMPP messages to connected clients.

## Implemented REST APIs

### Client mode APIs

| Method | Path | Description |
| --- | --- | --- |
| POST | /v1/smpp | Send a raw SMPP request as JSON (command_name or command_id + fields) |
| GET | /v1/gateway/status | Gateway status and configured SMPP connection metadata |
| GET | /v1/gateway/smpp/connections | List configured SMPP connections |
| POST | /v1/gateway/smpp/connections | Create a configured SMPP connection entry |
| PUT | /v1/gateway/smpp/connections/{connection_id} | Update a configured SMPP connection entry |
| DELETE | /v1/gateway/smpp/connections/{connection_id} | Delete a configured SMPP connection entry |
| POST | /v1/gateway/smpp/connections/{connection_id}/rebind | Mark a configured SMPP connection for rebind |
| GET | /v1/gateway/smpp/live-connections | List currently active live connection handles |
| POST | /v1/gateway/smpp/live-connections | Add a live SMSC connection at runtime |
| GET | /metrics | Prometheus metrics |

### Server mode APIs

| Method | Path | Description |
| --- | --- | --- |
| POST | /v1/smpp | Accept SMPP JSON and forward supported requests to connected SMPP clients |
| GET | /v1/healthz | Health check |

## /v1/smpp message support

The client mode /v1/smpp endpoint accepts only SMPP client request PDUs. The payload must include either:

- command_name (for example: submit_sm)
- command_id (number, hex string like 0x00000004, or supported command name variant)

### Supported message types in client mode

| SMPP command | command_name value | command_id |
| --- | --- | --- |
| BindReceiver | bind_receiver | 0x00000001 |
| BindTransmitter | bind_transmitter | 0x00000002 |
| BindTransceiver | bind_transceiver | 0x00000009 |
| Unbind | unbind | 0x00000006 |
| SubmitSm | submit_sm | 0x00000004 |
| SubmitMulti | submit_multi | 0x00000021 |
| DataSm | data_sm | 0x00000103 |
| QuerySm | query_sm | 0x00000003 |
| CancelSm | cancel_sm | 0x00000008 |
| ReplaceSm | replace_sm | 0x00000007 |
| BroadcastSm | broadcast_sm | 0x00000111 |
| QueryBroadcastSm | query_broadcast_sm | 0x00000112 |
| CancelBroadcastSm | cancel_broadcast_sm | 0x00000113 |

Notes:

- Response PDUs such as submit_sm_resp are rejected by the client mode /v1/smpp handler.
- Unknown or unsupported command IDs are rejected.

### Server mode /v1/smpp behavior

Server mode currently processes DeliverSm for forwarding to connected SMPP clients. Other commands can be parsed but are treated as unsupported in forwarding logic.

## Example payloads

Sample JSON payloads are available in:

- [submit-sm-example.json](submit-sm-example.json)
- [submit-sm-with-schedule-delivery-time-example.json](submit-sm-with-schedule-delivery-time-example.json)
- [submit-multi-example.json](submit-multi-example.json)
- [data-sm-example.json](data-sm-example.json)
- [deliver-sm-example.json](deliver-sm-example.json)
- [cancel-sm-example.json](cancel-sm-example.json)
- [replace-sm-example.json](replace-sm-example.json)

## Configuration

Configuration files can be YAML or JSON.

### Client configuration example

```yaml
rest_addresses:
  - address: "127.0.0.1:8080"

connections:
  - address: "127.0.0.1:2775"
    system_id: "my_system"
    password: "secret"
    system_type: ""
    addr_ton: 0
    addr_npi: 0
    address_range: ""
    interface_version: 52

load_balancing_algorithm: "round_robin"
request_handlers:
  - urls:
      - "http://127.0.0.1:8888/handle"
    algorithm: "round_robin"
```

### Server configuration example

```yaml
listen_addresses:
  - "127.0.0.1:2775"

interface_version: 52
rest_addresses:
  - address: "127.0.0.1:8880"

users_file: "users.yaml"
# auth_url: "http://localhost:9000/auth"

request_handlers:
  - urls:
      - "http://127.0.0.1:5000/handle"
    algorithm: "round-robin"
```

## Quick start

```bash
# Build
cargo build --release

# Run as SMPP client
rest-smpp-herald client --config-file client-config-1.yaml

# Run as SMPP server
rest-smpp-herald server --config-file server-config.yaml
```

## Logging options

```bash
# JSON logs to file with ISO 8601 timestamps
rest-smpp-herald client --config-file client-config-1.yaml \
  --log-output ./smpp.log \
  --log-format json \
  --log-timestamp iso8601
```

## License

See [LICENSE](LICENSE).
