# REST SMPP Herald

REST SMPP Herald is a Rust-based gateway that bridges HTTP JSON requests and SMPP PDUs.

It can run in two roles:

- **Client mode:**
  - Connects to one or more external SMSCs.
  - Exposes REST APIs to accept SMPP messages in JSON format, convert them to binary SMPP PDUs, and send them to an SMSC.
  - Accepts SMPP messages from an SMSC, converts them from binary to JSON, and forwards them to an HTTP REST server for processing.
- **Server mode:**
  - Accepts SMPP client bind requests.
  - Forwards messages to an ESME either as SMPP messages or as JSON payloads sent to an HTTP REST server for processing.

## Conversion between SMPP binary and JSON messages

### submit_sm message definition
The **Short Message Peer-to-Peer Protocol Specification** defines each SMPP message in a table. For example, **submit_sm** is defined as follows:

| Field Name | Size Octets | Type | Description | Ref. |
| --- | --- | --- | --- | --- |
| command_length | 4 | Integer | Set to overall length of PDU. | 4.7.4 |
| command_id | 4 | Integer | 0x00000004 | 4.7.5 |
| command_status | 4 | Integer | 0x00000000 | 4.7.6 |
| sequence_number | 4 | Integer | Set to a unique sequence number. The associated submit_sm_resp PDU echoes this sequence number. | 4.7.24 |
| service_type | Var. max 6 | C-Octet String | Indicates the SMS application service associated with the message. It can be used for enhanced services such as "replace by service_type" or to control the teleservice used on the air interface. Set to NULL for default MC settings. | 4.7.25 |
| source_addr_ton | 1 | Integer | Type of Number for source address. If unknown, set to NULL (Unknown). | 4.7.1 |
| source_addr_npi | 1 | Integer | Numbering Plan Indicator for source address. If unknown, set to NULL (Unknown). | 4.7.2 |
| source_addr | Var. max 21 | C-Octet String | Address of the SME that originated this message. If unknown, set to NULL (Unknown). | 4.7.29 |
| dest_addr_ton | 1 | Integer | Type of Number for destination address. | 4.7.1 |
| dest_addr_npi | 1 | Integer | Numbering Plan Indicator for destination address. | 4.7.2 |
| destination_addr | Var. max 21 | C-Octet String | Destination address of this short message. For mobile-terminated messages, this is the directory number of the recipient MS. | 4.7.8 |
| esm_class | 1 | Integer | Indicates message mode and message type. | 4.7.12 |
| protocol_id | 1 | Integer | Protocol identifier (network-specific field). | 4.7.20 |
| priority_flag | 1 | Integer | Designates the priority level of the message. | 4.7.19 |
| schedule_delivery_time | 1 or 17 | C-Octet String | Schedule requested MC delivery time. Set to NULL for immediate delivery. | 4.7.23.1 |
| validity_period | 1 or 17 | C-Octet String | Validity period of this message. Set to NULL to request the MC default validity period. Note: superseded by qos_time_to_live TLV if specified (Ref. 4.8.4.46). | 4.7.23.2 |
| registered_delivery | 1 | Integer | Indicates whether MC delivery receipt, manual ACK, delivery ACK, or intermediate notification is required. | 4.7.21 |
| replace_if_present_flag | 1 | Integer | Indicates whether the submitted message should replace an existing message. | 4.7.22 |
| data_coding | 1 | Integer | Defines the encoding scheme of the short message user data. | 4.7.7 |
| sm_default_msg_id | 1 | Integer | Indicates which pre-defined ("canned") MC message to send. If not using a canned message, set to NULL. | 4.7.27 |
| sm_length | 1 | Integer | Length in octets of the short_message user data. | 4.7.28 |
| short_message | Var. 0-255 | Octet String | Up to 255 octets of short message user data. The physical limit may vary by underlying network. Note: superseded by message_payload TLV if specified (Ref. 4.8.4.36). | 4.7.26 |
| Message Submission TLVs | Var. | TLV |  | 4.2.4 |
|  |  |  |  |  |

### Map submit_sm fields to JSON
The fields in **submit_sm** are mapped one-to-one to JSON fields with the same names, except for **command_status** and **sequence_number**:

- **command_status** in requests is always 0.
- **sequence_number** is allocated by SMPP Herald.

Example **submit_sm** JSON message:

```json
{
  "command_id": 4,
  "service_type": "CMT",
  "source_addr_ton": 0,
  "source_addr_npi": 0,
  "source_addr": "1234567",
  "dest_addr_ton": 0,
  "dest_addr_npi": 0,
  "destination_addr": "23456789",
  "esm_class": 0,
  "protocol_id": 0,
  "priority_flag": 0,
  "schedule_delivery_time": "",
  "validity_period": "",
  "registered_delivery": 0,
  "replace_if_present_flag": 0,
  "data_coding": 0,
  "sm_default_msg_id": 0,
  "short_message": "this is a test message"
}
```

`command_id` can be provided as:

- an integer,
- a hexadecimal string such as `"0x00000004"`, or
- a command name such as `"submit_sm"`.

The same request can therefore be written as:

```json
{
  "command_id": "0x00000004",
  "service_type": "CMT",
  "source_addr_ton": 0,
  "source_addr_npi": 0,
  "source_addr": "1234567",
  "dest_addr_ton": 0,
  "dest_addr_npi": 0,
  "destination_addr": "23456789",
  "esm_class": 0,
  "protocol_id": 0,
  "priority_flag": 0,
  "schedule_delivery_time": "",
  "validity_period": "",
  "registered_delivery": 0,
  "replace_if_present_flag": 0,
  "data_coding": 0,
  "sm_default_msg_id": 0,
  "short_message": "this is a test message"
}
```

or:

```json
{
  "command_id": "submit_sm",
  "service_type": "CMT",
  "source_addr_ton": 0,
  "source_addr_npi": 0,
  "source_addr": "1234567",
  "dest_addr_ton": 0,
  "dest_addr_npi": 0,
  "destination_addr": "23456789",
  "esm_class": 0,
  "protocol_id": 0,
  "priority_flag": 0,
  "schedule_delivery_time": "",
  "validity_period": "",
  "registered_delivery": 0,
  "replace_if_present_flag": 0,
  "data_coding": 0,
  "sm_default_msg_id": 0,
  "short_message": "this is a test message"
}
```

The **sm_length** field is derived from **short_message**, so it does not need to be provided in JSON.

### Encode binary/text short_message in base64
To send binary content to an SMSC, encode it as Base64 and place it in **short_message**. Text content may also be Base64-encoded and sent the same way.

After Base64 encoding, the previous **submit_sm** JSON example becomes:

```json
{
  "command_id": "submit_sm",
  "service_type": "CMT",
  "source_addr_ton": 0,
  "source_addr_npi": 0,
  "source_addr": "1234567",
  "dest_addr_ton": 0,
  "dest_addr_npi": 0,
  "destination_addr": "23456789",
  "esm_class": 0,
  "protocol_id": 0,
  "priority_flag": 0,
  "schedule_delivery_time": "",
  "validity_period": "",
  "registered_delivery": 0,
  "replace_if_present_flag": 0,
  "data_coding": 0,
  "sm_default_msg_id": 0,
  "short_message": "dGhpcyBpcyBhIHRlc3QgbWVzc2FnZQo="
}
```
In the **submit_multi** message, the field **number_of_dests** and **dest_address** are combined into **dest_addresses" field, it looks like:

```json
{
  ...
  "dest_addresses": [
    {
      "dest_addr_ton": 1,
      "dest_addr_npi": 1,
      "destination_addr": "1234567"
    },
    {
      "dl_name": "4567890"
    }
  ]
  ...
}
```

The the **number_of_dests** will be the length of array field **dest_addresses**. The **dest_flag** is not needed **dest_addresses** also. If the item of **dest_addresses** include field **destination_addr**, the **dest_flag** will be 1 and if **dl_name** is in item of **dest_addresses**, the **dest_flag** will be 2.

So a submit_multi json message would be like to:

```json
{
    "command_id": "submit_multi",
    "service_type": "CMT",
    "source_addr_ton": 1,
    "source_addr_npi": 1,
    "source_addr": "12345",
    "dest_addresses": [
        {
            "dest_addr_ton": 1,
            "dest_addr_npi": 1,
            "destination_addr": "12345"
        },
        {
            "dl_name": "455556809"
        }
    ],
    "esm_class": 0,
    "protocol_id": 0,
    "priority_flag": 0,
    "schedule_delivery_time": "",
    "validity_period": "",
    "registered_delivery": 1,
    "replace_if_present_flag": 0,
    "data_coding": 0,
    "sm_default_msg_id": 0,
    "short_message": "this is a test message"
}
```
for a **submit_multi_resp** message, the field **no_unsuccess** and **unsuccess_sme** will be combined to **unsuccess_smes** like below:

```json
{
  "command_id": "submit_multi_resp",
  ...
  "unsuccess_smes": [
    {
      "dest_addr_ton": 1,
      "dest_addr_npi": 1,
      "destination_addr": "456789",
      "error_status_code": 4
    },
    {
      "dest_addr_ton": 1,
      "dest_addr_npi": 1,
      "destination_addr": "1235670",
      "error_status_code": 2      
    }
  ]
  ...
}

```

the **no_unsuccess** field will be length of array **unsuccess_smes**.

so a **submit_multi_resp** message will look like:

```json
{
  "command_id": "submit_multi_resp",
  "command_status": 0,
  "message_id": "msg-12345",
  "unsuccess_smes": [
    {
      "dest_addr_ton": 1,
      "dest_addr_npi": 1,
      "destination_addr": "456789",
      "error_status_code": 4
    },
    {
      "dest_addr_ton": 1,
      "dest_addr_npi": 1,
      "destination_addr": "1235670",
      "error_status_code": 2      
    }
  ]
}
```
## Start SMPP Herald in client mode

### Configuration file
Put the following content in `client-config.yaml`:

```yaml
# must be: transmitter, receiver or transceiver
bind_type: transceiver
rest_addresses:
  - address: "127.0.0.1:8080"
    #key_file: "/path/to/https/key_file.pem"
    #cert_file: "/path/to/https/cert_file.pem"
    #ca_cert_file: "/path/to/https/ca_cert_file.pem"
connections:
  - address: "127.0.0.1:2775"
    system_id: "system_id"
    password: "password"
    system_type: "system_type"
    addr_ton: 0
    addr_npi: 0
    address_range: "^124"
    interface_version: 52
#session_init_timer: 30
#enquire_link_timer: 60
#inactive_timer: 120
#response_timer: 30
request_handlers:
  - urls:
      - "http://127.0.0.1:8888/handle"
      - "http://127.0.0.1:8889/handle"
    algorithm: "round_robin"
    # the address list
    #dest_addresses:
    #- 12457
    #- 12468
    # the address in regex
    #dest_address_patterns:
    #- .*678$
# alarm_config:
#  http:
#    notify_url: "http://127.0.0.1:8080/alarms"
#    alarm_id: "smsc_connection_lost"

```

### Start SMPP Herald

```shell
$ rest-smpp-herald client -c client-config.yaml
```

After startup, SMPP Herald listens on `127.0.0.1:8080` for SMPP JSON requests. Each accepted request is converted to binary SMPP format and sent to the SMSC at `127.0.0.1:2775`.

When SMPP requests are received from the SMSC, SMPP Herald converts them to JSON and forwards them to `127.0.0.1:8888` and `127.0.0.1:8889` for processing. Those HTTP servers return SMPP responses in JSON format.

### Submit a message

After SMPP Herald starts with the above configuration, you can submit a `submit_sm` message in JSON through the REST interface:

```shell
$ curl http://127.0.0.1:8080/v1/smpp -H "Content-Type: application/json" -d '{
  "command_id": "0x00000004",
  "service_type": "CMT",
  "source_addr_ton": 0,
  "source_addr_npi": 0,
  "source_addr": "1234567",
  "dest_addr_ton": 0,
  "dest_addr_npi": 0,
  "destination_addr": "23456789",
  "esm_class": 0,
  "protocol_id": 0,
  "priority_flag": 0,
  "schedule_delivery_time": "",
  "validity_period": "",
  "registered_delivery": 0,
  "replace_if_present_flag": 0,
  "data_coding": 0,
  "sm_default_msg_id": 0,
  "short_message": "this is a test message"
}'
```

This JSON `submit_sm` message is converted to binary SMPP format and sent to one of the configured SMSCs.

## Start SMPP Herald in server mode

### Configuration file

Put the following YAML in `server-config.yaml`:

```yaml
listen_addresses:
  - "127.0.0.1:2775"
interface_version: 52
rest_addresses:
  - address: "127.0.0.1:8880"
request_handlers:
  - urls:
      - "http://127.0.0.1:5000/handle"
    algorithm: "round-robin"
    dest_addresses:
      - "1234567890"
    dest_address_patterns:
      - "^128"
#ip_whitelist:
user_auth:
  users_file: "users.yaml"
  #auth_url: "http://127.0.0.1:8866/auth"
session_init_timer: 30
enquire_link_timer: 60
inactive_timer: 120
response_timer: 30
distribution_list:
  - name: "list1"
    addresses:
      - ton: 0
        npi: 1
        address: "1234567890"
      - ton: 0
        npi: 1
        address: "0987654321"
# default validity_period is in seconds, default is 3600 (1 hour)
default_validity_period: 3600
# default_mc_mode is one of "datagram", "forward", or "store_forward"
default_mc_mode: "store_forward"
# predefined_messages is a list of messages used by sm_default_msg_id
predefined_messages:
- this is a test message
- this is another test message
default_service_type: "CMT"
```

Put the following YAML in `users.yaml`:

```yaml
users:
  - system_id: "system_id"
    password: "password"
  - system_id: "client2"
    password: "secret2"
```

The server listens on `127.0.0.1:2775` for SMPP binary messages from ESMEs.

When a `bind_transmitter`, `bind_receiver`, or `bind_transceiver` message is received, SMPP Herald extracts `system_id` and `password`. If they match an entry in `users.yaml`, the bind succeeds; otherwise, it fails.

If `destination_addr` in `submit_sm`, `data_sm`, `deliver_sm`, or `submit_multi` is listed in **dest_addresses** or matches **dest_address_patterns**, SMPP Herald converts the SMPP binary message to JSON and forwards it to one of the HTTP REST servers in **urls**. The REST server should return the corresponding SMPP response in JSON format.

### Start SMPP Herald in server mode

Start SMPP Herald in server mode with:

```shell
$ rest-smpp-herald server -c server-config.yaml
```

## Supported messages

SMPP Herald supports the following messages:

- submit_sm
- submit_multi_sm
- data_sm
- query_sm
- replace_sm
- cancel_sm
- broadcast_sm
- query_broadcast_sm
- cancel_broadcast_sm

## Default field values

For each SMPP message received via the REST HTTP interface in JSON format, you can provide all fields explicitly, or omit fields that have defaults. The table below lists the default values by message type:

| message name | field name | default value |
| --- | --- | --- |
| **submit_sm/data_sm/deliver_sm** | service_type | empty string |
|  | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
|  | dest_addr_ton | 0 |
|  | dest_addr_npi | 0 |
|  | esm_class | 0 |
|  | protocol_id | 0 |
|  | priority_flag | 0 |
|  | schedule_delivery_time | empty string |
|  | validity_period | empty string |
|  | registered_delivery | 0 |
|  | replace_if_present_flag | 0 |
|  | data_coding | 0 |
|  | sm_default_msg_id | 0 |
| **submit_multi_sm** | service_type | empty string |
|  | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
|  | dest_addresses, array of (dest_addr_ton,dest_addr_npi,destination_addr)/dl_name, default value for dest_addr_ton and dest_addr_npi is 0 |  |
|  | esm_class | 0 |
|  | protocol_id | 0 |
|  | priority_flag | 0 |
|  | schedule_delivery_time | empty string |
|  | validity_period | empty string |
|  | registered_delivery | 0 |
|  | replace_if_present_flag | 0 |
|  | data_coding | 0 |
|  | sm_default_msg_id | 0 |
| **broadcast_sm** | service_type | empty string |
|  | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
|  | message_id | empty string |
|  | priority_flag | 0 |
|  | schedule_delivery_time | empty string |
|  | validity_period | empty string |
|  | replace_if_present_flag | 0 |
|  | sm_default_msg_id | empty string |
| **cancel_sm** | service_type | empty string |
|  | message_id | empty string |
|  | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
|  | dest_addr_ton | 0 |
|  | dest_addr_npi | 0 |
| **query_sm** | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
| **replace_sm** | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
|  | schedule_delivery_time | empty string |
|  | validity_period | empty string |
|  | registered_delivery | 0 |
|  | sm_default_msg_id | 0 |
| **query_broadcast_sm** | message_id | empty string |
|  | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
| **cancel_broadcast_sm** | service_type | empty string |
|  | message_id | empty string |
|  | source_addr_ton | 0 |
|  | source_addr_npi | 0 |
|  | source_addr | empty string |
|  |  |  |

The simplest **submit_sm** message looks like this:

```json
{
  "command_id": "submit_sm",
  "destination_addr": "23456789",
  "short_message": "dGhpcyBpcyBhIHRlc3QgbWVzc2FnZQo="
}
```

Default values are applied to any missing fields in the request above.

## License

See [LICENSE](LICENSE).
