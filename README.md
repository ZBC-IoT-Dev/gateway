# Gateway (Raspberry Pi Hub)

This repository contains the Raspberry Pi gateway process that sits between local MQTT devices and the cloud backend (HomeKit app + Convex API).

The gateway does four jobs:

1. Provisions itself (BLE + setup wizard).
2. Reads device telemetry from local MQTT (`discovery/announce`, `devices/+/state`).
3. Forwards telemetry to the cloud API with HMAC request signing.
4. Relays control commands from cloud/WebSocket to MQTT (`devices/<deviceId>/set`).

## 1. Hardware + Software Requirements

- Raspberry Pi 3B+/4/5/Zero 2 W (Wi-Fi + Bluetooth)
- Raspberry Pi OS / Linux with Python 3.10+
- Local MQTT broker (default: Mosquitto on same Pi)
- Arduino/UNO R4 devices publishing MQTT telemetry

System packages:

```bash
sudo apt update
sudo apt install -y \
  python3 python3-pip python3-venv \
  network-manager bluez libglib2.0-dev \
  mosquitto mosquitto-clients
```

## 2. Install

From this repo:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Run

Main process file is `hub_controller.py`.

```bash
python3 hub_controller.py
```

Useful flags:

```bash
# Run setup wizard and exit
python3 hub_controller.py --configure

# Reset and rerun setup wizard
python3 hub_controller.py --reset

# Enable/disable MQTT debug for this run
python3 hub_controller.py --mqtt-debug
python3 hub_controller.py --no-mqtt-debug
```

## 4. Configuration

Config is stored in `gateway_config.json` (auto-created). Key fields:

- `api_url`: Cloud API base URL (example: `http://<convex-site-host>:3211/api`)
- `inviteCode`: Home invite code from the HomeKit app
- `gateway_shared_secret`: Must match backend `GATEWAY_SHARED_SECRET`
- `mqtt_broker`, `mqtt_port`: Local MQTT broker
- `commands_enabled`: Enable cloud command polling
- `websocket_auth_token`: Token for LAN WebSocket control clients
- `mqtt_debug_enabled`, `mqtt_debug_topics`: Debug tracing

Optional environment overrides:

- `GATEWAY_SHARED_SECRET` -> fallback for `gateway_shared_secret`
- `HUB_WS_TOKEN` -> fallback for `websocket_auth_token`
- `MQTT_DEBUG=true` -> force MQTT debug mode

## 5. Provisioning Modes

### A. Interactive setup wizard

Run `python3 hub_controller.py --configure` and answer prompts.

### B. BLE provisioning

If not provisioned, gateway starts BLE service:

- Service UUID: `12345678-1234-5678-1234-56789abcdef0`
- Write characteristic (Wi-Fi/config): `...abcdef1`
- Read/notify characteristic (status): `...abcdef2`

Expected JSON write payload:

```json
{
  "ssid": "YOUR_WIFI_SSID",
  "password": "YOUR_WIFI_PASSWORD",
  "inviteCode": "HOME_INVITE_CODE",
  "api_url": "http://YOUR_SERVER:3211/api",
  "gateway_shared_secret": "MATCH_BACKEND_SECRET",
  "websocket_auth_token": "MATCH_FRONTEND_WS_TOKEN"
}
```

## 6. Runtime Flow

1. Connects MQTT broker.
2. Subscribes to `discovery/announce` and `devices/+/state`
3. Registers gateway in cloud: `POST /api/gateways/register`
4. Sends heartbeat every 60s: `POST /api/gateways/heartbeat`
5. For telemetry, forwards to: `POST /api/devices`
6. Polls commands (default every 3s): `GET /api/gateways/commands?gatewayIdentifier=...`
7. Acks commands: `POST /api/gateways/commands/ack`
8. Syncs paired devices: `GET /api/gateways/devices?gatewayIdentifier=...`

If cloud forwarding fails, payloads are persisted to `pending_device_queue.json` and retried with exponential backoff.

## 7. Local Files

- `gateway_config.json`: Runtime config
- `known_devices.json`: Last-seen device memory
- `pending_device_queue.json`: Durable outbound queue

## 8. MQTT Contract (Device Side)

### Incoming device telemetry

- Topic: `discovery/announce` and/or `devices/<id>/state`
- Payload: JSON (or plain text fallback)

Recommended telemetry JSON:

```json
{
  "id": "uno-r4-ABC123",
  "type": "LightSwitch",
  "hubId": "<gatewayIdentifier>",
  "state": "ON",
  "isOn": 1,
  "temp": 22.5,
  "humidity": 45.2
}
```

### Outgoing device command

- Topic: `devices/<deviceId>/set`
- Payload examples:

```json
{"state":"ON","fx":"ON"}
{"state":"OFF","fx":"OFF"}
{"toggle":true}
```

## 9. WebSocket Control Channel (LAN)

Gateway opens WebSocket server (default):

- URL: `ws://<gateway-ip>:8765/ws`
- Optional token query: `?token=<websocket_auth_token>`

Accepted command message:

```json
{
  "deviceId": "uno-r4-ABC123",
  "action": "light_fx_on",
  "command": { "state": "ON", "fx": "ON" }
}
```

Heartbeat message:

```json
{ "action": "ping" }
```

Response contains `type: "command_result"` and `status: "sent" | "failed"`.

## 10. Troubleshooting

- `Missing dependency ...`: run `pip install -r requirements.txt`
- Gateway not showing in app:
  1. confirm `inviteCode` is valid
  2. confirm `api_url` reaches Convex Site URL (`:3211`)
  3. confirm `gateway_shared_secret` matches backend
- Commands fail from UI:
  1. gateway must be `active` in app settings
  2. UI and gateway WS token must match (`NEXT_PUBLIC_HUB_WS_TOKEN` vs `websocket_auth_token`)
  3. device must subscribe to `devices/<deviceId>/set`
