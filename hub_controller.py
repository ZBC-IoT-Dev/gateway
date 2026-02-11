import argparse
import asyncio
import json
import logging
import os
import socket
import subprocess
import sys
import time
import uuid
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

try:
    import paho.mqtt.client as mqtt
    import requests
    import websockets
    from websockets.exceptions import ConnectionClosed
    from bless import (
        BlessGATTCharacteristic,
        BlessServer,
        GATTAttributePermissions,
        GATTCharacteristicProperties,
    )
except ImportError as e:
    print(
        "Error: Missing dependency "
        f"{e.name}. Please install: pip install paho-mqtt requests bless websockets"
    )
    sys.exit(1)

# === CONFIGURATION ===
CONFIG_FILE = "gateway_config.json"
KNOWN_DEVICES_FILE = "known_devices.json"
PENDING_QUEUE_FILE = "pending_device_queue.json"

DEFAULT_CONFIG = {
    "inviteCode": "",
    "api_url": "http://169.254.13.52:3211/api",
    "mqtt_broker": "localhost",
    "mqtt_port": 1883,
    "device_name": "Gateway_Pi",
    "provisioned": False,
    # Cloud command polling
    "commands_enabled": True,
    "commands_poll_interval_sec": 3,
    "commands_path": "/gateways/commands",
    "commands_ack_path": "/gateways/commands/ack",
    # Local realtime control
    "websocket_enabled": True,
    "websocket_host": "0.0.0.0",
    "websocket_port": 8765,
    "websocket_path": "/ws",
}

# BLE CONSTANTS
SERVICE_UUID = "12345678-1234-5678-1234-56789abcdef0"
CHAR_WIFI_UUID = "12345678-1234-5678-1234-56789abcdef1"
CHAR_STATUS_UUID = "12345678-1234-5678-1234-56789abcdef2"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GLOBAL STATE
config: Dict[str, Any] = {}
known_devices: Dict[str, Dict[str, Any]] = {}
pending_queue: List[Dict[str, Any]] = []
mqtt_client: Optional[mqtt.Client] = None
device_identifier = hex(uuid.getnode())
server: Optional[BlessServer] = None
main_event_loop: Optional[asyncio.AbstractEventLoop] = None
websocket_clients: Set[Any] = set()

# Deduplicate cloud commands in-memory
processed_command_ids: Deque[str] = deque(maxlen=500)
processed_command_set: Set[str] = set()

# If ack endpoint is missing in API, disable noisy repeated errors
command_ack_supported: Optional[bool] = None


def print_banner():
    print(
        r"""
   _____       _       _____ _
  / ____|     | |     |  __ (_)
 | |  __  __ _| |_ ___| |__) |
 | | |_ |/ _` | __/ _ \  ___/ |
 | |__| | (_| | ||  __/ |   | |
  \_____|\__,_|\__\___|_|   |_|
"""
    )


def _load_json(path: str, default: Any):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        logger.warning("Failed to read %s: %s", path, e)
        return default


def _save_json(path: str, payload: Any):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp_path, path)


def _api_url(path: str) -> str:
    base = str(config.get("api_url", "")).rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def load_config():
    global config
    config = _load_json(CONFIG_FILE, DEFAULT_CONFIG.copy())
    for k, v in DEFAULT_CONFIG.items():
        if k not in config:
            config[k] = v


def save_config():
    _save_json(CONFIG_FILE, config)


def load_state():
    global known_devices, pending_queue
    known_devices = _load_json(KNOWN_DEVICES_FILE, {})
    pending_queue = _load_json(PENDING_QUEUE_FILE, [])


def save_known_devices():
    _save_json(KNOWN_DEVICES_FILE, known_devices)


def save_pending_queue():
    _save_json(PENDING_QUEUE_FILE, pending_queue)


# === WIFI UTILS ===
def connect_wifi(ssid: str, password: str):
    logger.info("Attempting to connect to WiFi: %s", ssid)
    try:
        subprocess.run(["nmcli", "connection", "delete", ssid], capture_output=True)
        subprocess.run(
            ["nmcli", "dev", "wifi", "connect", ssid, "password", password],
            check=True,
            timeout=45,
        )
        return True
    except Exception as e:
        logger.error("WiFi Connection Failed: %s", e)
        return False


def check_internet():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return True
    except OSError:
        return False


# === API UTILS ===
def register_gateway():
    url = _api_url("/gateways/register")
    payload = {
        "inviteCode": config["inviteCode"],
        "identifier": device_identifier,
        "name": config["device_name"],
        "type": "raspberry_pi_4",
    }
    logger.info("Registering with: %s", url)
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code in (200, 201):
            logger.info("Gateway registered successfully")
            return True
        logger.error("Registration failed (%s): %s", resp.status_code, resp.text)
        return False
    except Exception as e:
        logger.error("Register API error: %s", e)
        return False


def send_heartbeat():
    url = _api_url("/gateways/heartbeat")
    try:
        requests.post(url, json={"identifier": device_identifier}, timeout=5)
    except Exception as e:
        logger.error("Heartbeat failed: %s", e)


def sync_known_devices_from_cloud():
    url = _api_url("/gateways/devices")
    try:
        resp = requests.get(
            url,
            params={"gatewayIdentifier": device_identifier},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning("Device sync failed (%s): %s", resp.status_code, resp.text)
            return

        rows = resp.json()
        changed = False
        for row in rows:
            identifier = str(row.get("identifier", "")).strip()
            if not identifier:
                continue
            existing = known_devices.get(identifier, {})
            known_devices[identifier] = {
                "identifier": identifier,
                "type": row.get("type", existing.get("type", "other")),
                "name": row.get("name", existing.get("name", "")),
                "paired": True,
                "lastSeen": existing.get("lastSeen", 0),
                "lastSynced": int(time.time()),
            }
            changed = True
        if changed:
            save_known_devices()
            logger.info("Synced %d known devices from cloud", len(rows))
    except Exception as e:
        logger.error("Cloud device sync error: %s", e)


def forward_device_data(payload: Dict[str, Any]):
    url = _api_url("/devices")
    identifier = str(
        payload.get("id")
        or payload.get("identifier")
        or payload.get("deviceId")
        or ""
    ).strip()
    if not identifier:
        logger.warning("Skipping device forward: missing identifier in payload")
        return False

    api_payload = {
        "identifier": identifier,
        "type": payload.get("type", "unknown"),
        "data": payload,
        "gatewayIdentifier": device_identifier,
    }
    resp = requests.post(url, json=api_payload, timeout=6)
    return resp.status_code in (200, 201)


def enqueue_payload(payload: Dict[str, Any]):
    pending_queue.append(payload)
    if len(pending_queue) > 1000:
        del pending_queue[0 : len(pending_queue) - 1000]
    save_pending_queue()


def flush_pending_queue(max_items: int = 25):
    if not pending_queue or not config.get("provisioned"):
        return

    sent = 0
    while pending_queue and sent < max_items:
        candidate = pending_queue[0]
        try:
            if forward_device_data(candidate):
                pending_queue.pop(0)
                sent += 1
            else:
                break
        except Exception:
            break

    if sent > 0:
        save_pending_queue()
        logger.info("Flushed %d queued payload(s)", sent)


def mark_device_seen(payload: Dict[str, Any], fallback_identifier: str = ""):
    identifier = str(payload.get("id", "")).strip() or fallback_identifier.strip()
    if not identifier:
        return ""
    existing = known_devices.get(identifier, {})
    known_devices[identifier] = {
        "identifier": identifier,
        "type": payload.get("type", existing.get("type", "other")),
        "name": existing.get("name", ""),
        "paired": existing.get("paired", False),
        "lastSeen": int(time.time()),
        "lastSynced": existing.get("lastSynced", 0),
    }
    save_known_devices()
    return identifier


# === CLOUD COMMANDS (API -> MQTT DEVICE) ===
def _remember_command_id(command_id: str):
    if not command_id:
        return
    if command_id in processed_command_set:
        return
    if len(processed_command_ids) == processed_command_ids.maxlen:
        oldest = processed_command_ids.popleft()
        processed_command_set.discard(oldest)
    processed_command_ids.append(command_id)
    processed_command_set.add(command_id)


def _extract_device_id_from_topic(topic: str) -> str:
    # Expected: devices/<deviceId>/state OR devices/<deviceId>/set
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "devices":
        return parts[1]
    return ""


def _normalize_command_row(row: Dict[str, Any]) -> Tuple[str, str, Any]:
    command_id = str(row.get("id") or row.get("commandId") or "").strip()
    device_id = str(
        row.get("deviceIdentifier")
        or row.get("identifier")
        or row.get("deviceId")
        or row.get("targetId")
        or ""
    ).strip()

    payload = row.get("command")
    if payload is None:
        payload = row.get("payload")
    if payload is None:
        payload = row.get("data")

    # Allow simplified command rows with only "state"/"isOn"
    if payload is None:
        if "state" in row:
            payload = {"state": row.get("state")}
        elif "isOn" in row:
            payload = {"isOn": row.get("isOn")}
        elif "value" in row:
            payload = {"state": row.get("value")}

    if payload is None:
        payload = {"state": "OFF"}

    return command_id, device_id, payload


def _publish_command_to_device(device_id: str, command_payload: Any) -> Tuple[bool, str]:
    global mqtt_client
    if not mqtt_client:
        return False, "mqtt_client_not_initialized"

    topic = f"devices/{device_id}/set"

    if isinstance(command_payload, (dict, list)):
        payload = json.dumps(command_payload)
    elif isinstance(command_payload, (str, int, float, bool)):
        payload = str(command_payload)
    else:
        payload = json.dumps({"state": "OFF"})

    try:
        info = mqtt_client.publish(topic, payload, qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return False, f"mqtt_publish_error_{info.rc}"

        # wait_for_publish exists in paho message info
        try:
            info.wait_for_publish(timeout=2.0)
        except Exception:
            pass

        logger.info("Dispatched command -> %s payload=%s", topic, payload)
        return True, ""
    except Exception as e:
        return False, str(e)


def _extract_device_id_from_websocket_message(message: Dict[str, Any]) -> str:
    return str(
        message.get("deviceId")
        or message.get("deviceIdentifier")
        or message.get("identifier")
        or message.get("targetId")
        or ""
    ).strip()


def _build_command_payload_from_websocket_message(message: Dict[str, Any]) -> Any:
    payload = message.get("command")
    if payload is None:
        payload = message.get("payload")
    if payload is not None:
        return payload

    normalized: Dict[str, Any] = {}
    if "state" in message:
        normalized["state"] = message.get("state")
    if "isOn" in message:
        normalized["isOn"] = message.get("isOn")
    if "fx" in message:
        normalized["fx"] = message.get("fx")

    action = str(message.get("action", "")).strip().lower()
    if action in ("light_fx_on", "fx_on", "light_on"):
        normalized.setdefault("state", "ON")
        normalized.setdefault("fx", "ON")
    elif action in ("light_fx_off", "fx_off", "light_off"):
        normalized.setdefault("state", "OFF")
        normalized.setdefault("fx", "OFF")

    return normalized or {"state": "ON", "fx": "ON"}


async def _broadcast_websocket_event(event: Dict[str, Any]):
    if not websocket_clients:
        return

    data = json.dumps(event)
    disconnected: List[Any] = []
    for ws in list(websocket_clients):
        try:
            await ws.send(data)
        except Exception:
            disconnected.append(ws)

    for ws in disconnected:
        websocket_clients.discard(ws)


def _schedule_websocket_event(event: Dict[str, Any]):
    if not websocket_clients:
        return
    if main_event_loop is None or main_event_loop.is_closed():
        return
    try:
        asyncio.run_coroutine_threadsafe(
            _broadcast_websocket_event(event),
            main_event_loop,
        )
    except Exception as e:
        logger.debug("WebSocket event scheduling failed: %s", e)


async def _handle_websocket_message(websocket: Any, raw_message: Any):
    if isinstance(raw_message, bytes):
        raw_text = raw_message.decode("utf-8", errors="ignore")
    else:
        raw_text = str(raw_message)

    try:
        body = json.loads(raw_text)
    except json.JSONDecodeError:
        await websocket.send(
            json.dumps(
                {
                    "type": "error",
                    "error": "invalid_json",
                    "message": "Expected JSON payload",
                }
            )
        )
        return

    if not isinstance(body, dict):
        await websocket.send(
            json.dumps(
                {
                    "type": "error",
                    "error": "invalid_payload",
                    "message": "Payload must be a JSON object",
                }
            )
        )
        return

    action = str(body.get("action", "set")).strip().lower()
    if action in ("ping", "health"):
        await websocket.send(
            json.dumps(
                {
                    "type": "pong",
                    "gatewayIdentifier": device_identifier,
                    "mqttConnected": bool(mqtt_client and mqtt_client.is_connected()),
                }
            )
        )
        return

    device_id = _extract_device_id_from_websocket_message(body)
    if not device_id:
        await websocket.send(
            json.dumps(
                {
                    "type": "error",
                    "error": "missing_device_id",
                    "message": "Provide deviceId/deviceIdentifier/identifier/targetId",
                }
            )
        )
        return

    command_payload = _build_command_payload_from_websocket_message(body)
    ok, err = _publish_command_to_device(device_id, command_payload)
    await websocket.send(
        json.dumps(
            {
                "type": "command_result",
                "deviceId": device_id,
                "status": "sent" if ok else "failed",
                "error": err,
                "command": command_payload,
                "timestamp": int(time.time()),
            }
        )
    )


async def _handle_websocket_connection(websocket: Any, path: Optional[str] = None):
    expected_path = str(config.get("websocket_path", "/ws"))
    current_path = str(path or getattr(websocket, "path", "") or "")
    if expected_path and current_path and current_path != expected_path:
        await websocket.send(
            json.dumps(
                {
                    "type": "error",
                    "error": "invalid_path",
                    "expectedPath": expected_path,
                }
            )
        )
        await websocket.close(code=1008, reason="Invalid websocket path")
        return

    websocket_clients.add(websocket)
    await websocket.send(
        json.dumps(
            {
                "type": "welcome",
                "gatewayIdentifier": device_identifier,
                "path": expected_path,
            }
        )
    )

    try:
        async for message in websocket:
            await _handle_websocket_message(websocket, message)
    except ConnectionClosed:
        pass
    except Exception as e:
        logger.warning("WebSocket client error: %s", e)
    finally:
        websocket_clients.discard(websocket)


async def run_websocket_server():
    if not config.get("websocket_enabled", True):
        logger.info("WebSocket server disabled in config")
        return

    host = str(config.get("websocket_host", "0.0.0.0"))
    port = int(config.get("websocket_port", 8765))
    path = str(config.get("websocket_path", "/ws"))

    ws_server = await websockets.serve(
        _handle_websocket_connection,
        host,
        port,
        ping_interval=20,
        ping_timeout=20,
        max_size=1024 * 1024,
    )
    logger.info("WebSocket control active at ws://%s:%d%s", host, port, path)
    await ws_server.wait_closed()


def _ack_command(command_id: str, device_id: str, success: bool, error: str = ""):
    global command_ack_supported
    if not command_id:
        return

    if command_ack_supported is False:
        return

    url = _api_url(str(config.get("commands_ack_path", "/gateways/commands/ack")))
    payload = {
        "commandId": command_id,
        "gatewayIdentifier": device_identifier,
        "deviceIdentifier": device_id,
        "status": "sent" if success else "failed",
        "error": error,
        "timestamp": int(time.time()),
    }

    try:
        resp = requests.post(url, json=payload, timeout=6)
        if resp.status_code in (200, 201, 204):
            command_ack_supported = True
            return
        if resp.status_code == 404:
            command_ack_supported = False
            logger.warning("Command ACK endpoint not found: %s (disabled)", url)
            return
        logger.warning("Command ACK failed (%s): %s", resp.status_code, resp.text)
    except Exception as e:
        logger.warning("Command ACK error: %s", e)


def poll_and_dispatch_commands():
    if not config.get("provisioned"):
        return
    if not config.get("commands_enabled", True):
        return

    path = str(config.get("commands_path", "/gateways/commands"))
    url = _api_url(path)

    try:
        resp = requests.get(
            url,
            params={"gatewayIdentifier": device_identifier},
            timeout=8,
        )
        if resp.status_code == 404:
            logger.debug("Commands endpoint not found: %s", url)
            return
        if resp.status_code != 200:
            logger.warning("Command poll failed (%s): %s", resp.status_code, resp.text)
            return

        rows = resp.json()
        if not isinstance(rows, list) or not rows:
            return

        for row in rows:
            if not isinstance(row, dict):
                continue

            command_id, device_id, command_payload = _normalize_command_row(row)
            if not device_id:
                logger.warning("Skipping command without device id: %s", row)
                continue

            if command_id and command_id in processed_command_set:
                continue

            ok, err = _publish_command_to_device(device_id, command_payload)
            if command_id:
                _ack_command(command_id, device_id, ok, err)
                _remember_command_id(command_id)

    except Exception as e:
        logger.error("Command polling error: %s", e)


# === MQTT HANDLERS ===
def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("MQTT connected")
        client.subscribe("discovery/announce")
        client.subscribe("devices/+/state")
        sync_known_devices_from_cloud()
        flush_pending_queue()
    else:
        logger.error("MQTT connection failed: %s", rc)


def on_mqtt_message(client, userdata, msg):
    try:
        text = msg.payload.decode("utf-8")
        fallback_id = _extract_device_id_from_topic(msg.topic)

        payload: Dict[str, Any]
        try:
            payload = json.loads(text)
            if not isinstance(payload, dict):
                payload = {"raw": payload}
        except json.JSONDecodeError:
            # Support plain text state payloads if needed
            payload = {"raw": text}

        device_id = mark_device_seen(payload, fallback_identifier=fallback_id)
        if not device_id:
            return

        # Ensure cloud payload always carries canonical identifier from topic fallback.
        if not str(payload.get("id", "")).strip():
            payload["id"] = device_id

        if config.get("provisioned"):
            enqueue_payload(payload)
            flush_pending_queue(max_items=5)
        else:
            logger.debug("Gateway not provisioned. Cached device %s locally.", device_id)

        _schedule_websocket_event(
            {
                "type": "device_state",
                "topic": msg.topic,
                "deviceId": device_id,
                "payload": payload,
                "timestamp": int(time.time()),
            }
        )
    except Exception as e:
        logger.error("MQTT message handling error: %s", e)


# === PROVISIONING SERVER ===
async def run_ble_provisioning(loop):
    global server
    logger.info("Starting BLE provisioning service")
    trigger_event = asyncio.Event()

    def read_request(characteristic: BlessGATTCharacteristic, **kwargs) -> bytes:
        status = "provisioned" if config.get("provisioned") else "waiting"
        return status.encode()

    def write_request(characteristic: BlessGATTCharacteristic, value: Any, **kwargs):
        logger.info("BLE write request on %s", characteristic.uuid)
        try:
            data = json.loads(value.decode("utf-8"))
            if "name" in data:
                config["device_name"] = data["name"]
            if "inviteCode" in data:
                config["inviteCode"] = data["inviteCode"]
            if "api_url" in data:
                config["api_url"] = data["api_url"]

            wifi_success = True
            if data.get("ssid"):
                wifi_success = connect_wifi(data["ssid"], data.get("password", ""))

            if wifi_success and register_gateway():
                config["provisioned"] = True
                save_config()
                sync_known_devices_from_cloud()
                logger.info("Provisioning complete")
            else:
                logger.error("Provisioning failed")
        except Exception as e:
            logger.error("BLE write error: %s", e)

    server = BlessServer(name=config["device_name"], loop=loop)
    server.read_request_func = read_request
    server.write_request_func = write_request

    await server.add_new_service(SERVICE_UUID)
    await server.add_new_characteristic(
        SERVICE_UUID,
        CHAR_WIFI_UUID,
        GATTCharacteristicProperties.write,
        None,
        GATTAttributePermissions.writeable,
    )
    await server.add_new_characteristic(
        SERVICE_UUID,
        CHAR_STATUS_UUID,
        GATTCharacteristicProperties.read | GATTCharacteristicProperties.notify,
        b"init",
        GATTAttributePermissions.readable,
    )
    await server.start()
    logger.info("BLE provisioning active")
    await trigger_event.wait()
    await server.stop()


def setup_wizard():
    print("\n========================================")
    print("INITIAL SETUP WIZARD")
    print("========================================")
    default_name = config.get("device_name", "Gateway_Pi")
    name = input(f"Device Name [{default_name}]: ").strip()
    if name:
        config["device_name"] = name

    default_api = config.get("api_url", "http://169.254.13.52:3211/api")
    api_url = input(f"API URL [{default_api}]: ").strip()
    if api_url:
        config["api_url"] = api_url

    invite = input("Invite Code (optional): ").strip()
    if invite:
        config["inviteCode"] = invite

    commands_enabled_input = input("Enable cloud commands? [Y/n]: ").strip().lower()
    config["commands_enabled"] = commands_enabled_input not in ("n", "no")

    config["provisioned"] = False
    save_config()


async def main_loop():
    parser = argparse.ArgumentParser(description="Raspberry Pi BLE Gateway")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset configuration and run setup wizard",
    )
    args = parser.parse_args()

    print_banner()
    load_config()
    load_state()
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()

    if args.reset or not os.path.exists(CONFIG_FILE):
        if args.reset:
            print("Resetting configuration...")
        setup_wizard()

    if config.get("provisioned"):
        register_gateway()
        sync_known_devices_from_cloud()

    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

    try:
        mqtt_client.connect(config["mqtt_broker"], int(config["mqtt_port"]), 60)
        mqtt_client.loop_start()
    except Exception as e:
        logger.warning("MQTT init error: %s", e)

    asyncio.create_task(run_ble_provisioning(asyncio.get_running_loop()))
    asyncio.create_task(run_websocket_server())

    last_heartbeat_at = 0.0
    last_sync_at = 0.0
    last_command_poll_at = 0.0

    while True:
        now = time.time()

        if config.get("provisioned"):
            if now - last_heartbeat_at >= 60:
                send_heartbeat()
                flush_pending_queue(max_items=20)
                last_heartbeat_at = now

            if now - last_sync_at >= 300:
                sync_known_devices_from_cloud()
                last_sync_at = now

            poll_interval = max(1, int(config.get("commands_poll_interval_sec", 3)))
            if now - last_command_poll_at >= poll_interval:
                poll_and_dispatch_commands()
                last_command_poll_at = now

        await asyncio.sleep(1)


if __name__ == "__main__":
    if sys.platform == "darwin":
        os.environ["PYTHONASYNCIODEBUG"] = "1"
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass