import asyncio
import json
import logging
import signal
import sys
import uuid
import os
import time
from typing import Any, Dict

# Check if libraries are installed, otherwise provide friendly error
try:
    import paho.mqtt.client as mqtt
    import requests
    import socket
    from bless import (
        BlessServer,
        BlessGATTCharacteristic,
        GATTCharacteristicProperties,
        GATTAttributePermissions,
    )
except ImportError as e:
    print(f"Error: Missing dependency {e.name}. Please install: pip install paho-mqtt requests netifaces bless")
    sys.exit(1)

import subprocess

# === CONFIGURATION ===
CONFIG_FILE = "gateway_config.json"
DEFAULT_CONFIG = {
    "inviteCode": "",
    "api_url": "http://169.254.13.52:3211/api", # Default to replace
    "mqtt_broker": "localhost",
    "mqtt_port": 1883,
    "device_name": "Gateway_Pi",
    "provisioned": False
}

# BLE CONSTANTS
SERVICE_UUID = "12345678-1234-5678-1234-56789abcdef0"
CHAR_WIFI_UUID = "12345678-1234-5678-1234-56789abcdef1"  # Write Credentials (JSON)
CHAR_STATUS_UUID = "12345678-1234-5678-1234-56789abcdef2" # Read/Notify Status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GLOBAL STATE
config = {}
scan_detected_devices = set()
mqtt_client = None
device_identifier = hex(uuid.getnode())
server = None

def print_banner():
    print(r"""
   _____       _eway   _____ _ 
  / ____|     | |     |  __ (_)
 | |  __  __ _| |_ ___| |__) | 
 | | |_ |/ _` | __/ _ \  ___/ |
 | |__| | (_| | ||  __/ |   | |
  \_____|\__,_|\__\___|_|   |_|
  
  Raspberry Pi BLE Provisioning Service
    """)

def load_config():
    global config
    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
            # Ensure defaults exists
            for k, v in DEFAULT_CONFIG.items():
                if k not in config:
                    config[k] = v
    except FileNotFoundError:
        config = DEFAULT_CONFIG
        save_config()

def save_config():
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

# === WIFI UTILS ===
def connect_wifi(ssid, password):
    logger.info(f"Attempting to connect to WiFi: {ssid}")
    try:
        # 1. Clean up existing connection to avoid "property missing" errors or conflicts
        subprocess.run(["nmcli", "connection", "delete", ssid], capture_output=True)
        
        # 2. Connect
        # Using nmcli for NetworkManager (Standard on RPi OS)
        subprocess.run(["nmcli", "dev", "wifi", "connect", ssid, "password", password], check=True, timeout=45)
        return True
    except Exception as e:
        logger.error(f"WiFi Connection Failed: {e}")
        return False

def check_internet():
    try:
        # Connect to a public DNS server (Google's 8.8.8.8) to check connectivity
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return True
    except OSError:
        return False

# === API UTILS ===
def register_gateway():
    url = f"{config['api_url']}/gateways/register"
    payload = {
        "inviteCode": config["inviteCode"],
        "identifier": device_identifier,
        "name": config["device_name"],
        "type": "raspberry_pi_4"
    }
    logger.info(f"Registering with: {url}")
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200 or resp.status_code == 201:
            logger.info("Gateway Registered Successfully")
            return True
        else:
            logger.error(f"Registration Failed: {resp.text}")
            return False
    except Exception as e:
        logger.error(f"API Error: {e}")
        return False

def send_heartbeat():
    url = f"{config['api_url']}/gateways/heartbeat"
    try:
        requests.post(url, json={"identifier": device_identifier}, timeout=5)
        logger.debug("Heartbeat sent")
    except Exception as e:
        logger.error(f"Heartbeat Failed: {e}")

# === API UTILS ===
def register_gateway():
    # ... (existing code, not replacing this part, just context)
    pass 

def forward_device_data(payload):
    url = f"{config['api_url']}/devices"
    try:
        api_payload = {
            "identifier": payload.get("id"),
            "type": payload.get("type", "unknown"),
            "data": payload,
            "gatewayIdentifier": device_identifier 
        }
        
        resp = requests.post(url, json=api_payload, timeout=5)
        if resp.status_code == 200:
             logger.info(f"Sync Successful: {payload.get('id')} data logged.")
        else:
             logger.error(f"Sync Failed: {resp.text}")
             
    except Exception as e:
        logger.error(f"Forwarding Failed: {e}")

# === MQTT HANDLERS ===
def on_mqtt_connect(client, userdata, flags, rc, properties=None): 
    if rc == 0:
        logger.info("MQTT Connected")
        client.subscribe("discovery/announce")
    else:
        logger.error(f"MQTT Connection Failed: {rc}")

def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        device_id = payload.get("id")
        
        if not device_id: return

        if device_id not in scan_detected_devices:
            logger.info(f"New Device: {device_id}")
            scan_detected_devices.add(device_id)
            
            # Forward Logic
            if config.get("provisioned"):
                 logger.info(f"Forwarding {device_id} to cloud...")
                 forward_device_data(payload)
            else:
                 logger.debug("Gateway not provisioned, skipping cloud sync.")
                 
    except json.JSONDecodeError:
        logger.error("Invalid JSON from MQTT")

# === PROVISIONING SERVER ===
async def run_ble_provisioning(loop):
    global server
    logger.info("Starting BLE Provisioning Service...")
    trigger_event = asyncio.Event()

    def read_request(characteristic: BlessGATTCharacteristic, **kwargs) -> bytes:
        logger.info(f"Read request on {characteristic.uuid}")
        status = "provisioned" if config.get("provisioned") else "waiting"
        return status.encode()

    def write_request(characteristic: BlessGATTCharacteristic, value: Any, **kwargs):
        logger.info(f"Write request on {characteristic.uuid}")
        try:
            data = json.loads(value.decode("utf-8"))
            
            # Update configuration from BLE payload
            if "name" in data:
                config["device_name"] = data["name"]
            
            if "inviteCode" in data:
                config["inviteCode"] = data["inviteCode"]
            
            if "api_url" in data:
                config["api_url"] = data["api_url"]
                
            wifi_success = True
            if "ssid" in data and "password" in data and data["ssid"]:
                logger.info("Received Credentials via BLE")
                wifi_success = connect_wifi(data["ssid"], data["password"])
            
            if wifi_success:
                if register_gateway():
                    config["provisioned"] = True
                    save_config()
                    logger.info("Provisioning Complete! Gateway is now active.")
                    # Notify logic could go here
                else:
                    logger.error("Cloud Registration Failed")
            else:
                 logger.error("Wifi Connection Failed")
            
        except Exception as e:
             logger.error(f"BLE Write Error: {e}")

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
    
    bd_addr = "Unknown"
    try:
        # Try reading from sysfs for hci0
        with open("/sys/class/bluetooth/hci0/address", "r") as f:
            bd_addr = f.read().strip()
    except:
        pass

    print(f"\n{'='*40}")
    print(f"📡 BLE STATUS: ONLINE")
    print(f"{'='*40}")
    print(f"🔹 Device Name  : {config['device_name']}")
    print(f"🔹 MAC Address  : {bd_addr}")
    print(f"🔹 Service UUID : {SERVICE_UUID}")
    print(f"🔹 Status       : Advertising...")
    print(f"{'='*40}\n")

    logger.info(f"BLE Provisioning Active. Waiting for iOS app...")
    
    # Run indefinitely
    await trigger_event.wait()
    await server.stop()

# === MAIN LOOP ===
import argparse

# ... (existing imports)

# === SETUP WIZARD ===
def setup_wizard():
    print(f"\n{'='*40}")
    print(f"🛠  INITIAL SETUP WIZARD")
    print(f"{'='*40}")
    print("Welcome! Let's configure your Raspberry Pi Gateway.\n")
    
    # Device Name
    default_name = config.get("device_name", "Gateway_Pi")
    name = input(f"🔹 Device Name [{default_name}]: ").strip()
    if name: config["device_name"] = name
    
    # API URL
    default_api = config.get("api_url", "http://169.254.13.52:3211/api")
    api_url = input(f"🔹 API URL [{default_api}]: ").strip()
    if api_url: config["api_url"] = api_url
    
    # Invite Code
    invite = input(f"🔹 Invite Code (optional): ").strip()
    if invite: config["inviteCode"] = invite
    
    print(f"\n✅ Configuration Saved!")
    print(f"{'='*40}\n")
    
    # Reset provisioned status if re-running setup
    config["provisioned"] = False
    save_config()
    time.sleep(1)

# === MAIN LOOP ===
async def main_loop():
    parser = argparse.ArgumentParser(description='Raspberry Pi BLE Gateway')
    parser.add_argument('--reset', action='store_true', help='Reset configuration and run setup wizard')
    args = parser.parse_args()
    
    print_banner()
    load_config()
    
    # Check for reset flag or missing essential config (like if it's fresh)
    if args.reset or not os.path.exists(CONFIG_FILE):
        if args.reset:
            print("⚠️  Resetting configuration...")
        setup_wizard()
    
    # Start MQTT (Optional if local broker is present)
    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    
    try:
        mqtt_client.connect(config["mqtt_broker"], config["mqtt_port"], 60)
        mqtt_client.loop_start()
    except Exception as e:
        logger.warning(f"MQTT Init Error (Is Mosquitto running?): {e}")

    # BLE Task
    ble_task = asyncio.create_task(run_ble_provisioning(asyncio.get_running_loop()))
    
    # Heartbeat loop
    while True:
        if config.get("provisioned"):
            send_heartbeat()
        await asyncio.sleep(60)

if __name__ == "__main__":
    if sys.platform == 'darwin':
        # Bless fix for macOS
        os.environ["PYTHONASYNCIODEBUG"] = "1"
        
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
