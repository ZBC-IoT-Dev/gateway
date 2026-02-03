import paho.mqtt.client as mqtt
import json
import requests

# CONFIGURATION
# Replace with your computer's actual local IP address
NEXTJS_API_URL = "http://YOUR_COMPUTER_IP:3000/api/devices" 
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

discovered_devices = set()

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Gateway Online: Listening for Arduino devices...")
        client.subscribe("discovery/announce")
    else:
        print(f"Connection failed. Reason Code: {reason_code}")

def on_message(client, userdata, msg):
    try:
        # 1. Decode incoming JSON from Arduino
        payload = json.loads(msg.payload.decode())
        device_id = payload.get("id")
        device_type = payload.get("type")

        if not device_id:
            return

        if device_id not in discovered_devices:
            print(f"\nNEW DEVICE DISCOVERED: {device_id} ({device_type})")
            discovered_devices.add(device_id)

            # 2. Forward to Next.js API
            try:
                response = requests.post(NEXTJS_API_URL, json=payload, timeout=5)
                if response.status_code == 200:
                    print(f"Sync Successful: {device_id} registered in database.")
                else:
                    print(f"Server Error: Next.js returned status {response.status_code}")
            except Exception as e:
                print(f"Network Error: Could not reach Next.js server. Details: {e}")
        else:
            print(f"Heartbeat: Received ping from {device_id}")

    except json.JSONDecodeError:
        print("Data Error: Received malformed packet (invalid JSON).")

# Setup MQTT Client with Version 2 API
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()
except KeyboardInterrupt:
    print("\nShutting down Gateway controller...")
except Exception as e:
    print(f"Critical Startup Error: {e}")