# Raspberry Pi Gateway Setup

This guide will help you set up your Raspberry Pi as a BLE-provisioned IoT Gateway that forwards Arduino/MQTT data to your cloud.

## 1. Prerequisites (Hardware)

- Raspberry Pi (3B+, 4, 5, or Zero 2 W) with WiFi & Bluetooth.
- Arduino/ESP32 devices publishing JSON to a local MQTT broker.

## 2. Installation on Raspberry Pi

Run these commands on your Pi terminal:

```bash
# 1. Update system
sudo apt update && sudo apt upgrade -y

# 2. Install dependencies (NetworkManager, Bluetooth, Python tools)
sudo apt install -y python3-pip git network-manager bluez libglib2.0-dev mosquitto mosquitto-clients

# 3. Create a project folder
mkdir ~/gateway
cd ~/gateway

# 4. Copy the files
# (Copy 'requirements.txt' and 'main.py' from this project to your Pi)

# 5. Install Python libraries
pip3 install -r requirements.txt
```

## 3. Running the Gateway

```bash
sudo python3 main.py
```

_Note: `sudo` is often required for direct Bluetooth/WiFi hardware access._

## 4. Provisioning (BLE)

When the gateway starts for the first time (or if `gateway_config.json` doesn't exist), it enters **Provisioning Mode**.

1.  Use a BLE Scanner app (like nRF Connect) on your phone.
2.  Scan for device named **"GatewayProv"**.
3.  Connect to it.
4.  Write to Characteristic `...-abcdef1` (WIFI_Config) with this JSON:
    ```json
    {
      "ssid": "YOUR_WIFI_NAME",
      "password": "YOUR_WIFI_PASSWORD",
      "inviteCode": "YOUR_HOME_INVITE_CODE",
      "api_url": "http://YOUR_SERVER_IP:3211/api"
    }
    ```
5.  Read Characteristic `...-abcdef2` (Status) to see updates (e.g., "Provisioning Complete!").

## 5. Operations

Once provisioned:

- It connects to WiFi.
- registers itself with the cloud.
- Starts listening to local MQTT topic `discovery/announce`.
- Forwards any JSON packets from `discovery/announce` to the Cloud API (`/api/devices`).

To reset: Delete `gateway_config.json` and restart the script.
