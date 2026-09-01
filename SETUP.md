# EIN — Complete Setup Guide (v3 — 30 cameras, EasyOCR, standalone backend)

## Prerequisites
1. Python 3.11+ (check "Add to PATH" on Windows)
2. Docker Desktop (optional — for MQTT/Redis/PostgreSQL)
3. VS Code

## Step 1: Install Dependencies
```
cd ein-v3
pip install -r requirements.txt
cd backend
pip install -r requirements.txt
cd ..
```

## Step 2: Start Backend (works standalone — no Docker needed!)
```
cd backend
uvicorn server:app --host 0.0.0.0 --port 8000
```
The backend auto-detects if Redis/PostgreSQL/MQTT are available. If not, it uses SQLite + in-memory. You should see:
```
[BACKEND] Using SQLite ✓
[BACKEND] === EIN Central Brain started ===
```

## Step 3: Start Dashboard
```
cd dashboard
python -m http.server 8080
```
Open http://localhost:8080 — you should see the Palantir-style dark command center with the live map of Gujarat and 30 camera markers.

## Step 4: Test with Mock Detections
```
python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/detect?camera_id=cam04&plate=GJ01AB1234&vehicle_type=car&vehicle_color=white')"
```
The dashboard should instantly show the detection on the map and fire a watchlist alert.

## Step 5: Start Real Edge Node
```
cd ein-v3
$env:CAMERA_ID="cam04"; $env:CAMERA_LABEL="Paldi Circle"; $env:HLS_URL="https://cctv.corp8.cloud/cam04/index.m3u8"; $env:MQTT_HOST="localhost"; python edge.py
```

## Step 6: Launch All 30 Cameras
```
./start.sh localhost true
```

## Optional: Full Docker Stack
```
docker-compose up --build
```

## Camera Feeds
| Protocol | URL |
|---|---|
| HLS | https://cctv.corp8.cloud/<id>/index.m3u8 |
| RTSP | rtsp://103.250.160.189:8554/stream/<id> |
30 cameras: cam01 through cam30.

## Troubleshooting
- "ModuleNotFoundError" → use `python -m pip install`
- EasyOCR first run downloads models (~100MB) — be patient
- Dashboard shows "SIM MODE" → backend not running at localhost:8000
- Dashboard shows "LIVE MODE" but no detections → edge node not running or can't read plates
- Backend "MQTT not available" → normal in standalone mode, use HTTP POST /api/detect for testing
