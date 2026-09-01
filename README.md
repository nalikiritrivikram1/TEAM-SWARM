# EIN — Edge Intelligence Network (v3)
> Don't move the video. Move the understanding.

30 real Sentinel cameras. EasyOCR (works on any Python). Standalone backend (no Docker required).

## Quick Start
```
pip install -r requirements.txt
cd backend && pip install -r requirements.txt && uvicorn server:app --port 8000 &
cd ../dashboard && python -m http.server 8080 &
CAMERA_ID=cam04 CAMERA_LABEL="Paldi Circle" HLS_URL="https://cctv.corp8.cloud/cam04/index.m3u8" MQTT_HOST=localhost python edge.py
```

Team: **The Swarm**
