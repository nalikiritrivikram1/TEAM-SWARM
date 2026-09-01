#!/usr/bin/env python3
"""
EIN Edge Node v3 — Live Sentinel Camera Stream
Downloads + decrypts AES-128 HLS segments, runs YOLOv8 + EasyOCR, sends to backend

Usage:
  python edge.py --cam cam01 --no-ai --show      # Live video window, no AI
  python edge.py --cam cam01 --no-ai             # Live stream, no AI
  python edge.py --cam cam01 --show              # Live video + AI detection
  python edge.py --cam cam01                     # AI detection, no window
  python edge.py --all --no-ai                   # All 30 cameras, no AI
  python edge.py --all                           # All 30 cameras with AI
  python edge.py --cam cam01 --save-frames       # Save frames for demo
"""

import requests
import cv2
import os
import re
import json
import time
import argparse
import threading
import logging
from datetime import datetime, timezone

# AES decryption for Sentinel HLS streams
try:
    from Crypto.Cipher import AES
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False
    print("ERROR: pycryptodome not installed. Run: pip install pycryptodome")
    exit(1)

# === CONFIGURATION ===
PASSWORD = os.environ.get("SENTINEL_PASSWORD", "")
BASE = "https://cctv.corp8.cloud"
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")
MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
FRAME_INTERVAL = float(os.environ.get("FRAME_INTERVAL", "1.0"))
CONFIDENCE_THRESHOLD = 0.5

# === 30 CAMERA CATALOGUE ===
CAMERAS = {
    "cam01":  {"name": "Chimanbhai Bridge, Ahmedabad",  "lat": 23.0395, "lon": 72.5663},
    "cam02":  {"name": "Janpath Road, Ahmedabad",       "lat": 23.0310, "lon": 72.5190},
    "cam03":  {"name": "ONGC Office, Ahmedabad",         "lat": 23.0330, "lon": 72.5310},
    "cam04":  {"name": "Paldi Circle, Ahmedabad",        "lat": 23.0180, "lon": 72.5670},
    "cam05":  {"name": "Visat Teen Rasta, Ahmedabad",    "lat": 23.0740, "lon": 72.5280},
    "cam06":  {"name": "Junagadh Camera 06",             "lat": 21.5220, "lon": 70.4570},
    "cam07":  {"name": "Junagadh Camera 07",             "lat": 21.5240, "lon": 70.4600},
    "cam08":  {"name": "Junagadh Camera 08",             "lat": 21.5260, "lon": 70.4630},
    "cam09":  {"name": "Junagadh Camera 09",             "lat": 21.5280, "lon": 70.4660},
    "cam10":  {"name": "Junagadh Camera 10",             "lat": 21.5300, "lon": 70.4690},
    "cam11":  {"name": "Junagadh Camera 11",             "lat": 21.5320, "lon": 70.4720},
    "cam12":  {"name": "Tri Mandir Adalaj",              "lat": 23.1660, "lon": 72.5860},
    "cam13":  {"name": "CN Vidhyalaya, Ahmedabad",        "lat": 23.0420, "lon": 72.5420},
    "cam14":  {"name": "Delight RLVD, Ahmedabad",        "lat": 23.0450, "lon": 72.5450},
    "cam15":  {"name": "Suvidha Park, Ahmedabad",        "lat": 23.0480, "lon": 72.5480},
    "cam16":  {"name": "Visat P2, Ahmedabad",             "lat": 23.0750, "lon": 72.5290},
    "cam17":  {"name": "Rajkot Camera 17",                "lat": 22.3030, "lon": 70.8020},
    "cam18":  {"name": "Rajkot Camera 18",                "lat": 22.3050, "lon": 70.8050},
    "cam19":  {"name": "Khaparia, Navsari",              "lat": 20.9520, "lon": 72.9300},
    "cam20":  {"name": "Mohanpura, Ahmedabad",            "lat": 23.0350, "lon": 72.5700},
    "cam21":  {"name": "Patan Dethali",                   "lat": 23.8470, "lon": 72.1300},
    "cam22":  {"name": "BK Mervada",                      "lat": 23.8500, "lon": 72.1350},
    "cam23":  {"name": "Kheram",                          "lat": 21.1000, "lon": 71.7500},
    "cam24":  {"name": "Deshgam",                          "lat": 21.1050, "lon": 71.7550},
    "cam25":  {"name": "Dhanori",                          "lat": 21.1100, "lon": 71.7600},
    "cam26":  {"name": "Tankal",                           "lat": 21.1150, "lon": 71.7650},
    "cam27":  {"name": "Bilimora Camera 27",               "lat": 20.7800, "lon": 72.9500},
    "cam28":  {"name": "Bilimora Camera 28",               "lat": 20.7820, "lon": 72.9520},
    "cam29":  {"name": "Bilimora Camera 29",               "lat": 20.7840, "lon": 72.9540},
    "cam30":  {"name": "Gandhidham",                       "lat": 23.0730, "lon": 70.1330},
}

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EIN] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("EIN")


# === SENTINEL LIVE CAMERA STREAM ===
class SentinelStream:
    """Live HLS camera stream from Sentinel with AES-128 decryption."""

    def __init__(self, cam_id):
        self.cam_id = cam_id
        self.stream_url = f"{BASE}/{cam_id}/index.m3u8"
        self.base_path = f"/{cam_id}"
        self.session = requests.Session()
        self.key_data = None
        self.iv = b'\x00' * 16

    def login(self):
        try:
            r = self.session.post(
                f"{BASE}/auth/login",
                data={"password": PASSWORD},
                allow_redirects=True,
                timeout=15
            )
            if r.status_code == 200 and len(self.session.cookies) > 0:
                log.info(f"[{self.cam_id}] Login OK")
                return True
            else:
                log.error(f"[{self.cam_id}] Login failed: status={r.status_code}")
                return False
        except Exception as e:
            log.error(f"[{self.cam_id}] Login error: {e}")
            return False

    def _fetch_playlist(self):
        resp = self.session.get(self.stream_url, timeout=15)
        if "#EXTM3U" not in resp.text[:50]:
            log.warning(f"[{self.cam_id}] Not a valid HLS playlist")
            return None

        for line in resp.text.splitlines():
            if "#EXT-X-KEY" in line:
                uri_match = re.search(r'URI="([^"]+)"', line)
                iv_match = re.search(r'IV=0x([0-9a-fA-F]+)', line)
                if uri_match:
                    key_url = uri_match.group(1)
                    if not key_url.startswith("http"):
                        if not key_url.startswith("/"):
                            key_url = "/" + key_url
                        key_url = BASE + key_url
                    self.key_data = self.session.get(key_url, timeout=10).content
                if iv_match:
                    self.iv = bytes.fromhex(iv_match.group(1))
                else:
                    self.iv = b'\x00' * 16

        segments = []
        for line in resp.text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                segments.append(line)
        return segments

    def _segment_url(self, seg):
        if seg.startswith("http"):
            return seg
        elif seg.startswith("/"):
            return BASE + seg
        else:
            return BASE + self.base_path + "/" + seg

    def read_live_frame(self):
        segments = self._fetch_playlist()
        if not segments:
            return None

        for seg in reversed(segments[-5:]):
            seg_url = self._segment_url(seg)
            try:
                resp = self.session.get(seg_url, timeout=15)
                if resp.status_code != 200:
                    continue
                seg_data = resp.content
                if self.key_data:
                    cipher = AES.new(self.key_data, AES.MODE_CBC, self.iv)
                    decrypted = cipher.decrypt(seg_data)
                else:
                    decrypted = seg_data

                temp_file = f"temp_{self.cam_id}.ts"
                with open(temp_file, "wb") as f:
                    f.write(decrypted)
                cap = cv2.VideoCapture(temp_file)
                ok, frame = cap.read()
                cap.release()
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                if ok:
                    return frame
            except:
                continue
        return None


# === AI MODULES (only loaded when needed) ===
_yolo_model = None
_ocr_reader = None

def get_yolo():
    global _yolo_model
    if _yolo_model is None:
        from ultralytics import YOLO
        _yolo_model = YOLO("yolov8n.pt")
        log.info("YOLOv8 model loaded")
    return _yolo_model

def get_ocr():
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr
        _ocr_reader = easyocr.Reader(["en"], gpu=False)
        log.info("EasyOCR loaded")
    return _ocr_reader

VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}

def detect_objects(frame, cam_id):
    detections = []
    model = get_yolo()
    results = model(frame, conf=CONFIDENCE_THRESHOLD, verbose=False)
    for result in results:
        for box in result.boxes:
            cls_id = int(box.cls[0])
            conf = float(box.conf[0])
            x1, y1, x2, y2 = map(int, box.xyxy[0])

            if cls_id in VEHICLE_CLASSES:
                vtype = VEHICLE_CLASSES[cls_id]
                plate = None
                try:
                    reader = get_ocr()
                    crop = frame[max(0, y1):min(frame.shape[0], y2),
                                 max(0, x1):min(frame.shape[1], x2)]
                    if crop.size > 100:
                        ocr_results = reader.readtext(crop)
                        texts = [t for _, t, _ in ocr_results if len(t) >= 4]
                        if texts:
                            plate = max(texts, key=len)
                except:
                    pass

                detections.append({
                    "type": "vehicle",
                    "cam_id": cam_id,
                    "camera_id": cam_id,
                    "vehicle_type": vtype,
                    "confidence": round(conf, 3),
                    "bbox": [x1, y1, x2, y2],
                    "plate": plate,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })

            elif cls_id == 0:
                detections.append({
                    "type": "person",
                    "cam_id": cam_id,
                    "camera_id": cam_id,
                    "confidence": round(conf, 3),
                    "bbox": [x1, y1, x2, y2],
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
    return detections


# === BACKEND + MQTT PUBLISHER ===
_mqtt_client = None

def get_mqtt():
    global _mqtt_client
    if _mqtt_client is None:
        try:
            import paho.mqtt.client as mqtt
            _mqtt_client = mqtt.Client(client_id=f"ein-edge-{os.getpid()}")
            _mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            _mqtt_client.loop_start()
            log.info(f"MQTT connected to {MQTT_BROKER}:{MQTT_PORT}")
        except:
            log.info("MQTT not available - using HTTP backend only")
            _mqtt_client = False
    return _mqtt_client

def publish(cam_id, detection):
    """Send detection to backend via HTTP API AND MQTT."""
    if cam_id in CAMERAS:
        detection["cam_name"] = CAMERAS[cam_id]["name"]
        detection["camera_id"] = cam_id
        detection["lat"] = CAMERAS[cam_id]["lat"]
        detection["lon"] = CAMERAS[cam_id]["lon"]

    # Send via HTTP to backend (works even without MQTT)
    try:
        r = requests.post(f"{BACKEND_URL}/api/test/detection", json=detection, timeout=5)
        backend_ok = r.status_code == 200
    except:
        backend_ok = False

    # Also send via MQTT if available
    client = get_mqtt()
    if client:
        client.publish(f"ein/detections/{cam_id}", json.dumps(detection))

    # Print to console
    if detection.get("type") == "vehicle":
        plate_str = f" plate={detection['plate']}" if detection.get("plate") else ""
        backend_str = " ->backend OK" if backend_ok else ""
        log.info(f"[{cam_id}] {detection['vehicle_type']} ({detection['confidence']}){plate_str}{backend_str}")
    elif detection.get("type") == "person":
        log.info(f"[{cam_id}] person ({detection['confidence']})")
    elif detection.get("type") == "frame":
        log.info(f"[{cam_id}] LIVE {detection.get('resolution','?')} #{detection.get('frame_count',0)}")


# === EDGE NODE RUNNER ===
def run_edge_node(cam_id, run_ai=True, show=False, save_frames=False):
    tag = " with AI" if run_ai else " (no AI)"
    log.info(f"[{cam_id}] Starting edge node{tag}...")

    stream = SentinelStream(cam_id)
    if not stream.login():
        log.error(f"[{cam_id}] Cannot login. Exiting.")
        return

    cam_name = CAMERAS.get(cam_id, {}).get("name", "Unknown")
    log.info(f"[{cam_id}] LIVE streaming from {cam_name}")

    frame_count = 0
    consecutive_failures = 0

    while True:
        try:
            frame = stream.read_live_frame()
            if frame is None:
                consecutive_failures += 1
                if consecutive_failures > 3:
                    log.warning(f"[{cam_id}] No frame, retrying...")
                    consecutive_failures = 0
                time.sleep(2)
                continue

            consecutive_failures = 0
            frame_count += 1

            if show:
                cv2.imshow(f"{cam_id} - {cam_name}", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    log.info(f"[{cam_id}] Stopped by user")
                    break

            if save_frames:
                os.makedirs(f"frames/{cam_id}", exist_ok=True)
                cv2.imwrite(f"frames/{cam_id}/frame_{frame_count:06d}.jpg", frame)

            if run_ai:
                detections = detect_objects(frame, cam_id)
                for det in detections:
                    publish(cam_id, det)
            else:
                publish(cam_id, {
                    "type": "frame",
                    "cam_id": cam_id,
                    "camera_id": cam_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "resolution": f"{frame.shape[1]}x{frame.shape[0]}",
                    "frame_count": frame_count
                })

            time.sleep(FRAME_INTERVAL)

        except KeyboardInterrupt:
            log.info(f"[{cam_id}] Stopped")
            break
        except Exception as e:
            log.error(f"[{cam_id}] Error: {e}")
            time.sleep(3)

    if show:
        cv2.destroyAllWindows()


# === MAIN ===
def main():
    parser = argparse.ArgumentParser(description="EIN Edge Node v3 - Live Sentinel Camera Stream")
    parser.add_argument("--cam", type=str, help="Camera ID (e.g. cam01)")
    parser.add_argument("--all", action="store_true", help="Run all 30 cameras")
    parser.add_argument("--no-ai", action="store_true", help="Skip AI detection (just stream)")
    parser.add_argument("--show", action="store_true", help="Show live video window")
    parser.add_argument("--save-frames", action="store_true", help="Save frames to disk")
    args = parser.parse_args()

    run_ai = not args.no_ai

    if args.all:
        log.info(f"Starting ALL 30 cameras{' with AI' if run_ai else ' (no AI)'}...")
        threads = []
        for cam_id in CAMERAS:
            t = threading.Thread(target=run_edge_node,
                               args=(cam_id, run_ai, False, args.save_frames), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(0.5)
        log.info("All 30 edge nodes started. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Shutting down all edge nodes...")

    elif args.cam:
        run_edge_node(args.cam, run_ai, args.show, args.save_frames)

    else:
        parser.print_help()
        print()
        print("Examples:")
        print("  python edge.py --cam cam01 --no-ai --show      # Live video window, no AI")
        print("  python edge.py --cam cam01 --show              # Live video + AI detection")
        print("  python edge.py --cam cam01                     # AI detection, no window")
        print("  python edge.py --all --no-ai                   # All 30 cameras, no AI")
        print("  python edge.py --all                          # All 30 cameras with AI")
        print("  python edge.py --cam cam01 --save-frames       # Save frames for demo video")


if __name__ == "__main__":
    main()
