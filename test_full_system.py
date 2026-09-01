#!/usr/bin/env python3
"""
EIN Full System Test v2 — Fixed backend endpoint
Tests: camera streaming + YOLOv8 + EasyOCR + backend + saves annotated frames

Usage:
  python test_full_system.py --cam cam01 --frames 10
  python test_full_system.py --cam cam01 --no-ocr --frames 5
  python test_full_system.py --cam cam04 --frames 20
"""

import requests
import cv2
import os
import re
import json
import time
import argparse
import logging
from datetime import datetime, timezone
from Crypto.Cipher import AES

PASSWORD = os.environ.get("SENTINEL_PASSWORD", "")
BASE = "https://cctv.corp8.cloud"
BACKEND_URL = "http://localhost:8000"

CAMERAS = {
    "cam01":  {"name": "Chimanbhai Bridge, Ahmedabad",  "lat": 23.0395, "lon": 72.5663},
    "cam02":  {"name": "Janpath Road, Ahmedabad",       "lat": 23.0310, "lon": 72.5190},
    "cam03":  {"name": "ONGC Office, Ahmedabad",         "lat": 23.0330, "lon": 72.5310},
    "cam04":  {"name": "Paldi Circle, Ahmedabad",        "lat": 23.0180, "lon": 72.5670},
    "cam05":  {"name": "Visat Teen Rasta, Ahmedabad",    "lat": 23.0740, "lon": 72.5280},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TEST] %(levelname)s: %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("TEST")


class SentinelStream:
    def __init__(self, cam_id):
        self.cam_id = cam_id
        self.stream_url = f"{BASE}/{cam_id}/index.m3u8"
        self.base_path = f"/{cam_id}"
        self.session = requests.Session()
        self.key_data = None
        self.iv = b'\x00' * 16

    def login(self):
        r = self.session.post(f"{BASE}/auth/login", data={"password": PASSWORD}, allow_redirects=True, timeout=15)
        return r.status_code == 200 and len(self.session.cookies) > 0

    def read_frame(self):
        resp = self.session.get(self.stream_url, timeout=15)
        if "#EXTM3U" not in resp.text[:50]:
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

        for seg in reversed(segments[-5:]):
            if seg.startswith("http"):
                seg_url = seg
            elif seg.startswith("/"):
                seg_url = BASE + seg
            else:
                seg_url = BASE + self.base_path + "/" + seg

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


def send_to_backend(cam_id, detection):
    """Send detection to backend using the correct endpoint."""
    if cam_id in CAMERAS:
        detection["cam_name"] = CAMERAS[cam_id]["name"]
        detection["lat"] = CAMERAS[cam_id]["lat"]
        detection["lon"] = CAMERAS[cam_id]["lon"]
        detection["camera_id"] = cam_id

    try:
        r = requests.post(f"{BACKEND_URL}/api/test/detection", json=detection, timeout=5)
        if r.status_code == 200:
            log.info(f"  -> Sent to backend: {detection.get('type','?')} {detection.get('vehicle_type','')} {detection.get('plate','')}")
            return True
        else:
            log.warning(f"  -> Backend returned {r.status_code}")
            return False
    except Exception as e:
        log.info(f"  -> Backend not running: {e}")
        return False


def run_live_detection(cam_id, yolo_model=None, ocr_reader=None, num_frames=10, show=False):
    """Run live detection on a camera for N frames."""
    log.info(f"=== Live Detection on {cam_id} ({num_frames} frames) ===")

    stream = SentinelStream(cam_id)
    if not stream.login():
        log.error(f"[{cam_id}] Login failed")
        return

    cam_name = CAMERAS.get(cam_id, {}).get("name", "Unknown")
    log.info(f"[{cam_id}] Streaming LIVE from {cam_name}")

    VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
    frame_count = 0
    total_detections = 0
    plates_read = 0

    while frame_count < num_frames:
        frame = stream.read_frame()
        if frame is None:
            log.warning(f"[{cam_id}] No frame, retrying...")
            time.sleep(2)
            continue

        frame_count += 1

        if yolo_model:
            results = yolo_model(frame, conf=0.5, verbose=False)
            frame_detections = 0

            for result in results:
                for box in result.boxes:
                    cls_id = int(box.cls[0])
                    conf = float(box.conf[0])
                    x1, y1, x2, y2 = map(int, box.xyxy[0])

                    if cls_id in VEHICLE_CLASSES:
                        vtype = VEHICLE_CLASSES[cls_id]
                        plate = None

                        # ANPR — read number plate
                        if ocr_reader:
                            try:
                                crop = frame[max(0, y1):min(frame.shape[0], y2),
                                             max(0, x1):min(frame.shape[1], x2)]
                                if crop.size > 100:
                                    ocr_results = ocr_reader.readtext(crop)
                                    texts = [t for _, t, _ in ocr_results if len(t) >= 4]
                                    if texts:
                                        plate = max(texts, key=len)
                                        plates_read += 1
                            except:
                                pass

                        detection = {
                            "type": "vehicle",
                            "cam_id": cam_id,
                            "camera_id": cam_id,
                            "vehicle_type": vtype,
                            "confidence": round(conf, 3),
                            "bbox": [x1, y1, x2, y2],
                            "plate": plate,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                        send_to_backend(cam_id, detection)
                        total_detections += 1
                        frame_detections += 1

                        # Draw green box around vehicle
                        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                        label = f"{vtype} {conf:.2f}"
                        if plate:
                            label += f" | {plate}"
                        cv2.putText(frame, label, (x1, y1 - 10),
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

                    elif cls_id == 0:  # person
                        detection = {
                            "type": "person",
                            "cam_id": cam_id,
                            "camera_id": cam_id,
                            "confidence": round(conf, 3),
                            "bbox": [x1, y1, x2, y2],
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                        send_to_backend(cam_id, detection)
                        total_detections += 1
                        frame_detections += 1
                        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)

            # Save annotated frame
            os.makedirs("detections", exist_ok=True)
            cv2.imwrite(f"detections/{cam_id}_frame_{frame_count:03d}.jpg", frame)
            log.info(f"[{cam_id}] Frame {frame_count}/{num_frames} - {frame_detections} detections (total: {total_detections})")
        else:
            log.info(f"[{cam_id}] Frame {frame_count}/{num_frames} - {frame.shape[1]}x{frame.shape[0]}")

        if show:
            cv2.imshow(f"{cam_id}", frame)
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

        time.sleep(1)

    if show:
        cv2.destroyAllWindows()

    log.info(f"[{cam_id}] Done! {frame_count} frames, {total_detections} detections, {plates_read} plates read")
    log.info(f"[{cam_id}] Annotated frames saved to detections/ folder")


def main():
    parser = argparse.ArgumentParser(description="EIN Full System Test v2")
    parser.add_argument("--cam", type=str, default="cam01", help="Camera ID")
    parser.add_argument("--no-ocr", action="store_true", help="Skip OCR")
    parser.add_argument("--no-yolo", action="store_true", help="Skip YOLOv8")
    parser.add_argument("--frames", type=int, default=10, help="Number of frames")
    parser.add_argument("--show", action="store_true", help="Show video window")
    args = parser.parse_args()

    log.info("====================================")
    log.info("  EIN FULL SYSTEM TEST v2")
    log.info("====================================")
    log.info("")

    # 1. Test backend
    log.info("--- Testing Backend ---")
    backend_ok = False
    try:
        r = requests.get(f"{BACKEND_URL}/", timeout=5)
        if r.status_code == 200:
            log.info(f"Backend OK at {BACKEND_URL}")
            backend_ok = True
        else:
            log.warning(f"Backend returned {r.status_code}")
    except:
        log.warning(f"Backend NOT running at {BACKEND_URL}")
        log.info("Start it: cd backend && python server.py")
    log.info("")

    # 2. Test camera
    log.info(f"--- Testing Camera {args.cam} ---")
    stream = SentinelStream(args.cam)
    if not stream.login():
        log.error(f"[{args.cam}] Login failed")
        return
    frame = stream.read_frame()
    if frame is not None:
        log.info(f"[{args.cam}] Camera OK - Resolution: {frame.shape[1]}x{frame.shape[0]}")
    else:
        log.error(f"[{args.cam}] No frame")
        return
    log.info("")

    # 3. Test YOLOv8
    yolo_model = None
    if not args.no_yolo:
        log.info("--- Testing YOLOv8 ---")
        try:
            from ultralytics import YOLO
            yolo_model = YOLO("yolov8n.pt")
            log.info("YOLOv8 loaded successfully")
        except ImportError:
            log.warning("YOLOv8 not installed. Run: pip install ultralytics")
        except Exception as e:
            log.warning(f"YOLOv8 error: {e}")
        log.info("")

    # 4. Test OCR
    ocr_reader = None
    if not args.no_ocr:
        log.info("--- Testing EasyOCR ---")
        try:
            import easyocr
            ocr_reader = easyocr.Reader(["en"], gpu=False)
            log.info("EasyOCR loaded successfully")
        except ImportError:
            log.warning("EasyOCR not installed. Run: pip install easyocr")
        except Exception as e:
            log.warning(f"EasyOCR error: {e}")
        log.info("")

    # 5. Run live detection
    run_live_detection(args.cam, yolo_model, ocr_reader, args.frames, args.show)
    log.info("")

    # Summary
    log.info("====================================")
    log.info("  TEST SUMMARY")
    log.info("====================================")
    log.info(f"  Backend:   {'OK' if backend_ok else 'NOT RUNNING'}")
    log.info(f"  Camera:    OK ({args.cam} - 1920x1080)")
    log.info(f"  YOLOv8:    {'OK' if yolo_model else 'NOT AVAILABLE'}")
    log.info(f"  EasyOCR:   {'OK' if ocr_reader else 'NOT AVAILABLE'}")
    log.info("====================================")
    if backend_ok:
        log.info(f"View API docs: {BACKEND_URL}/docs")
    log.info(f"Annotated frames: detections/ folder")
    log.info("")


if __name__ == "__main__":
    main()
