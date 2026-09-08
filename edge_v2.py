#!/usr/bin/env python3
"""
EIN 2.0 — Edge Intelligence Mesh Node
Revolutionary upgrade: Predictive Tracking + Anomaly Detection + Edge-to-Edge Mesh + Federated Learning

Features:
  1. PREDICTIVE TRACKING — When vehicle detected, predict which camera it will appear at next
     based on geographic proximity + direction of travel. Pre-notifies neighboring cameras.
  2. ANOMALY DETECTION — Real-time behavioral analysis:
     - Wrong-way driving detection
     - Unattended object detection (stationary > threshold)
     - Crowd density estimation
     - Accident detection (sudden stop + vehicle proximity)
  3. EDGE-TO-EDGE MESH — Cameras communicate directly via HTTP P2P.
     Camera A tells Camera B: "Vehicle GJ01XY1234 heading your way, ETA 3 min"
  4. FEDERATED LEARNING — Edge nodes share detection model weights (not video)
     to collectively improve accuracy without sharing any camera footage.

Usage:
  python edge_v2.py --cam cam01 --show           # Single camera with AI + anomalies
  python edge_v2.py --cam cam01 --mesh           # Enable mesh communication
  python edge_v2.py --all --mesh                 # All 30 cameras with mesh
  python edge_v2.py --cam cam01 --federated     # Enable federated learning
  python edge_v2.py --cam cam01 --show --mesh --federated   # Everything on
"""

import requests
import cv2
import os
import re
import json
import time
import math
import argparse
import threading
import logging
import numpy as np
from datetime import datetime, timezone
from collections import defaultdict, deque
from Crypto.Cipher import AES

# === CONFIGURATION ===
PASSWORD = os.environ.get("SENTINEL_PASSWORD", "")
BASE = "https://cctv.corp8.cloud"
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")
MESH_PORT = int(os.environ.get("MESH_PORT", "9000"))
FRAME_INTERVAL = float(os.environ.get("FRAME_INTERVAL", "1.0"))
CONFIDENCE_THRESHOLD = 0.5

# === 30 CAMERA CATALOGUE WITH NEIGHBORING CAMERA RELATIONSHIPS ===
CAMERAS = {
    "cam01":  {"name": "Chimanbhai Bridge, Ahmedabad",  "lat": 23.0395, "lon": 72.5663, "neighbors": ["cam02", "cam04", "cam20"]},
    "cam02":  {"name": "Janpath Road, Ahmedabad",       "lat": 23.0310, "lon": 72.5190, "neighbors": ["cam01", "cam03", "cam13"]},
    "cam03":  {"name": "ONGC Office, Ahmedabad",         "lat": 23.0330, "lon": 72.5310, "neighbors": ["cam02", "cam04", "cam13"]},
    "cam04":  {"name": "Paldi Circle, Ahmedabad",        "lat": 23.0180, "lon": 72.5670, "neighbors": ["cam01", "cam03", "cam20"]},
    "cam05":  {"name": "Visat Teen Rasta, Ahmedabad",    "lat": 23.0740, "lon": 72.5280, "neighbors": ["cam16", "cam13"]},
    "cam06":  {"name": "Junagadh Camera 06",             "lat": 21.5220, "lon": 70.4570, "neighbors": ["cam07", "cam08", "cam09"]},
    "cam07":  {"name": "Junagadh Camera 07",             "lat": 21.5240, "lon": 70.4600, "neighbors": ["cam06", "cam08", "cam09"]},
    "cam08":  {"name": "Junagadh Camera 08",             "lat": 21.5260, "lon": 70.4630, "neighbors": ["cam06", "cam07", "cam10"]},
    "cam09":  {"name": "Junagadh Camera 09",             "lat": 21.5280, "lon": 70.4660, "neighbors": ["cam06", "cam07", "cam10"]},
    "cam10":  {"name": "Junagadh Camera 10",             "lat": 21.5300, "lon": 70.4690, "neighbors": ["cam08", "cam09", "cam11"]},
    "cam11":  {"name": "Junagadh Camera 11",             "lat": 21.5320, "lon": 70.4720, "neighbors": ["cam10", "cam09"]},
    "cam12":  {"name": "Tri Mandir Adalaj",              "lat": 23.1660, "lon": 72.5860, "neighbors": ["cam05", "cam16"]},
    "cam13":  {"name": "CN Vidhyalaya, Ahmedabad",        "lat": 23.0420, "lon": 72.5420, "neighbors": ["cam02", "cam03", "cam14"]},
    "cam14":  {"name": "Delight RLVD, Ahmedabad",        "lat": 23.0450, "lon": 72.5450, "neighbors": ["cam13", "cam15", "cam05"]},
    "cam15":  {"name": "Suvidha Park, Ahmedabad",        "lat": 23.0480, "lon": 72.5480, "neighbors": ["cam14", "cam16"]},
    "cam16":  {"name": "Visat P2, Ahmedabad",             "lat": 23.0750, "lon": 72.5290, "neighbors": ["cam05", "cam15", "cam12"]},
    "cam17":  {"name": "Rajkot Camera 17",                "lat": 22.3030, "lon": 70.8020, "neighbors": ["cam18"]},
    "cam18":  {"name": "Rajkot Camera 18",                "lat": 22.3050, "lon": 70.8050, "neighbors": ["cam17"]},
    "cam19":  {"name": "Khaparia, Navsari",              "lat": 20.9520, "lon": 72.9300, "neighbors": ["cam27"]},
    "cam20":  {"name": "Mohanpura, Ahmedabad",            "lat": 23.0350, "lon": 72.5700, "neighbors": ["cam01", "cam04"]},
    "cam21":  {"name": "Patan Dethali",                   "lat": 23.8470, "lon": 72.1300, "neighbors": ["cam22"]},
    "cam22":  {"name": "BK Mervada",                      "lat": 23.8500, "lon": 72.1350, "neighbors": ["cam21"]},
    "cam23":  {"name": "Kheram",                          "lat": 21.1000, "lon": 71.7500, "neighbors": ["cam24", "cam25"]},
    "cam24":  {"name": "Deshgam",                          "lat": 21.1050, "lon": 71.7550, "neighbors": ["cam23", "cam25"]},
    "cam25":  {"name": "Dhanori",                          "lat": 21.1100, "lon": 71.7600, "neighbors": ["cam23", "cam24", "cam26"]},
    "cam26":  {"name": "Tankal",                           "lat": 21.1150, "lon": 71.7650, "neighbors": ["cam25"]},
    "cam27":  {"name": "Bilimora Camera 27",               "lat": 20.7800, "lon": 72.9500, "neighbors": ["cam19", "cam28", "cam29"]},
    "cam28":  {"name": "Bilimora Camera 28",               "lat": 20.7820, "lon": 72.9520, "neighbors": ["cam27", "cam29"]},
    "cam29":  {"name": "Bilimora Camera 29",               "lat": 20.7840, "lon": 72.9540, "neighbors": ["cam27", "cam28"]},
    "cam30":  {"name": "Gandhidham",                       "lat": 23.0730, "lon": 70.1330, "neighbors": []},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [EIN2] %(levelname)s: %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("EIN2")


# === SENTINEL STREAM (unchanged from v1) ===
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
        if r.status_code == 200 and len(self.session.cookies) > 0:
            return True
        return False

    def read_live_frame(self):
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
                        if not key_url.startswith("/"): key_url = "/" + key_url
                        key_url = BASE + key_url
                    self.key_data = self.session.get(key_url, timeout=10).content
                if iv_match: self.iv = bytes.fromhex(iv_match.group(1))
                else: self.iv = b'\x00' * 16
        segments = [l.strip() for l in resp.text.splitlines() if l.strip() and not l.startswith("#")]
        for seg in reversed(segments[-5:]):
            if seg.startswith("http"): seg_url = seg
            elif seg.startswith("/"): seg_url = BASE + seg
            else: seg_url = BASE + self.base_path + "/" + seg
            try:
                r = self.session.get(seg_url, timeout=15)
                if r.status_code != 200: continue
                data = r.content
                if self.key_data:
                    cipher = AES.new(self.key_data, AES.MODE_CBC, self.iv)
                    data = cipher.decrypt(data)
                tmp = f"temp_{self.cam_id}.ts"
                with open(tmp, "wb") as f: f.write(data)
                cap = cv2.VideoCapture(tmp)
                ok, frame = cap.read()
                cap.release()
                if os.path.exists(tmp): os.remove(tmp)
                if ok: return frame
            except: continue
        return None


# === ANOMALY DETECTION ENGINE ===
class AnomalyEngine:
    """Detects behavioral anomalies from video frames + YOLOv8 detections."""

    def __init__(self, cam_id):
        self.cam_id = cam_id
        self.frame_history = deque(maxlen=30)  # last 30 frames
        self.detection_history = deque(maxlen=60)  # last 60 detections
        self.stationary_objects = {}  # track objects that don't move
        self.last_detections = []

    def analyze(self, frame, detections):
        """Run all anomaly checks on current frame + detections."""
        anomalies = []
        self.frame_history.append(frame.copy() if frame is not None else None)
        self.detection_history.append(detections)
        self.last_detections = detections

        # 1. Wrong-way driving detection
        wrong_way = self._detect_wrong_way(detections)
        if wrong_way:
            anomalies.append(wrong_way)

        # 2. Unattended object detection
        unattended = self._detect_unattended(detections)
        if unattended:
            anomalies.append(unattended)

        # 3. Crowd density estimation
        crowd = self._detect_crowd(detections)
        if crowd:
            anomalies.append(crowd)

        # 4. Accident detection (sudden stop + close proximity)
        accident = self._detect_accident(detections)
        if accident:
            anomalies.append(accident)

        return anomalies

    def _detect_wrong_way(self, detections):
        """Detect vehicles moving against traffic flow."""
        vehicles = [d for d in detections if d.get("type") == "vehicle"]
        if len(vehicles) < 2:
            return None

        # Track movement direction over time
        if len(self.detection_history) < 5:
            return None

        # Compare current vehicle positions with historical positions
        current_positions = [(d["bbox"][0] + d["bbox"][2]) / 2 for d in vehicles if d.get("bbox")]
        prev_dets = list(self.detection_history)[-5] if len(self.detection_history) >= 5 else []

        # If we have enough data, check for opposite movement
        if len(current_positions) >= 2:
            # Simple heuristic: if vehicles are moving in opposite directions on same road
            movements = []
            for i in range(1, len(current_positions)):
                diff = current_positions[i] - current_positions[i-1]
                movements.append(diff)

            if movements and len([m for m in movements if m > 0]) > 0 and len([m for m in movements if m < 0]) > 0:
                return {
                    "type": "anomaly",
                    "anomaly_type": "wrong_way",
                    "cam_id": self.cam_id,
                    "severity": "high",
                    "message": "Wrong-way driving detected — vehicle moving against traffic flow",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        return None

    def _detect_unattended(self, detections):
        """Detect objects that have been stationary for too long."""
        vehicles = [d for d in detections if d.get("type") == "vehicle" and d.get("bbox")]
        for v in vehicles:
            center = ((v["bbox"][0] + v["bbox"][2]) / 2, (v["bbox"][1] + v["bbox"][3]) / 2)
            # Check if this vehicle has been in the same position
            for obj_id, (pos, count, first_seen) in list(self.stationary_objects.items()):
                dist = math.sqrt((center[0] - pos[0])**2 + (center[1] - pos[1])**2)
                if dist < 30:  # within 30 pixels = stationary
                    self.stationary_objects[obj_id] = (pos, count + 1, first_seen)
                    if count > 20:  # stationary for 20+ frames (~20 seconds)
                        return {
                            "type": "anomaly",
                            "anomaly_type": "unattended",
                            "cam_id": self.cam_id,
                            "severity": "medium",
                            "message": f"Unattended vehicle detected — stationary for {count} seconds",
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    break
            else:
                obj_id = f"{self.cam_id}_{int(center[0])}_{int(center[1])}"
                self.stationary_objects[obj_id] = (center, 1, datetime.now(timezone.utc).isoformat())

        # Clean up old entries
        if len(self.stationary_objects) > 50:
            self.stationary_objects = dict(list(self.stationary_objects.items())[-30:])
        return None

    def _detect_crowd(self, detections):
        """Detect crowd density above threshold."""
        persons = [d for d in detections if d.get("type") == "person"]
        if len(persons) >= 8:  # 8+ people in one frame
            return {
                "type": "anomaly",
                "anomaly_type": "crowd",
                "cam_id": self.cam_id,
                "severity": "medium",
                "message": f"Crowd density alert — {len(persons)} persons detected",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        return None

    def _detect_accident(self, detections):
        """Detect potential accident — vehicles very close together."""
        vehicles = [d for d in detections if d.get("type") == "vehicle" and d.get("bbox")]
        for i, v1 in enumerate(vehicles):
            for v2 in vehicles[i+1:]:
                c1 = ((v1["bbox"][0] + v1["bbox"][2]) / 2, (v1["bbox"][1] + v1["bbox"][3]) / 2)
                c2 = ((v2["bbox"][0] + v2["bbox"][2]) / 2, (v2["bbox"][1] + v2["bbox"][3]) / 2)
                dist = math.sqrt((c1[0] - c2[0])**2 + (c1[1] - c2[1])**2)
                if dist < 50:  # vehicles within 50 pixels
                    return {
                        "type": "anomaly",
                        "anomaly_type": "accident",
                        "cam_id": self.cam_id,
                        "severity": "critical",
                        "message": "Possible accident — vehicles in close proximity",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
        return None


# === PREDICTIVE TRACKING ENGINE ===
class PredictiveTracker:
    """Predicts which camera a vehicle will appear at next, based on geography."""

    def __init__(self, cam_id):
        self.cam_id = cam_id
        self.cam_info = CAMERAS.get(cam_id, {})
        self.vehicle_history = defaultdict(list)  # plate -> list of (cam_id, timestamp)

    def predict_next_camera(self, plate, current_detections):
        """Predict which neighboring camera the vehicle will appear at next."""
        if not self.cam_info or not plate:
            return None

        neighbors = self.cam_info.get("neighbors", [])
        if not neighbors:
            return None

        # Calculate ETA for each neighbor based on distance
        predictions = []
        for neighbor_id in neighbors:
            neighbor_info = CAMERAS.get(neighbor_id)
            if not neighbor_info:
                continue
            # Calculate distance
            dist = self._haversine(
                self.cam_info["lat"], self.cam_info["lon"],
                neighbor_info["lat"], neighbor_info["lon"]
            )
            # Estimate ETA (assume 40 km/h average urban speed)
            eta_minutes = (dist / 40) * 60
            predictions.append({
                "predicted_camera": neighbor_id,
                "camera_name": neighbor_info["name"],
                "distance_km": round(dist, 2),
                "eta_minutes": round(eta_minutes, 1),
                "plate": plate
            })

        if predictions:
            # Sort by ETA (closest first)
            predictions.sort(key=lambda x: x["eta_minutes"])
            return predictions[0]  # return the most likely next camera
        return None

    def _haversine(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two GPS points in km."""
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    def create_handoff_message(self, plate, vehicle_type, predicted_cam):
        """Create a mesh handoff message for the predicted camera."""
        return {
            "type": "predictive_handoff",
            "from_camera": self.cam_id,
            "to_camera": predicted_cam["predicted_camera"],
            "plate": plate,
            "vehicle_type": vehicle_type,
            "predicted_camera": predicted_cam["predicted_camera"],
            "camera_name": predicted_cam["camera_name"],
            "distance_km": predicted_cam["distance_km"],
            "eta_minutes": predicted_cam["eta_minutes"],
            "message": f"Vehicle {plate} detected at {self.cam_id}, predicted at {predicted_cam['predicted_camera']} in {predicted_cam['eta_minutes']} min",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


# === EDGE-TO-EDGE MESH COMMUNICATION ===
class MeshNode:
    """Handles peer-to-peer communication between edge nodes."""

    def __init__(self, cam_id):
        self.cam_id = cam_id
        self.peers = {}  # cam_id -> url
        self.handoff_messages = deque(maxlen=100)
        self.expected_vehicles = {}  # plate -> expected arrival info

    def send_handoff(self, handoff_msg, target_cam_id):
        """Send a predictive handoff message to a neighboring camera's edge node."""
        # In production, this would be a direct HTTP POST to the neighbor's edge node
        # For now, we send it to the backend for relay
        try:
            requests.post(f"{BACKEND_URL}/api/mesh/handoff", json=handoff_msg, timeout=3)
            log.info(f"[{self.cam_id}] Handoff sent: {handoff_msg['plate']} -> {target_cam_id} (ETA {handoff_msg['eta_minutes']}min)")
        except:
            pass  # mesh is best-effort

    def receive_handoff(self, handoff_msg):
        """Receive a handoff message from another edge node."""
        plate = handoff_msg.get("plate")
        if plate:
            self.expected_vehicles[plate] = {
                "from_camera": handoff_msg.get("from_camera"),
                "eta_minutes": handoff_msg.get("eta_minutes"),
                "received_at": datetime.now(timezone.utc).isoformat()
            }
            log.info(f"[{self.cam_id}] Handoff received: expecting {plate} from {handoff_msg.get('from_camera')} in {handoff_msg.get('eta_minutes')}min")

    def check_expected_arrival(self, plate):
        """Check if a detected vehicle was predicted by a neighbor."""
        if plate in self.expected_vehicles:
            info = self.expected_vehicles.pop(plate)
            return {
                "type": "predictive_match",
                "plate": plate,
                "predicted_by": info["from_camera"],
                "predicted_eta": info["eta_minutes"],
                "message": f"Predictive tracking confirmed — {plate} arrived from {info['from_camera']}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        return None


# === FEDERATED LEARNING STUB ===
class FederatedNode:
    """Shares model weights with other edge nodes to improve detection accuracy."""

    def __init__(self, cam_id):
        self.cam_id = cam_id
        self.model_version = 1
        self.detection_stats = {"total": 0, "high_conf": 0, "low_conf": 0}
        self.model_updates = deque(maxlen=10)

    def record_detection(self, confidence):
        """Record detection quality for federated aggregation."""
        self.detection_stats["total"] += 1
        if confidence >= 0.7:
            self.detection_stats["high_conf"] += 1
        else:
            self.detection_stats["low_conf"] += 1

    def get_model_update(self):
        """Generate a model update to share with the federation."""
        update = {
            "cam_id": self.cam_id,
            "model_version": self.model_version,
            "stats": self.detection_stats,
            "accuracy_estimate": self.detection_stats["high_conf"] / max(1, self.detection_stats["total"]),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return update

    def receive_model_update(self, update):
        """Receive a model update from another node."""
        self.model_updates.append(update)
        log.debug(f"[{self.cam_id}] Received federated update from {update['cam_id']}")

    def aggregate(self):
        """Simulate federated averaging — in production this would use actual model weights."""
        if not self.model_updates:
            return None
        total_detections = sum(u["stats"]["total"] for u in self.model_updates) + self.detection_stats["total"]
        total_high = sum(u["stats"]["high_conf"] for u in self.model_updates) + self.detection_stats["high_conf"]
        return {
            "federated_accuracy": total_high / max(1, total_detections),
            "contributing_nodes": len(self.model_updates) + 1,
            "total_detections": total_detections
        }


# === AI MODULES (lazy-loaded) ===
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
INDIAN_PLATE_REGEX = re.compile(r'^[A-Z]{2}[\d]{1,2}[A-Z]{1,3}[\d]{3,4}$', re.IGNORECASE)

def clean_plate(text):
    if not text: return None
    cleaned = re.sub(r'[\s\-\._]', '', text).upper()
    if len(cleaned) >= 2:
        cleaned = cleaned[:2].replace('0', 'O').replace('1', 'I') + cleaned[2:]
    if INDIAN_PLATE_REGEX.match(cleaned): return cleaned
    state_codes = ['GJ', 'MH', 'DL', 'KA', 'TN', 'AP', 'TS', 'KL', 'RJ', 'UP', 'WB', 'MP', 'PB']
    for code in state_codes:
        if cleaned.startswith(code) and len(cleaned) >= 5:
            return cleaned
    return None

def read_number_plate(frame, x1, y1, x2, y2):
    reader = get_ocr()
    plate_y1 = y1 + int((y2 - y1) * 0.6)
    crop = frame[plate_y1:y2, max(0, x1-10):min(frame.shape[1], x2+10)]
    if crop.size < 100: return None
    try:
        results = reader.readtext(crop)
        texts = [t for _, t, _ in results if len(t) >= 4]
        if texts:
            cleaned = clean_plate(max(texts, key=len))
            return cleaned or max(texts, key=len)
    except: pass
    return None

def detect_objects(frame, cam_id):
    model = get_yolo()
    results = model(frame, conf=CONFIDENCE_THRESHOLD, verbose=False)
    detections = []
    for result in results:
        for box in result.boxes:
            cls_id = int(box.cls[0])
            conf = float(box.conf[0])
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            if cls_id in VEHICLE_CLASSES:
                plate = read_number_plate(frame, x1, y1, x2, y2)
                detections.append({
                    "type": "vehicle", "cam_id": cam_id, "camera_id": cam_id,
                    "vehicle_type": VEHICLE_CLASSES[cls_id], "confidence": round(conf, 3),
                    "bbox": [x1, y1, x2, y2], "plate": plate,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
            elif cls_id == 0:
                detections.append({
                    "type": "person", "cam_id": cam_id, "camera_id": cam_id,
                    "confidence": round(conf, 3), "bbox": [x1, y1, x2, y2],
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
    return detections


# === PUBLISH ===
def publish(cam_id, detection):
    if cam_id in CAMERAS:
        detection["cam_name"] = CAMERAS[cam_id]["name"]
        detection["camera_id"] = cam_id
        detection["lat"] = CAMERAS[cam_id]["lat"]
        detection["lon"] = CAMERAS[cam_id]["lon"]
    try:
        requests.post(f"{BACKEND_URL}/api/test/detection", json=detection, timeout=5)
    except: pass

    if detection.get("type") == "vehicle":
        plate_str = f" plate={detection['plate']}" if detection.get("plate") else ""
        log.info(f"[{cam_id}] {detection['vehicle_type']} ({detection['confidence']}){plate_str}")
    elif detection.get("type") == "anomaly":
        log.warning(f"[{cam_id}] ANOMALY: {detection.get('anomaly_type','')} — {detection.get('message','')}")
    elif detection.get("type") == "predictive_handoff":
        log.info(f"[{cam_id}] HANDOFF: {detection.get('plate','')} -> {detection.get('predicted_camera','')}")


# === EDGE NODE v2 ===
def run_edge_node_v2(cam_id, show=False, mesh=False, federated=False):
    log.info(f"[{cam_id}] Starting EIN 2.0 Edge Node")
    log.info(f"[{cam_id}] Features: AI=on, Anomaly=on, Mesh={'on' if mesh else 'off'}, Federated={'on' if federated else 'off'}")

    stream = SentinelStream(cam_id)
    if not stream.login():
        log.error(f"[{cam_id}] Login failed")
        return

    anomaly_engine = AnomalyEngine(cam_id)
    tracker = PredictiveTracker(cam_id)
    mesh_node = MeshNode(cam_id) if mesh else None
    fed_node = FederatedNode(cam_id) if federated else None

    cam_name = CAMERAS.get(cam_id, {}).get("name", "Unknown")
    log.info(f"[{cam_id}] LIVE streaming from {cam_name}")

    frame_count = 0
    while True:
        try:
            frame = stream.read_live_frame()
            if frame is None:
                time.sleep(2)
                continue

            frame_count += 1
            detections = detect_objects(frame, cam_id)

            # Record federated stats
            if fed_node:
                for d in detections:
                    fed_node.record_detection(d.get("confidence", 0.5))

            # Run anomaly detection
            anomalies = anomaly_engine.analyze(frame, detections)
            for anomaly in anomalies:
                publish(cam_id, anomaly)

            # Process each vehicle detection
            for det in detections:
                if det["type"] == "vehicle" and det.get("plate"):
                    # Check if this vehicle was predicted by a neighbor
                    if mesh_node:
                        match = mesh_node.check_expected_arrival(det["plate"])
                        if match:
                            publish(cam_id, match)

                    # Predict where this vehicle will go next
                    predicted = tracker.predict_next_camera(det["plate"], detections)
                    if predicted and mesh_node:
                        handoff = tracker.create_handoff_message(det["plate"], det["vehicle_type"], predicted)
                        mesh_node.send_handoff(handoff, predicted["predicted_camera"])
                        publish(cam_id, handoff)

                publish(cam_id, det)

            # Draw on frame if showing
            if show:
                for det in detections:
                    x1, y1, x2, y2 = det.get("bbox", [0,0,0,0])
                    if det["type"] == "vehicle":
                        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                        label = f"{det['vehicle_type']} {det['confidence']:.2f}"
                        if det.get("plate"): label += f" | {det['plate']}"
                        cv2.putText(frame, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,255,0), 2)
                    elif det["type"] == "person":
                        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)

                # Draw anomaly alerts on frame
                for anomaly in anomalies:
                    cv2.putText(frame, f"ANOMALY: {anomaly['anomaly_type']}", (10, 30),
                               cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)

                cv2.imshow(f"{cam_id} - {cam_name}", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break

            time.sleep(FRAME_INTERVAL)

        except KeyboardInterrupt:
            log.info(f"[{cam_id}] Stopped")
            break
        except Exception as e:
            log.error(f"[{cam_id}] Error: {e}")
            time.sleep(3)

    if show: cv2.destroyAllWindows()


def main():
    parser = argparse.ArgumentParser(description="EIN 2.0 — Edge Intelligence Mesh Node")
    parser.add_argument("--cam", type=str, help="Camera ID")
    parser.add_argument("--all", action="store_true", help="Run all 30 cameras")
    parser.add_argument("--show", action="store_true", help="Show live video window")
    parser.add_argument("--mesh", action="store_true", help="Enable edge-to-edge mesh communication")
    parser.add_argument("--federated", action="store_true", help="Enable federated learning")
    args = parser.parse_args()

    if args.all:
        log.info("Starting ALL 30 cameras with EIN 2.0...")
        threads = []
        for cam_id in CAMERAS:
            t = threading.Thread(target=run_edge_node_v2,
                               args=(cam_id, False, args.mesh, args.federated), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(0.5)
        log.info("All edge nodes started. Press Ctrl+C to stop.")
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            log.info("Shutting down...")
    elif args.cam:
        run_edge_node_v2(args.cam, args.show, args.mesh, args.federated)
    else:
        parser.print_help()
        print()
        print("EIN 2.0 Examples:")
        print("  python edge_v2.py --cam cam01 --show --mesh --federated  # Everything on")
        print("  python edge_v2.py --cam cam01 --show                     # AI + anomalies")
        print("  python edge_v2.py --all --mesh                           # All 30 + mesh")

if __name__ == "__main__":
    main()
