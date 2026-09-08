#!/usr/bin/env python3
"""
EIN 2.0 Backend — Mesh Intelligence Server
Upgraded backend with: mesh handoff relay, predictive tracking, anomaly aggregation,
80K camera scalability endpoints, federated learning coordination.

Endpoints (all v1 endpoints preserved + new v2 endpoints):
  GET  /                           Health check
  GET  /api/detections             Recent detections
  POST /api/test/detection         Inject detection (from edge nodes)
  GET  /api/stats                  KPI statistics
  GET  /api/cameras                Camera catalogue
  GET  /api/alerts                 Recent alerts
  GET  /api/trails                 Vehicle trails
  GET  /ws                         WebSocket (dashboard)
  --- NEW v2 ENDPOINTS ---
  POST /api/mesh/handoff           Receive predictive handoff from edge node
  GET  /api/mesh/handoffs          Get active handoffs
  GET  /api/anomalies              Get anomaly detections
  POST /api/federated/update       Receive federated learning update
  GET  /api/federated/status       Get federated learning status
  GET  /api/scalability            Get 80K camera scalability metrics
  GET  /api/predictions            Get active vehicle predictions
"""

import os, json, sqlite3, asyncio, math
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

DB_PATH = os.path.join(os.path.dirname(__file__), "ein_database.db")

CAMERAS = {
    "cam01":  {"name": "Chimanbhai Bridge, Ahmedabad",  "lat": 23.0395, "lon": 72.5663, "neighbors": ["cam02", "cam04", "cam20"]},
    "cam02":  {"name": "Janpath Road, Ahmedabad",       "lat": 23.0310, "lon": 72.5190, "neighbors": ["cam01", "cam03", "cam13"]},
    "cam03":  {"name": "ONGC Office, Ahmedabad",         "lat": 23.0330, "lon": 72.5310, "neighbors": ["cam02", "cam04"]},
    "cam04":  {"name": "Paldi Circle, Ahmedabad",        "lat": 23.0180, "lon": 72.5670, "neighbors": ["cam01", "cam03", "cam20"]},
    "cam05":  {"name": "Visat Teen Rasta, Ahmedabad",    "lat": 23.0740, "lon": 72.5280, "neighbors": ["cam16"]},
    "cam06":  {"name": "Junagadh Camera 06",             "lat": 21.5220, "lon": 70.4570, "neighbors": ["cam07", "cam08"]},
    "cam07":  {"name": "Junagadh Camera 07",             "lat": 21.5240, "lon": 70.4600, "neighbors": ["cam06", "cam08"]},
    "cam08":  {"name": "Junagadh Camera 08",             "lat": 21.5260, "lon": 70.4630, "neighbors": ["cam06", "cam07"]},
    "cam09":  {"name": "Junagadh Camera 09",             "lat": 21.5280, "lon": 70.4660, "neighbors": ["cam06", "cam10"]},
    "cam10":  {"name": "Junagadh Camera 10",             "lat": 21.5300, "lon": 70.4690, "neighbors": ["cam08", "cam11"]},
    "cam11":  {"name": "Junagadh Camera 11",             "lat": 21.5320, "lon": 70.4720, "neighbors": ["cam10"]},
    "cam12":  {"name": "Tri Mandir Adalaj",              "lat": 23.1660, "lon": 72.5860, "neighbors": ["cam05"]},
    "cam13":  {"name": "CN Vidhyalaya, Ahmedabad",        "lat": 23.0420, "lon": 72.5420, "neighbors": ["cam02", "cam14"]},
    "cam14":  {"name": "Delight RLVD, Ahmedabad",        "lat": 23.0450, "lon": 72.5450, "neighbors": ["cam13", "cam15"]},
    "cam15":  {"name": "Suvidha Park, Ahmedabad",        "lat": 23.0480, "lon": 72.5480, "neighbors": ["cam14", "cam16"]},
    "cam16":  {"name": "Visat P2, Ahmedabad",             "lat": 23.0750, "lon": 72.5290, "neighbors": ["cam05", "cam15"]},
    "cam17":  {"name": "Rajkot Camera 17",                "lat": 22.3030, "lon": 70.8020, "neighbors": ["cam18"]},
    "cam18":  {"name": "Rajkot Camera 18",                "lat": 22.3050, "lon": 70.8050, "neighbors": ["cam17"]},
    "cam19":  {"name": "Khaparia, Navsari",              "lat": 20.9520, "lon": 72.9300, "neighbors": ["cam27"]},
    "cam20":  {"name": "Mohanpura, Ahmedabad",            "lat": 23.0350, "lon": 72.5700, "neighbors": ["cam01", "cam04"]},
    "cam21":  {"name": "Patan Dethali",                   "lat": 23.8470, "lon": 72.1300, "neighbors": ["cam22"]},
    "cam22":  {"name": "BK Mervada",                      "lat": 23.8500, "lon": 72.1350, "neighbors": ["cam21"]},
    "cam23":  {"name": "Kheram",                          "lat": 21.1000, "lon": 71.7500, "neighbors": ["cam24"]},
    "cam24":  {"name": "Deshgam",                          "lat": 21.1050, "lon": 71.7550, "neighbors": ["cam23", "cam25"]},
    "cam25":  {"name": "Dhanori",                          "lat": 21.1100, "lon": 71.7600, "neighbors": ["cam24", "cam26"]},
    "cam26":  {"name": "Tankal",                           "lat": 21.1150, "lon": 71.7650, "neighbors": ["cam25"]},
    "cam27":  {"name": "Bilimora Camera 27",               "lat": 20.7800, "lon": 72.9500, "neighbors": ["cam19", "cam28"]},
    "cam28":  {"name": "Bilimora Camera 28",               "lat": 20.7820, "lon": 72.9520, "neighbors": ["cam27", "cam29"]},
    "cam29":  {"name": "Bilimora Camera 29",               "lat": 20.7840, "lon": 72.9540, "neighbors": ["cam27", "cam28"]},
    "cam30":  {"name": "Gandhidham",                       "lat": 23.0730, "lon": 70.1330, "neighbors": []},
}

WATCHLIST = ["GJ01AB1234", "GJ05XY9988", "GJ01CD4567"]

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS detections (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cam_id TEXT, cam_name TEXT, type TEXT,
        vehicle_type TEXT, confidence REAL, plate TEXT, bbox TEXT, lat REAL, lon REAL,
        timestamp TEXT, created_at TEXT DEFAULT (datetime('now')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cam_id TEXT, alert_type TEXT, message TEXT,
        plate TEXT, severity TEXT, timestamp TEXT, created_at TEXT DEFAULT (datetime('now')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS anomalies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cam_id TEXT, anomaly_type TEXT, severity TEXT,
        message TEXT, timestamp TEXT, created_at TEXT DEFAULT (datetime('now')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS handoffs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, from_cam TEXT, to_cam TEXT, plate TEXT,
        vehicle_type TEXT, distance_km REAL, eta_minutes REAL, timestamp TEXT,
        created_at TEXT DEFAULT (datetime('now')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS federated_updates (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cam_id TEXT, model_version INTEGER,
        total_detections INTEGER, high_conf INTEGER, accuracy REAL, timestamp TEXT,
        created_at TEXT DEFAULT (datetime('now')))""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cam ON detections(cam_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_plate ON detections(plate)")
    conn.commit()
    conn.close()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        print(f"[WS] Dashboard connected. Total: {len(self.active)}")
    def disconnect(self, ws: WebSocket):
        if ws in self.active: self.active.remove(ws)
    async def broadcast(self, channel: str, data: dict):
        msg = {"channel": channel, "data": data}
        dead = []
        for ws in self.active:
            try: await ws.send_json(msg)
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

manager = ConnectionManager()
app = FastAPI(title="EIN 2.0 Backend — Mesh Intelligence Server", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# === v1 ENDPOINTS (preserved) ===
@app.get("/")
async def health():
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    anomalies = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
    handoffs = conn.execute("SELECT COUNT(*) FROM handoffs").fetchone()[0]
    conn.close()
    return {"status": "online", "service": "EIN 2.0 Mesh Intelligence", "version": "2.0.0",
            "cameras": len(CAMERAS), "total_detections": count, "anomalies": anomalies, "handoffs": handoffs}

@app.get("/api/detections")
async def get_detections(limit: int = 100, cam_id: Optional[str] = None):
    conn = get_db()
    q = "SELECT * FROM detections WHERE 1=1"
    p = []
    if cam_id: q += " AND cam_id = ?"; p.append(cam_id)
    q += " ORDER BY id DESC LIMIT ?"; p.append(limit)
    rows = conn.execute(q, p).fetchall()
    conn.close()
    return {"count": len(rows), "detections": [dict(r) for r in rows]}

@app.post("/api/test/detection")
async def test_detection(request: Request):
    data = await request.json()
    cam_id = data.get("cam_id") or data.get("camera_id") or "unknown"
    cam_info = CAMERAS.get(cam_id, {"name": "Unknown", "lat": 0, "lon": 0})
    det = {"camera_id": cam_id, "cam_id": cam_id, "cam_name": data.get("cam_name") or cam_info["name"],
           "type": data.get("type", "unknown"), "vehicle_type": data.get("vehicle_type"),
           "vehicle_color": data.get("vehicle_color", "unknown"), "confidence": data.get("confidence"),
           "plate": data.get("plate"), "bbox": data.get("bbox"),
           "lat": data.get("lat") or cam_info["lat"], "lon": data.get("lon") or cam_info["lon"],
           "timestamp": data.get("timestamp") or datetime.now(timezone.utc).isoformat()}

    conn = get_db()
    conn.execute("INSERT INTO detections (cam_id, cam_name, type, vehicle_type, confidence, plate, bbox, lat, lon, timestamp) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (det["cam_id"], det["cam_name"], det["type"], det["vehicle_type"], det["confidence"],
         det["plate"], json.dumps(det["bbox"]) if det["bbox"] else None, det["lat"], det["lon"], det["timestamp"]))

    # Store anomaly separately
    if det["type"] == "anomaly":
        conn.execute("INSERT INTO anomalies (cam_id, anomaly_type, severity, message, timestamp) VALUES (?,?,?,?,?)",
            (cam_id, data.get("anomaly_type", "unknown"), data.get("severity", "medium"),
             data.get("message", ""), det["timestamp"]))
        await manager.broadcast("alert", {**det, "anomaly_type": data.get("anomaly_type"), "severity": data.get("severity")})

    plate = det.get("plate")
    if plate and any(w.upper() in plate.upper() for w in WATCHLIST):
        conn.execute("INSERT INTO alerts (cam_id, alert_type, message, plate, severity, timestamp) VALUES (?,?,?,?,?,?)",
            (cam_id, "watchlist", f"Watchlist match: {plate}", plate, "high", det["timestamp"]))
        await manager.broadcast("alert", {"camera_id": cam_id, "plate": plate, "description": "matched watchlist", "timestamp": det["timestamp"]})

    conn.commit(); conn.close()
    await manager.broadcast("detection", det)
    return {"status": "ok", "cam_id": cam_id}

@app.get("/api/stats")
async def get_stats():
    conn = get_db()
    return {"cameras_online": len(CAMERAS), "cameras_total": len(CAMERAS),
            "total_detections": conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0],
            "total_vehicles": conn.execute("SELECT COUNT(*) FROM detections WHERE type='vehicle'").fetchone()[0],
            "total_persons": conn.execute("SELECT COUNT(*) FROM detections WHERE type='person'").fetchone()[0],
            "plates_read": conn.execute("SELECT COUNT(*) FROM detections WHERE plate IS NOT NULL AND plate != ''").fetchone()[0],
            "unique_plates": conn.execute("SELECT COUNT(DISTINCT plate) FROM detections WHERE plate IS NOT NULL AND plate != ''").fetchone()[0],
            "alerts_24h": conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0],
            "anomalies_detected": conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0],
            "handoffs_sent": conn.execute("SELECT COUNT(*) FROM handoffs").fetchone()[0],
            "federated_nodes": conn.execute("SELECT COUNT(DISTINCT cam_id) FROM federated_updates").fetchone()[0]}

@app.get("/api/cameras")
async def get_cameras():
    conn = get_db()
    cams = []
    for cid, info in CAMERAS.items():
        count = conn.execute("SELECT COUNT(*) FROM detections WHERE cam_id=?", (cid,)).fetchone()[0]
        cams.append({"cam_id": cid, "name": info["name"], "lat": info["lat"], "lon": info["lon"],
                      "neighbors": info["neighbors"], "detection_count": count, "status": "online"})
    conn.close()
    return {"count": len(cams), "cameras": cams}

@app.get("/api/alerts")
async def get_alerts(limit: int = 50):
    conn = get_db()
    rows = conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return {"count": len(rows), "alerts": [dict(r) for r in rows]}

@app.get("/api/trails")
async def get_trails():
    conn = get_db()
    rows = conn.execute("SELECT plate, cam_id, cam_name, lat, lon, timestamp FROM detections WHERE plate IS NOT NULL AND plate != '' ORDER BY timestamp DESC LIMIT 500").fetchall()
    conn.close()
    trails = {}
    for r in rows:
        p = r["plate"]
        if p not in trails: trails[p] = []
        trails[p].append(dict(r))
    multi = {k: v for k, v in trails.items() if len(set(x["cam_id"] for x in v)) >= 2}
    return {"count": len(multi), "trails": multi}

# === NEW v2 ENDPOINTS ===

@app.post("/api/mesh/handoff")
async def mesh_handoff(request: Request):
    """Receive predictive handoff from an edge node."""
    data = await request.json()
    conn = get_db()
    conn.execute("INSERT INTO handoffs (from_cam, to_cam, plate, vehicle_type, distance_km, eta_minutes, timestamp) VALUES (?,?,?,?,?,?,?)",
        (data.get("from_camera"), data.get("to_camera"), data.get("plate"),
         data.get("vehicle_type"), data.get("distance_km"), data.get("eta_minutes"),
         data.get("timestamp", datetime.now(timezone.utc).isoformat())))
    conn.commit(); conn.close()
    await manager.broadcast("prediction", data)
    return {"status": "ok", "from": data.get("from_camera"), "to": data.get("to_camera")}

@app.get("/api/mesh/handoffs")
async def get_handoffs(limit: int = 50):
    conn = get_db()
    rows = conn.execute("SELECT * FROM handoffs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return {"count": len(rows), "handoffs": [dict(r) for r in rows]}

@app.get("/api/anomalies")
async def get_anomalies(limit: int = 50):
    conn = get_db()
    rows = conn.execute("SELECT * FROM anomalies ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return {"count": len(rows), "anomalies": [dict(r) for r in rows]}

@app.post("/api/federated/update")
async def federated_update(request: Request):
    """Receive federated learning model update from an edge node."""
    data = await request.json()
    conn = get_db()
    conn.execute("INSERT INTO federated_updates (cam_id, model_version, total_detections, high_conf, accuracy, timestamp) VALUES (?,?,?,?,?,?)",
        (data.get("cam_id"), data.get("model_version", 1),
         data.get("stats", {}).get("total", 0), data.get("stats", {}).get("high_conf", 0),
         data.get("accuracy_estimate", 0), data.get("timestamp", datetime.now(timezone.utc).isoformat())))
    conn.commit(); conn.close()
    return {"status": "ok", "cam_id": data.get("cam_id")}

@app.get("/api/federated/status")
async def federated_status():
    conn = get_db()
    rows = conn.execute("SELECT * FROM federated_updates ORDER BY id DESC LIMIT 30").fetchall()
    total_det = sum(r["total_detections"] for r in rows)
    total_high = sum(r["high_conf"] for r in rows)
    conn.close()
    return {"contributing_nodes": len(rows), "total_detections": total_det,
            "federated_accuracy": total_high / max(1, total_det),
            "updates": [dict(r) for r in rows]}

@app.get("/api/scalability")
async def scalability():
    """80,000-camera readiness analysis."""
    # Current system stats
    conn = get_db()
    current_detections = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    current_cameras = len(CAMERAS)
    conn.close()

    # 80,000 camera projections
    # Each detection = ~1KB JSON. Average 10 detections/min per camera.
    # 80,000 cameras * 10 det/min * 1KB = ~800 MB/min = ~13 MB/s = ~106 Mbps
    # vs traditional VMS: 80,000 * 4 Mbps (1080p) = 320 Gbps
    return {
        "current_scale": {"cameras": current_cameras, "detections": current_detections},
        "target_scale": {"cameras": 80000},
        "ein_bandwidth": {
            "per_camera_per_detection_kb": 1,
            "avg_detections_per_min": 10,
            "total_bandwidth_mbps": round(80000 * 10 * 1 / 1024 / 60 * 8, 1),
            "description": "~106 Mbps for 80,000 cameras (metadata only)"
        },
        "traditional_vms_bandwidth": {
            "per_camera_mbps": 4,
            "total_bandwidth_gbps": 80000 * 4 / 1000,
            "description": "320 Gbps for 80,000 cameras (full video)"
        },
        "bandwidth_reduction": "99.97%",
        "architecture": {
            "edge": "Kubernetes pods — 1 pod per 100 cameras, auto-scaling",
            "message_queue": "Apache Kafka — 10 partitions, 1M msgs/sec capacity",
            "database": "PostgreSQL + TimescaleDB — time-series optimized",
            "cache": "Redis cluster — real-time state for 80K cameras",
            "websocket": "WebSocket gateway with sticky sessions, horizontal scaling",
            "dashboard": "CDN-distributed static dashboard, geo-replicated"
        },
        "cost_comparison": {
            "ein_monthly": "Approx Rs 2-3 lakh/month (cloud compute + bandwidth)",
            "vms_monthly": "Approx Rs 50-80 lakh/month (bandwidth + storage + compute)",
            "savings": "94%+ cost reduction"
        }
    }

@app.get("/api/predictions")
async def get_predictions():
    """Get active vehicle predictions (predictive tracking)."""
    conn = get_db()
    rows = conn.execute("SELECT * FROM handoffs ORDER BY id DESC LIMIT 20").fetchall()
    conn.close()
    return {"count": len(rows), "predictions": [dict(r) for r in rows]}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_json({"channel": "pong", "data": {"detections": 0}})
    except: manager.disconnect(ws)

@app.on_event("startup")
async def startup():
    init_db()
    print("=" * 60)
    print("  EIN 2.0 Backend — Mesh Intelligence Server")
    print("=" * 60)
    print(f"  Cameras:     {len(CAMERAS)}")
    print(f"  Endpoints:   14 (v1 + v2)")
    print(f"  New:         mesh/handoff, anomalies, federated, scalability")
    print(f"  API docs:    http://localhost:8000/docs")
    print(f"  WebSocket:   ws://localhost:8000/ws")
    print("=" * 60)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
