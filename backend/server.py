#!/usr/bin/env python3
"""
EIN Backend Server v2 — Edge Intelligence Network
Complete FastAPI backend with SQLite, WebSocket, and all API endpoints.

The WebSocket broadcasts detections in the format the dashboard expects:
  {"channel": "detection", "data": {...}}

Endpoints:
  GET  /                           Health check
  GET  /api/detections             Get recent detections (?limit=100&cam_id=cam01)
  POST /api/test/detection          Inject a detection (from edge nodes)
  GET  /api/stats                   Get system statistics (KPI cards)
  GET  /api/cameras                 Get camera catalogue with live status
  GET  /api/alerts                  Get recent alerts
  GET  /api/trails                  Get vehicle trails
  GET  /ws                          WebSocket for real-time detections
  GET  /docs                        Swagger API documentation

Run:
  cd backend
  python server.py
"""

import os
import json
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# === DATABASE ===
DB_PATH = os.path.join(os.path.dirname(__file__), "ein_database.db")

# 30 Cameras catalogue — IDs match dashboard's CAMERA_BY_ID
CAMERAS = {
    "cam01":  {"name": "Chimanbhai Bridge, Ahmedabad",  "lat": 23.0395, "lon": 72.5663, "city": "Ahmedabad"},
    "cam02":  {"name": "Janpath Road, Ahmedabad",       "lat": 23.0310, "lon": 72.5190, "city": "Ahmedabad"},
    "cam03":  {"name": "ONGC Office, Ahmedabad",         "lat": 23.0330, "lon": 72.5310, "city": "Ahmedabad"},
    "cam04":  {"name": "Paldi Circle, Ahmedabad",        "lat": 23.0180, "lon": 72.5670, "city": "Ahmedabad"},
    "cam05":  {"name": "Visat Teen Rasta, Ahmedabad",    "lat": 23.0740, "lon": 72.5280, "city": "Ahmedabad"},
    "cam06":  {"name": "Junagadh Camera 06",             "lat": 21.5220, "lon": 70.4570, "city": "Junagadh"},
    "cam07":  {"name": "Junagadh Camera 07",             "lat": 21.5240, "lon": 70.4600, "city": "Junagadh"},
    "cam08":  {"name": "Junagadh Camera 08",             "lat": 21.5260, "lon": 70.4630, "city": "Junagadh"},
    "cam09":  {"name": "Junagadh Camera 09",             "lat": 21.5280, "lon": 70.4660, "city": "Junagadh"},
    "cam10":  {"name": "Junagadh Camera 10",             "lat": 21.5300, "lon": 70.4690, "city": "Junagadh"},
    "cam11":  {"name": "Junagadh Camera 11",             "lat": 21.5320, "lon": 70.4720, "city": "Junagadh"},
    "cam12":  {"name": "Tri Mandir Adalaj",              "lat": 23.1660, "lon": 72.5860, "city": "Gandhinagar"},
    "cam13":  {"name": "CN Vidhyalaya, Ahmedabad",        "lat": 23.0420, "lon": 72.5420, "city": "Ahmedabad"},
    "cam14":  {"name": "Delight RLVD, Ahmedabad",        "lat": 23.0450, "lon": 72.5450, "city": "Ahmedabad"},
    "cam15":  {"name": "Suvidha Park, Ahmedabad",        "lat": 23.0480, "lon": 72.5480, "city": "Ahmedabad"},
    "cam16":  {"name": "Visat P2, Ahmedabad",             "lat": 23.0750, "lon": 72.5290, "city": "Ahmedabad"},
    "cam17":  {"name": "Rajkot Camera 17",                "lat": 22.3030, "lon": 70.8020, "city": "Rajkot"},
    "cam18":  {"name": "Rajkot Camera 18",                "lat": 22.3050, "lon": 70.8050, "city": "Rajkot"},
    "cam19":  {"name": "Khaparia, Navsari",              "lat": 20.9520, "lon": 72.9300, "city": "Navsari"},
    "cam20":  {"name": "Mohanpura, Ahmedabad",            "lat": 23.0350, "lon": 72.5700, "city": "Ahmedabad"},
    "cam21":  {"name": "Patan Dethali",                   "lat": 23.8470, "lon": 72.1300, "city": "Patan"},
    "cam22":  {"name": "BK Mervada",                      "lat": 23.8500, "lon": 72.1350, "city": "Patan"},
    "cam23":  {"name": "Kheram",                          "lat": 21.1000, "lon": 71.7500, "city": "Amreli"},
    "cam24":  {"name": "Deshgam",                          "lat": 21.1050, "lon": 71.7550, "city": "Amreli"},
    "cam25":  {"name": "Dhanori",                          "lat": 21.1100, "lon": 71.7600, "city": "Amreli"},
    "cam26":  {"name": "Tankal",                           "lat": 21.1150, "lon": 71.7650, "city": "Amreli"},
    "cam27":  {"name": "Bilimora Camera 27",               "lat": 20.7800, "lon": 72.9500, "city": "Bilimora"},
    "cam28":  {"name": "Bilimora Camera 28",               "lat": 20.7820, "lon": 72.9520, "city": "Bilimora"},
    "cam29":  {"name": "Bilimora Camera 29",               "lat": 20.7840, "lon": 72.9540, "city": "Bilimora"},
    "cam30":  {"name": "Gandhidham",                       "lat": 23.0730, "lon": 70.1330, "city": "Gandhidham"},
}

# Watchlist plates
WATCHLIST = ["GJ01AB1234", "GJ05XY9988", "GJ01CD4567"]


# === DATABASE SETUP ===
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS detections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cam_id TEXT,
            cam_name TEXT,
            type TEXT,
            vehicle_type TEXT,
            confidence REAL,
            plate TEXT,
            bbox TEXT,
            lat REAL,
            lon REAL,
            timestamp TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cam_id TEXT,
            alert_type TEXT,
            message TEXT,
            plate TEXT,
            severity TEXT,
            timestamp TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_cam ON detections(cam_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_plate ON detections(plate)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_type ON detections(type)")
    conn.commit()
    conn.close()
    print(f"[DB] SQLite initialized at {DB_PATH}")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# === WEBSOCKET MANAGER ===
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        print(f"[WS] Dashboard connected. Total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
        print(f"[WS] Dashboard disconnected. Total: {len(self.active)}")

    async def broadcast(self, channel: str, data: dict):
        """Broadcast message in the format the dashboard expects:
        {"channel": "detection", "data": {...}}
        or
        {"channel": "alert", "data": {...}}
        """
        message = {"channel": channel, "data": data}
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

manager = ConnectionManager()


# === FASTAPI APP ===
app = FastAPI(title="EIN Backend", version="2.0.0")

# CORS — allow dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# === ENDPOINTS ===

@app.get("/")
async def health():
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    conn.close()
    return {
        "status": "online",
        "service": "EIN Edge Intelligence Network",
        "version": "2.0.0",
        "cameras": len(CAMERAS),
        "total_detections": count,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/detections")
async def get_detections(limit: int = 100, cam_id: Optional[str] = None, det_type: Optional[str] = None):
    """Get recent detections."""
    conn = get_db()
    query = "SELECT * FROM detections WHERE 1=1"
    params = []

    if cam_id:
        query += " AND cam_id = ?"
        params.append(cam_id)
    if det_type:
        query += " AND type = ?"
        params.append(det_type)

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append({
            "id": row["id"],
            "cam_id": row["cam_id"],
            "camera_id": row["cam_id"],
            "cam_name": row["cam_name"],
            "type": row["type"],
            "vehicle_type": row["vehicle_type"],
            "confidence": row["confidence"],
            "plate": row["plate"],
            "bbox": json.loads(row["bbox"]) if row["bbox"] else None,
            "lat": row["lat"],
            "lon": row["lon"],
            "timestamp": row["timestamp"]
        })

    return {"count": len(results), "detections": results}


@app.post("/api/test/detection")
async def test_detection(request: Request):
    """Receive a detection from an edge node. Store in DB + broadcast via WebSocket."""
    try:
        data = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    cam_id = data.get("cam_id") or data.get("camera_id") or "unknown"
    cam_info = CAMERAS.get(cam_id, {"name": "Unknown", "lat": 0, "lon": 0})

    detection = {
        "camera_id": cam_id,
        "cam_id": cam_id,
        "cam_name": data.get("cam_name") or cam_info["name"],
        "type": data.get("type", "unknown"),
        "vehicle_type": data.get("vehicle_type"),
        "vehicle_color": data.get("vehicle_color", "unknown"),
        "confidence": data.get("confidence"),
        "plate": data.get("plate"),
        "bbox": data.get("bbox"),
        "lat": data.get("lat") or cam_info["lat"],
        "lon": data.get("lon") or cam_info["lon"],
        "timestamp": data.get("timestamp") or datetime.now(timezone.utc).isoformat()
    }

    # Store in SQLite
    conn = get_db()
    conn.execute("""
        INSERT INTO detections (cam_id, cam_name, type, vehicle_type, confidence, plate, bbox, lat, lon, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        detection["cam_id"],
        detection["cam_name"],
        detection["type"],
        detection["vehicle_type"],
        detection["confidence"],
        detection["plate"],
        json.dumps(detection["bbox"]) if detection["bbox"] else None,
        detection["lat"],
        detection["lon"],
        detection["timestamp"]
    ))

    # Check watchlist
    plate = detection.get("plate")
    if plate and any(w.upper() in plate.upper() for w in WATCHLIST):
        alert_data = {
            "camera_id": cam_id,
            "plate": plate,
            "description": f"matched watchlist — flagged vehicle",
            "timestamp": detection["timestamp"]
        }
        conn.execute("""
            INSERT INTO alerts (cam_id, alert_type, message, plate, severity, timestamp)
            VALUES (?, 'watchlist', ?, ?, 'high', ?)
        """, (cam_id, f"Watchlist match: {plate} at {detection['cam_name']}", plate, detection["timestamp"]))
        conn.commit()
        conn.close()

        # Broadcast alert to dashboard
        await manager.broadcast("alert", alert_data)
        print(f"[ALERT] Watchlist match: {plate} at {cam_id}")
    else:
        conn.commit()
        conn.close()

    # Broadcast detection to dashboard in the format it expects
    await manager.broadcast("detection", detection)

    return {"status": "ok", "cam_id": cam_id}


@app.get("/api/stats")
async def get_stats():
    """Get system statistics for dashboard KPI cards."""
    conn = get_db()

    total_detections = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    total_vehicles = conn.execute("SELECT COUNT(*) FROM detections WHERE type='vehicle'").fetchone()[0]
    total_persons = conn.execute("SELECT COUNT(*) FROM detections WHERE type='person'").fetchone()[0]
    plates_read = conn.execute("SELECT COUNT(*) FROM detections WHERE plate IS NOT NULL AND plate != ''").fetchone()[0]
    unique_plates = conn.execute("SELECT COUNT(DISTINCT plate) FROM detections WHERE plate IS NOT NULL AND plate != ''").fetchone()[0]
    total_alerts = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]

    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    recent = conn.execute("SELECT COUNT(*) FROM detections WHERE timestamp > ?", (one_hour_ago,)).fetchone()[0]

    top_cams = conn.execute("""
        SELECT cam_id, cam_name, COUNT(*) as count
        FROM detections
        GROUP BY cam_id
        ORDER BY count DESC
        LIMIT 5
    """).fetchall()

    recent_plates = conn.execute("""
        SELECT DISTINCT plate FROM detections
        WHERE plate IS NOT NULL AND plate != ''
        ORDER BY id DESC LIMIT 10
    """).fetchall()

    conn.close()

    return {
        "cameras_online": len(CAMERAS),
        "cameras_total": len(CAMERAS),
        "total_detections": total_detections,
        "total_vehicles": total_vehicles,
        "total_persons": total_persons,
        "plates_read": plates_read,
        "unique_plates": unique_plates,
        "alerts_24h": total_alerts,
        "detections_last_hour": recent,
        "detect_per_min": round(recent / 60, 1) if recent > 0 else 0,
        "top_cameras": [{"cam_id": r[0], "cam_name": r[1], "count": r[2]} for r in top_cams],
        "recent_plates": [r[0] for r in recent_plates],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/cameras")
async def get_cameras():
    """Get all 30 cameras with their status and detection counts."""
    conn = get_db()

    camera_list = []
    for cam_id, info in CAMERAS.items():
        count = conn.execute("SELECT COUNT(*) FROM detections WHERE cam_id = ?", (cam_id,)).fetchone()[0]
        last_det = conn.execute("SELECT timestamp FROM detections WHERE cam_id = ? ORDER BY id DESC LIMIT 1", (cam_id,)).fetchone()

        camera_list.append({
            "cam_id": cam_id,
            "name": info["name"],
            "city": info["city"],
            "lat": info["lat"],
            "lon": info["lon"],
            "detection_count": count,
            "status": "online",
            "last_detection": last_det[0] if last_det else None
        })

    conn.close()
    return {"count": len(camera_list), "cameras": camera_list}


@app.get("/api/alerts")
async def get_alerts(limit: int = 50):
    """Get recent alerts."""
    conn = get_db()
    rows = conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append({
            "id": row["id"],
            "cam_id": row["cam_id"],
            "alert_type": row["alert_type"],
            "message": row["message"],
            "plate": row["plate"],
            "severity": row["severity"],
            "timestamp": row["timestamp"]
        })

    return {"count": len(results), "alerts": results}


@app.get("/api/trails")
async def get_trails(limit: int = 20):
    """Get vehicle trails — same plate seen at multiple cameras."""
    conn = get_db()

    rows = conn.execute("""
        SELECT plate, cam_id, cam_name, lat, lon, timestamp
        FROM detections
        WHERE plate IS NOT NULL AND plate != ''
        ORDER BY timestamp DESC
        LIMIT 500
    """).fetchall()
    conn.close()

    trails = {}
    for row in rows:
        plate = row["plate"]
        if plate not in trails:
            trails[plate] = []
        trails[plate].append({
            "cam_id": row["cam_id"],
            "cam_name": row["cam_name"],
            "lat": row["lat"],
            "lon": row["lon"],
            "timestamp": row["timestamp"]
        })

    multi_cam_trails = {k: v for k, v in trails.items() if len(set(p["cam_id"] for p in v)) >= 2}

    return {"count": len(multi_cam_trails), "trails": multi_cam_trails}


# === WEBSOCKET ===
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                conn = get_db()
                count = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
                conn.close()
                await ws.send_json({"channel": "pong", "data": {"detections": count}})
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)


# === STARTUP ===
@app.on_event("startup")
async def startup():
    init_db()
    print("=" * 50)
    print("  EIN Backend v2 — Edge Intelligence Network")
    print("=" * 50)
    print(f"  Database:    {DB_PATH}")
    print(f"  Cameras:     {len(CAMERAS)}")
    print(f"  API docs:    http://localhost:8000/docs")
    print(f"  Dashboard:   http://localhost:8080")
    print(f"  WebSocket:   ws://localhost:8000/ws")
    print("=" * 50)
    print()
    print("  Waiting for edge nodes to send detections...")
    print("  Dashboard will auto-switch to LIVE MODE when connected.")
    print()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
