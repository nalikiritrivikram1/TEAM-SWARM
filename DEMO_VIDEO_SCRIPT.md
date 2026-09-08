# EIN 2.0 — Demo Video Recording Script
## For Gujarat Police Innovation Challenge 2026 Submission

### Recording Setup:
- Use OBS Studio or Windows screen recorder (Win+G)
- Record at 1920x1080, 30fps
- Target length: 3-4 minutes
- Speak clearly or use text overlays

---

## VIDEO SCRIPT (scene by scene)

### SCENE 1: Dashboard Overview (30 seconds)
**Action:** Open http://localhost:8080 in browser
**Show:** The Palantir-style dark dashboard with Gujarat map
**Text overlay:** "EIN 2.0 — Edge Intelligence Mesh Network"
**Text overlay:** "30 live Sentinel cameras across Gujarat"

### SCENE 2: Live AI Detection (45 seconds)
**Action:** Switch to terminal showing: python edge_v2.py --cam cam01 --show --mesh --federated
**Show:** Live video window with green detection boxes around vehicles
**Show:** Terminal output showing detections:
  [cam01] bus (0.865) plate=GJ01AB1234 ->backend OK
  [cam01] car (0.599) ->backend OK
**Text overlay:** "Real-time YOLOv8 detection on live Sentinel camera"
**Text overlay:** "AES-128 HLS decryption + ANPR"

### SCENE 3: Dashboard LIVE MODE (30 seconds)
**Action:** Switch back to browser dashboard
**Show:** Status bar saying "LIVE MODE"
**Show:** Detection ripples appearing on the map
**Show:** KPI counters updating in real-time
**Show:** Alerts feed showing new detections
**Text overlay:** "Dashboard auto-switches to LIVE MODE when backend connects"

### SCENE 4: Anomaly Detection (30 seconds)
**Action:** Show terminal with anomaly alerts
**Show:** ANOMALY: wrong_way — Wrong-way driving detected
**Show:** ANOMALY: unattended — Unattended vehicle detected
**Text overlay:** "4 anomaly types: Wrong-way, Unattended, Crowd, Accident"

### SCENE 5: Predictive Tracking (30 seconds)
**Action:** Show the demo scenario
**Show:** Click "Run Demo" button on dashboard
**Show:** Stolen vehicle GJ01AB1234 tracked across 6 cameras
**Show:** Trail timeline showing the route
**Show:** Map camera following the vehicle
**Text overlay:** "Predictive tracking — cameras predict where vehicle appears next"

### SCENE 6: Scalability (30 seconds)
**Action:** Run: python load_test_80k.py --cams 1000 --duration 30
**Show:** Load test output showing 1,000 cameras sending detections
**Show:** Backend stats after load test
**Text overlay:** "Scales to 80,000 cameras — 99.97% bandwidth reduction"
**Text overlay:** "EIN: 106 Mbps vs Traditional VMS: 320 Gbps"

### SCENE 7: API + GitHub (15 seconds)
**Action:** Open http://localhost:8000/docs in browser
**Show:** 14 API endpoints including mesh, federated, scalability
**Show:** GitHub repo: https://github.com/nalikiritrivikram1/TEAM-SWARM
**Text overlay:** "14 REST API endpoints + WebSocket"
**Text overlay:** "Open source on GitHub"

### SCENE 8: Closing (15 seconds)
**Text overlay:** "EIN 2.0 — Edge Intelligence Mesh Network"
**Text overlay:** "Team: The Swarm"
**Text overlay:** "Predictive Tracking • Anomaly Detection • Federated Learning • Edge Mesh"
**Text overlay:** "80,000-camera ready • 99.97% bandwidth reduction"
**Text overlay:** "Gujarat Police Innovation Challenge 2026"

---

## Before Recording Checklist:
1. Start backend: python ein_mesh_server.py
2. Start dashboard: python -m http.server 8080
3. Start edge AI: python edge_v2.py --cam cam01 --show --mesh --federated
4. Wait for first detections to appear
5. Open dashboard in browser — confirm LIVE MODE
6. Start recording
7. Follow the script above
8. Upload to Google Drive
9. Make link public
10. Paste in hackathon form
