# EIN — Edge Intelligence Network

### Gujarat Police Innovation Challenge 2026 | Team: The Swarm

EIN puts AI at the edge of each CCTV camera. Instead of streaming video to a central server, each edge node processes video locally and sends only metadata, reducing bandwidth by 99%+ compared to traditional VMS systems.

## Features

- Live camera streaming with AES-128 decryption
- YOLOv8 vehicle detection (cars, buses, trucks, motorcycles)
- EasyOCR number plate recognition (ANPR)
- Palantir-style dark dashboard with live Gujarat map
- 30 cameras across Gujarat
- Real-time WebSocket updates
- Watchlist matching and alerts
- Vehicle trail correlation

## Quick Start

```bash
pip install requests opencv-python pycryptodome fastapi uvicorn ultralytics easyocr

# Terminal 1: Backend
cd backend && python server.py

# Terminal 2: Dashboard
cd dashboard && python -m http.server 8080

# Terminal 3: Edge AI
python edge.py --cam cam01 --show
```

## Live Dashboard

https://nalikiritrivikram1.github.io/TEAM-SWARM/

## GitHub

https://github.com/nalikiritrivikram1/TEAM-SWARM

## Team

- Team: The Swarm
- Member: 1.Imran Khan
          2.Ummadi Usha Sree
          3.Nasina Hima Harshitha
          4.Chevula Rupavathi
          5.Nalikiri Siva Venkata Trivikram
- Hackathon: Gujarat Police Innovation Challenge 2026
