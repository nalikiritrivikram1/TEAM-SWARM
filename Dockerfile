FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg libgl1 libglib2.0-0 libsm6 libxext6 libxrender-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY edge.py .
ENV CAMERA_ID=cam04 CAMERA_LABEL="Paldi Circle" USE_HLS=true MQTT_HOST=localhost MQTT_PORT=1883
CMD ["python", "edge.py"]
