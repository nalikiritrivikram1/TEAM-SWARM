import os
import time

import cv2
import mss
import numpy as np

FPS = 10
CAPTURE_FPS = 2
OUTPUT_SCALE = 0.5
OUTPUT = "EIN2_Edge_Terminal.mp4"
RECORD_SECONDS = 30

with mss.mss() as sct:
    monitor = sct.monitors[1]
    width, height = monitor["width"], monitor["height"]
    output_width, output_height = round(width * OUTPUT_SCALE), round(height * OUTPUT_SCALE)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(OUTPUT, fourcc, FPS, (output_width, output_height))

    if not out.isOpened():
        raise RuntimeError("Could not open the MP4 writer")

    print(f"Recording terminal at {width}x{height} for {RECORD_SECONDS} seconds...")
    start = time.time()
    frame_count = 0
    while time.time() - start < RECORD_SECONDS:
        frame = np.asarray(sct.grab(monitor))[:, :, :3]
        frame = cv2.resize(frame, (output_width, output_height), interpolation=cv2.INTER_AREA)
        target_frames = min(int((time.time() - start) * FPS), RECORD_SECONDS * FPS)
        for _ in range(max(1, target_frames - frame_count)):
            out.write(frame)
            frame_count += 1

    out.release()

print(f"Saved {OUTPUT} ({os.path.getsize(OUTPUT) / 1024 / 1024:.1f} MB)")
print(f"Frames: {frame_count}, Duration: {frame_count / FPS:.1f}s")
