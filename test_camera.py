import requests
import cv2
import os
import re
from Crypto.Cipher import AES

print("=== EIN Camera Connection — FINAL FIX ===")
print()

PASSWORD = os.environ.get("SENTINEL_PASSWORD", "")
BASE = "https://cctv.corp8.cloud"
STREAM_URL = f"{BASE}/cam01/index.m3u8"

# Step 1: Login
print("1. Logging in to Sentinel...")
session = requests.Session()
r = session.post(f"{BASE}/auth/login", data={"password": PASSWORD}, allow_redirects=True)
print(f"   Login: {r.status_code}, cookies: {len(session.cookies)}")

# Step 2: Fetch the m3u8 playlist
print("2. Fetching HLS playlist...")
m3u8 = session.get(STREAM_URL).text

# Step 3: Parse encryption key and segment URLs
print("3. Parsing playlist...")

key_url = None
iv_hex = None
for line in m3u8.splitlines():
    if "#EXT-X-KEY" in line:
        uri_match = re.search(r'URI="([^"]+)"', line)
        if uri_match:
            key_url = uri_match.group(1)
        iv_match = re.search(r'IV=0x([0-9a-fA-F]+)', line)
        if iv_match:
            iv_hex = iv_match.group(1)

print(f"   Key URL: {key_url}")
print(f"   IV: {iv_hex}")

# Get segment URLs
segments = []
for line in m3u8.splitlines():
    line = line.strip()
    if line and not line.startswith("#"):
        segments.append(line)

print(f"   Segments found: {len(segments)}")

# Step 4: Fetch encryption key
print("4. Fetching encryption key...")
if key_url and not key_url.startswith("http"):
    if not key_url.startswith("/"):
        key_url = "/" + key_url
    key_url = BASE + key_url
print(f"   Full key URL: {key_url}")
key_data = session.get(key_url).content
print(f"   Key length: {len(key_data)} bytes")

# Step 5: Download and decrypt first segment
print("5. Downloading first video segment...")
seg = segments[0]
print(f"   Raw segment URL from playlist: {seg}")

# Build proper absolute URL
if seg.startswith("http"):
    seg_url = seg
elif seg.startswith("/"):
    seg_url = BASE + seg
else:
    # Relative URL - needs to be relative to the m3u8 path
    # m3u8 is at: https://cctv.corp8.cloud/cam01/index.m3u8
    # segments like seq0000.ts should become https://cctv.corp8.cloud/cam01/seq0000.ts
    seg_url = BASE + "/cam01/" + seg

print(f"   Full segment URL: {seg_url}")

seg_data = session.get(seg_url).content
print(f"   Segment size: {len(seg_data)} bytes")

# Step 6: Decrypt
print("6. Decrypting segment...")
if iv_hex:
    iv = bytes.fromhex(iv_hex)
else:
    iv = b'\x00' * 16

cipher = AES.new(key_data, AES.MODE_CBC, iv)
decrypted = cipher.decrypt(seg_data)
print(f"   Decrypted size: {len(decrypted)} bytes")

# Step 7: Read frame with OpenCV
print("7. Reading video frame...")
temp_file = "test_segment.ts"
with open(temp_file, "wb") as f:
    f.write(decrypted)

cap = cv2.VideoCapture(temp_file)
ok, frame = cap.read()

if ok:
    print(f"   *** CAMERA CONNECTED! Resolution: {frame.shape[1]}x{frame.shape[0]} ***")
    cv2.imwrite("test_frame.jpg", frame)
    print(f"   *** Frame saved as test_frame.jpg ***")
else:
    print("   Decrypted segment failed, trying raw segment...")
    with open("test_raw.ts", "wb") as f:
        f.write(seg_data)
    cap2 = cv2.VideoCapture("test_raw.ts")
    ok2, frame2 = cap2.read()
    if ok2:
        print(f"   *** RAW WORKS! Resolution: {frame2.shape[1]}x{frame2.shape[0]} ***")
        cv2.imwrite("test_frame.jpg", frame2)
    cap2.release()

cap.release()

# Cleanup temp files
for f in ["test_segment.ts", "test_raw.ts"]:
    if os.path.exists(f):
        os.remove(f)

print()
print("=== DONE ===")
