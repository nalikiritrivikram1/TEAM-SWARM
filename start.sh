#!/usr/bin/env bash
set -e
MQTT_HOST="${1:-localhost}"
USE_HLS="${2:-true}"
MAX="${3:-30}"
echo "============================================"
echo "  EIN Edge Launcher | MQTT:$MQTT_HOST HLS:$USE_HLS Max:$MAX"
echo "============================================"
CAMERAS=(
  "cam01|Chimanbhai Bridge" "cam02|Janpath" "cam03|ONGC Office" "cam04|Paldi Circle"
  "cam05|Visat Teen Rasta" "cam06|Timbavadi Gate Junagadh" "cam07|Hero Showroom Gir-Somnath"
  "cam08|Majewadi Gate Junagadh" "cam09|New Bypass Junagadh" "cam10|Char Chowk Road Junagadh"
  "cam11|Dolatpara Junagadh" "cam12|Tri Mandir Adalaj Tollnaka" "cam13|CN Vidhyalaya"
  "cam14|Delight RLVD" "cam15|Suvidha Park" "cam16|Visat P2" "cam17|Rajkot Bus Port"
  "cam18|Rajkot CCTV" "cam19|Khaparia Gram Panchayat Navsari" "cam20|Mohanpura"
  "cam21|Patan Dethali Char Rasta" "cam22|BK Mervada Trani Rasta" "cam23|Kheram"
  "cam24|Deshgam" "cam25|Dhanori" "cam26|Tankal" "cam27|Bilimora 1"
  "cam28|Bilimora 2" "cam29|Bilimora 3" "cam30|Gandhidham Rambaugh P2"
)
PIDS=(); count=0
for entry in "${CAMERAS[@]}"; do
  [ "$count" -ge "$MAX" ] && break
  IFS='|' read -r id label <<< "$entry"
  echo "Starting: $id ($label)"
  CAMERA_ID="$id" CAMERA_LABEL="$label" HLS_URL="https://cctv.corp8.cloud/$id/index.m3u8" \
  RTSP_URL="rtsp://103.250.160.189:8554/stream/$id" USE_HLS="$USE_HLS" \
  MQTT_HOST="$MQTT_HOST" MQTT_PORT="1883" python edge.py &
  PIDS+=($!); count=$((count+1)); sleep 2
done
echo "============================================"
echo "  $count edge nodes started | PIDs: ${PIDS[*]}"
echo "============================================"
echo "Ctrl+C to stop all"
trap 'kill ${PIDS[*]} 2>/dev/null; exit 0' INT TERM
wait
