from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from services.dashboard.api_client import ApiClient


st.set_page_config(page_title="People Counting Pipeline", layout="wide")

client = ApiClient()
SAMPLE_VIDEO_DIR = Path(os.getenv("SAMPLE_VIDEO_DIR", "/app/data/sample_videos"))
VIDEO_EXTENSIONS = {".mp4", ".avi", ".mov", ".mkv"}


def list_sample_videos() -> list[Path]:
    if not SAMPLE_VIDEO_DIR.exists():
        return []
    return sorted(
        path
        for path in SAMPLE_VIDEO_DIR.iterdir()
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS
    )

if "run_id" not in st.session_state:
    st.session_state.run_id = None

st.title("People Counting Pipeline")

with st.sidebar:
    st.header("Run")
    camera_id = st.text_input("Camera ID", value="camera_001")
    sample_fps = st.number_input("Sample FPS", min_value=0.1, max_value=10.0, value=1.0, step=0.5)
    resize_width = st.number_input("Resize width", min_value=160, max_value=1920, value=640, step=80)
    max_frames_enabled = st.checkbox("Limit frames for demo", value=True)
    max_frames = None
    if max_frames_enabled:
        max_frames = st.number_input("Max sampled frames", min_value=1, max_value=10000, value=120, step=10)
    save_annotated = st.checkbox("Save annotated frames", value=True)
    auto_refresh = st.checkbox("Auto refresh", value=True)
    refresh_seconds = st.slider("Refresh seconds", min_value=1, max_value=10, value=3)

sample_videos = list_sample_videos()
source_options = ["Sample video", "Upload video"] if sample_videos else ["Upload video"]
source_mode = st.radio("Video input", source_options, horizontal=True)

selected_sample = None
uploaded = None

if source_mode == "Sample video":
    selected_name = st.selectbox("Sample video", [path.name for path in sample_videos])
    selected_sample = next(path for path in sample_videos if path.name == selected_name)
    st.video(str(selected_sample))
else:
    uploaded = st.file_uploader("Upload video", type=["mp4", "avi", "mov", "mkv"])

col_start, col_run = st.columns([1, 3])
has_input = selected_sample is not None or uploaded is not None
with col_start:
    start_clicked = st.button("Start run", type="primary", disabled=not has_input)
with col_run:
    if st.session_state.run_id:
        st.code(st.session_state.run_id)

if start_clicked and has_input:
    try:
        with st.spinner("Uploading video and starting ingestion..."):
            if selected_sample is not None:
                filename = selected_sample.name
                file_bytes = selected_sample.read_bytes()
            else:
                filename = uploaded.name
                file_bytes = uploaded.getvalue()

            result = client.start_run(
                filename=filename,
                file_bytes=file_bytes,
                camera_id=camera_id,
                sample_fps=sample_fps,
                resize_width=int(resize_width),
                save_annotated_frames=save_annotated,
                max_frames=int(max_frames) if max_frames else None,
            )
            st.session_state.run_id = result["run_id"]
        st.success("Run started")
    except Exception as exc:
        st.error(f"Cannot start run: {exc}")

run_id = st.session_state.run_id
if not run_id:
    if sample_videos:
        st.info("Select the sample video and start a run.")
    else:
        st.info(f"No sample video found in {SAMPLE_VIDEO_DIR}. Upload a video or add one to data/sample_videos.")
    st.stop()

camera_run = None
storage_run = None
stats = None
detections = None

try:
    camera_run = client.get_camera_run(run_id)
except Exception as exc:
    st.warning(f"Camera Server status unavailable: {exc}")

try:
    storage_run = client.get_storage_run(run_id)
    stats = client.get_stats(run_id)
    detections = client.get_detections(run_id, limit=100)
except Exception as exc:
    st.warning(f"Storage Server data unavailable yet: {exc}")

left, middle, right, last = st.columns(4)
status = (storage_run or camera_run or {}).get("status", "waiting")
processed = (storage_run or {}).get("processed_frames", 0)
sampled = (storage_run or camera_run or {}).get("sampled_frames") or (camera_run or {}).get("published_frames", 0)
max_people = (storage_run or {}).get("max_people_count", 0)
avg_people = (storage_run or {}).get("avg_people_count", 0)

left.metric("Status", status)
middle.metric("Processed frames", processed)
right.metric("Sampled frames", sampled or 0)
last.metric("Max people", max_people)

if avg_people:
    st.caption(f"Average people per processed frame: {avg_people:.2f}")

if stats and stats.get("items"):
    chart_df = pd.DataFrame(stats["items"])
    if {"timestamp_ms", "person_count"}.issubset(chart_df.columns):
        chart_df["second"] = chart_df["timestamp_ms"] / 1000
        st.line_chart(chart_df, x="second", y="person_count")

if detections and detections.get("items"):
    items = detections["items"]
    table_rows = [
        {
            "frame_id": item["frame_id"],
            "timestamp_s": round(item.get("timestamp_ms", 0) / 1000, 2),
            "person_count": item["person_count"],
            "boxes": len(item.get("boxes", [])),
        }
        for item in items
    ]
    st.dataframe(pd.DataFrame(table_rows), use_container_width=True)

    frame_options = [item["frame_id"] for item in items if item.get("annotated_object_key")]
    if frame_options:
        selected_frame = st.selectbox("Annotated frame", frame_options)
        image_bytes = client.get_annotated_frame(run_id, selected_frame)
        if image_bytes:
            st.image(image_bytes, caption=f"Frame {selected_frame}", use_column_width=True)
else:
    st.info("Waiting for detections...")

if auto_refresh and status not in {"completed", "failed"}:
    time.sleep(refresh_seconds)
    st.rerun()
