import json
import math
import os
import random
import shutil
import sqlite3
import subprocess
import traceback
from datetime import datetime, timezone
from shutil import which
from urllib.parse import urlparse

import ffmpeg
import pika
import requests
from dotenv import load_dotenv

try:
    from exiftool import ExifTool
except Exception:
    ExifTool = None

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jobs.db")
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "guest")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "reel_jobs")
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.getenv("FFPROBE_BIN", "ffprobe")
YTDLP_BIN = os.getenv("YTDLP_BIN", "yt-dlp")
TARGET_VARIANTS = 2


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def update_job(
    job_id: int,
    status: str,
    phase: str,
    result_json: str | None = None,
    error_message: str | None = None,
) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            UPDATE jobs
            SET status=?, phase=?, result_json=COALESCE(?, result_json), error_message=?, updated_at=?
            WHERE id=?
            """,
            (status, phase, result_json, error_message, now_iso(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reel_url TEXT NOT NULL,
                status TEXT NOT NULL,
                phase TEXT NOT NULL,
                result_json TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def download_direct_video(url: str, save_path: str) -> str:
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    content_type = (response.headers.get("Content-Type") or "").lower()
    if content_type and "video" not in content_type:
        raise RuntimeError(f"URL returned non-video Content-Type: {content_type}")

    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return save_path


def download_with_ytdlp(url: str, save_path: str) -> str:
    cmd = [YTDLP_BIN, "-o", save_path, "--no-playlist", "-f", "mp4/best", url]
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if not os.path.exists(save_path) or os.path.getsize(save_path) == 0:
            raise RuntimeError("yt-dlp finished without creating a video file")
        if completed.stderr:
            print(completed.stderr.strip())
        return save_path
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Non-direct URL ({url}) and {YTDLP_BIN} was not found. "
            "Install yt-dlp or provide a direct video URL."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        raise RuntimeError(
            f"yt-dlp failed downloading the reel. stderr={stderr or 'N/A'} stdout={stdout or 'N/A'}"
        ) from exc


def download_video(url: str, save_path: str) -> str:
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    parsed = urlparse(url)
    looks_direct = parsed.path.lower().endswith((".mp4", ".mov", ".webm", ".mkv"))

    if looks_direct:
        return download_direct_video(url, save_path)
    return download_with_ytdlp(url, save_path)


def process_video(
    input_path: str, output_path: str, config: dict, metadata: dict | None = None
) -> bool:
    try:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        probe = ffmpeg.probe(input_path, cmd=FFPROBE_BIN)
        video_stream = next(
            (stream for stream in probe["streams"] if stream["codec_type"] == "video"),
            None,
        )
        if video_stream is None:
            raise RuntimeError("No video stream found in the input file")
        original_width = int(video_stream["width"])
        original_height = int(video_stream["height"])
    except ffmpeg.Error as exc:
        stderr = (
            exc.stderr.decode("utf-8", errors="replace")
            if getattr(exc, "stderr", None)
            else str(exc)
        )
        print(f"ffprobe error on {input_path}:\n{stderr}")
        return False
    except FileNotFoundError:
        print(
            "Error reading input: ffprobe or input file was not found. "
            f"FFPROBE_BIN={FFPROBE_BIN}"
        )
        return False
    except Exception as exc:
        print(f"Error reading input: {exc}")
        return False

    filters = []
    final_output_width = original_width
    final_output_height = original_height

    crop_factor = config.get("crop_factor", 1)
    if 0 < crop_factor < 1:
        final_output_width = int(original_width * crop_factor)
        final_output_height = int(original_height * crop_factor)
        x_offset_expr = f"(iw-{final_output_width})/2"
        y_offset_expr = f"(ih-{final_output_height})/2"
        filters.append(
            f"crop={final_output_width}:{final_output_height}:{x_offset_expr}:{y_offset_expr}"
        )

    rotate = config.get("rotate", 0)
    if abs(rotate) > 0.1:
        radians_angle = math.radians(abs(rotate))
        cos_a = math.cos(radians_angle)
        sin_a = math.sin(radians_angle)
        scale_factor = max(
            (final_output_width * abs(cos_a) + final_output_height * abs(sin_a))
            / final_output_width,
            (final_output_width * abs(sin_a) + final_output_height * abs(cos_a))
            / final_output_height,
        )
        filters.append(f"scale=iw*{scale_factor}:ih*{scale_factor}")
        filters.append(f"rotate={rotate}*PI/180")
        filters.append(f"crop={final_output_width}:{final_output_height}")

    speed = config.get("speed", 1.0)
    if speed > 0:
        filters.append(f"setpts=PTS/{speed}")

    brightness = max(-1.0, min(1.0, config.get("brightness", 0)))
    contrast = max(0.0, min(3.0, config.get("contrast", 1)))
    saturation = max(0.0, min(3.0, config.get("saturation", 1)))
    filters.append(f"eq=brightness={brightness}:saturation={saturation}:contrast={contrast}")

    lut_r = config.get("lut_r", 1)
    lut_g = config.get("lut_g", 1)
    lut_b = config.get("lut_b", 1)
    filters.append(f"lutrgb=r='val*{lut_r}':g='val*{lut_g}':b='val*{lut_b}'")
    filters.append("scale=ceil(iw/2)*2:ceil(ih/2)*2")

    filter_complex = ",".join(filters)

    try:
        stream = (
            ffmpeg.input(input_path)
            .output(
                output_path,
                vf=filter_complex,
                vcodec="libx264",
                crf=14,
                preset="slow",
                acodec="aac",
                profile="high",
                level="4.0",
                pix_fmt="yuv420p",
                audio_bitrate="192k",
                map_metadata="-1",
                metadata="encoder=",
                movflags="+faststart",
            )
            .overwrite_output()
        )
        stream.run(cmd=FFMPEG_BIN, capture_stdout=True, capture_stderr=True)

        if metadata and ExifTool is not None:
            with ExifTool() as et:
                args = ["-overwrite_original"]
                for tag, value in metadata.items():
                    args.append(f"-{tag}={value}")
                args.append(str(output_path))
                et.execute(*args)
        return True
    except ffmpeg.Error as exc:
        stderr = (
            exc.stderr.decode("utf-8", errors="replace")
            if getattr(exc, "stderr", None)
            else str(exc)
        )
        print(f"ffmpeg error while processing {output_path}:\n{stderr}")
        return False
    except FileNotFoundError:
        print(f"ffmpeg error: binary not found. FFMPEG_BIN={FFMPEG_BIN}")
        return False
    except Exception as exc:
        print(f"ffmpeg error: {exc}")
        traceback.print_exc()
        return False


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def jitter(base: float, delta: float, low: float, high: float) -> float:
    return clamp(base + random.uniform(-delta, delta), low, high)


def build_variants() -> list[dict]:
    # Keep adjustments mild to avoid strong distortion.
    profiles = [
        {"name": "neon_pulse", "brightness": 0.03, "contrast": 1.06, "saturation": 1.10, "speed": 1.02, "rotate": 0.6, "crop": 0.98, "lut_r": 1.04, "lut_g": 0.99, "lut_b": 1.03},
        {"name": "retro_recall", "brightness": -0.02, "contrast": 0.98, "saturation": 0.94, "speed": 0.99, "rotate": -0.4, "crop": 0.98, "lut_r": 1.02, "lut_g": 0.97, "lut_b": 0.95},
        {"name": "silent_luxury", "brightness": 0.02, "contrast": 1.03, "saturation": 0.97, "speed": 1.00, "rotate": 0.2, "crop": 0.99, "lut_r": 1.00, "lut_g": 1.01, "lut_b": 1.01},
        {"name": "kinetic_burst", "brightness": 0.01, "contrast": 1.08, "saturation": 1.08, "speed": 1.03, "rotate": 0.8, "crop": 0.97, "lut_r": 1.05, "lut_g": 1.01, "lut_b": 1.04},
    ]

    selected_profiles = random.sample(profiles, k=min(TARGET_VARIANTS, len(profiles)))
    variants = []
    for p in selected_profiles:
        config = {
            "crop_factor": jitter(p["crop"], 0.015, 0.95, 1.0),
            "rotate": jitter(p["rotate"], 0.7, -2.0, 2.0),
            "speed": jitter(p["speed"], 0.03, 0.94, 1.08),
            "brightness": jitter(p["brightness"], 0.035, -0.08, 0.08),
            "contrast": jitter(p["contrast"], 0.10, 0.9, 1.2),
            "saturation": jitter(p["saturation"], 0.12, 0.88, 1.22),
            "lut_r": jitter(p["lut_r"], 0.04, 0.94, 1.10),
            "lut_g": jitter(p["lut_g"], 0.04, 0.94, 1.08),
            "lut_b": jitter(p["lut_b"], 0.04, 0.94, 1.10),
        }
        variants.append({"name": p["name"], "config": config})

    return variants


def describe_variant_changes(config: dict) -> list[str]:
    speed = float(config.get("speed", 1))
    brightness = float(config.get("brightness", 0))
    contrast = float(config.get("contrast", 1))
    saturation = float(config.get("saturation", 1))
    rotate = float(config.get("rotate", 0))
    crop_factor = float(config.get("crop_factor", 1))
    lut_r = float(config.get("lut_r", 1))
    lut_g = float(config.get("lut_g", 1))
    lut_b = float(config.get("lut_b", 1))

    speed_delta = (speed - 1.0) * 100.0
    crop_percent = (1.0 - crop_factor) * 100.0

    return [
        f"Speed: {speed:.2f}x ({speed_delta:+.1f}%)",
        f"Brightness: {brightness:+.2f}",
        f"Contrast: {contrast:.2f}",
        f"Saturation: {saturation:.2f}",
        f"Rotacion: {rotate:+.2f}°",
        f"Crop: {crop_factor:.3f} ({crop_percent:.1f}% zoom)",
        (
            "LUT RGB: "
            f"R {lut_r:.2f} / "
            f"G {lut_g:.2f} / "
            f"B {lut_b:.2f}"
        ),
    ]


def describe_variant_changes(config: dict) -> list[str]:
    speed = float(config.get("speed", 1))
    brightness = float(config.get("brightness", 0))
    contrast = float(config.get("contrast", 1))
    saturation = float(config.get("saturation", 1))
    rotate = float(config.get("rotate", 0))
    crop_factor = float(config.get("crop_factor", 1))
    lut_r = float(config.get("lut_r", 1))
    lut_g = float(config.get("lut_g", 1))
    lut_b = float(config.get("lut_b", 1))

    speed_delta = (speed - 1.0) * 100.0
    crop_percent = (1.0 - crop_factor) * 100.0

    return [
        f"Speed: {speed:.2f}x ({speed_delta:+.1f}%)",
        f"Brightness: {brightness:+.2f}",
        f"Contrast: {contrast:.2f}",
        f"Saturation: {saturation:.2f}",
        f"Rotation: {rotate:+.2f} degrees",
        f"Crop: {crop_factor:.3f} ({crop_percent:.1f}% zoom)",
        f"LUT RGB: R {lut_r:.2f} / G {lut_g:.2f} / B {lut_b:.2f}",
    ]


def callback(ch: pika.adapters.blocking_connection.BlockingChannel, method, properties, body: bytes) -> None:
    job_id = None
    tmp_dir = None
    uploaded_dir = None
    try:
        payload = json.loads(body.decode("utf-8"))
        job_id = int(payload["job_id"])
        update_job(job_id, "analyzing", "Analyzing video...")

        source_path = payload.get("source_path")
        if source_path:
            if not os.path.exists(source_path):
                raise RuntimeError(f"Uploaded file not found: {source_path}")
            uploaded_dir = os.path.dirname(source_path)
        else:
            reel_url = payload.get("reel_url")
            if not reel_url:
                raise RuntimeError("Invalid payload: missing source_path or reel_url")
            tmp_dir = os.path.join(DATA_DIR, str(job_id))
            source_path = os.path.join(tmp_dir, "source.mp4")
            download_video(reel_url, source_path)

        variants = build_variants()
        out_dir = os.path.join(OUTPUTS_DIR, str(job_id))
        os.makedirs(out_dir, exist_ok=True)
        outputs = []

        total_variants = max(1, len(variants))
        for idx, variant in enumerate(variants, start=1):
            update_job(job_id, "creating", f"Creating new videos... ({idx}/{total_variants})")
            output_name = f"{variant['name']}.mp4"
            output_path = os.path.join(out_dir, output_name)
            ok = process_video(source_path, output_path, variant["config"], metadata=None)
            if not ok:
                raise RuntimeError(f"Failed to process variant {variant['name']}")
            outputs.append(
                {
                    "name": variant["name"],
                    "url": f"/outputs/{job_id}/{output_name}",
                    "changes": describe_variant_changes(variant["config"]),
                }
            )

        random.shuffle(outputs)
        update_job(job_id, "done", "Done", result_json=json.dumps(outputs), error_message=None)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        if job_id is not None:
            update_job(job_id, "error", "Processing error", error_message=str(exc))
        print(f"Error processing message: {exc}")
        traceback.print_exc()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        if tmp_dir and os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
        if uploaded_dir and os.path.exists(uploaded_dir):
            shutil.rmtree(uploaded_dir, ignore_errors=True)


def consume_from_queue() -> None:
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            credentials=credentials,
            heartbeat=30,
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RABBIT_QUEUE, on_message_callback=callback)
    print("Waiting for messages in queue...")
    channel.start_consuming()


def binary_exists(binary: str) -> bool:
    if os.path.isabs(binary):
        return os.path.isfile(binary)
    return which(binary) is not None


def check_binaries() -> None:
    ffmpeg_ok = binary_exists(FFMPEG_BIN)
    ffprobe_ok = binary_exists(FFPROBE_BIN)
    if not ffmpeg_ok or not ffprobe_ok:
        missing = []
        if not ffmpeg_ok:
            missing.append(f"ffmpeg ({FFMPEG_BIN})")
        if not ffprobe_ok:
            missing.append(f"ffprobe ({FFPROBE_BIN})")
        raise RuntimeError(
            "Missing required binaries: "
            + ", ".join(missing)
            + ". Install FFmpeg or set FFMPEG_BIN/FFPROBE_BIN in .env."
        )


if __name__ == "__main__":
    init_db()
    check_binaries()
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    consume_from_queue()
