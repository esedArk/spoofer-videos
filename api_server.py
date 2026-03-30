import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone

import pika
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from werkzeug.utils import secure_filename

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jobs.db")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")
DATA_DIR = os.path.join(BASE_DIR, "data")

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "guest")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "reel_jobs")

app = Flask(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
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


def db_execute(query: str, params: tuple = ()) -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def db_fetchall(query: str, params: tuple = ()) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def delete_job_files(job_id: int) -> None:
    for root in (OUTPUTS_DIR, UPLOADS_DIR, DATA_DIR):
        path = os.path.join(root, str(job_id))
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)


def enqueue_job(payload: dict) -> None:
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            credentials=credentials,
            heartbeat=30,
        )
    )
    try:
        channel = connection.channel()
        channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
        body = json.dumps(payload)
        channel.basic_publish(
            exchange="",
            routing_key=RABBIT_QUEUE,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        connection.close()


def serialize_job(row: dict) -> dict:
    outputs = []
    if row.get("result_json"):
        try:
            outputs = json.loads(row["result_json"])
        except json.JSONDecodeError:
            outputs = []
    return {
        "id": row["id"],
        "reel_url": row["reel_url"],
        "status": row["status"],
        "phase": row["phase"],
        "outputs": outputs,
        "error_message": row["error_message"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


@app.get("/")
def root() -> any:
    return send_from_directory(BASE_DIR, "index.html")


@app.get("/<path:path>")
def static_file(path: str) -> any:
    return send_from_directory(BASE_DIR, path)


@app.post("/api/jobs")
def create_job() -> any:
    payload = request.get_json(silent=True) or {}
    reel_url = (payload.get("url") or "").strip()
    if not reel_url:
        return jsonify({"ok": False, "error": "url is required"}), 400

    now = now_iso()
    job_id = db_execute(
        """
        INSERT INTO jobs (reel_url, status, phase, result_json, error_message, created_at, updated_at)
        VALUES (?, 'queued', 'Queued', NULL, NULL, ?, ?)
        """,
        (reel_url, now, now),
    )

    try:
        enqueue_job({"job_id": job_id, "reel_url": reel_url})
    except Exception as exc:
        db_execute(
            """
            UPDATE jobs
            SET status='error', phase='Queue error', error_message=?, updated_at=?
            WHERE id=?
            """,
            (str(exc), now_iso(), job_id),
        )
        return jsonify({"ok": False, "error": "queue unavailable", "details": str(exc)}), 503

    rows = db_fetchall("SELECT * FROM jobs WHERE id=?", (job_id,))
    return jsonify({"ok": True, "job": serialize_job(rows[0])})


@app.post("/api/jobs/upload")
def create_upload_job() -> any:
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "file is required"}), 400

    file = request.files["file"]
    if not file or not file.filename:
        return jsonify({"ok": False, "error": "empty filename"}), 400

    safe_name = secure_filename(file.filename)
    if not safe_name:
        return jsonify({"ok": False, "error": "invalid filename"}), 400

    ext = os.path.splitext(safe_name)[1].lower()
    allowed = {".mp4", ".mov", ".webm", ".mkv"}
    if ext not in allowed:
        return jsonify({"ok": False, "error": "unsupported file type"}), 400

    now = now_iso()
    source_label = f"upload:{safe_name}"
    job_id = db_execute(
        """
        INSERT INTO jobs (reel_url, status, phase, result_json, error_message, created_at, updated_at)
        VALUES (?, 'queued', 'Queued', NULL, NULL, ?, ?)
        """,
        (source_label, now, now),
    )

    job_upload_dir = os.path.join(UPLOADS_DIR, str(job_id))
    os.makedirs(job_upload_dir, exist_ok=True)
    source_path = os.path.join(job_upload_dir, f"source{ext}")
    file.save(source_path)

    try:
        enqueue_job({"job_id": job_id, "source_path": source_path, "source_name": safe_name})
    except Exception as exc:
        db_execute(
            """
            UPDATE jobs
            SET status='error', phase='Queue error', error_message=?, updated_at=?
            WHERE id=?
            """,
            (str(exc), now_iso(), job_id),
        )
        return jsonify({"ok": False, "error": "queue unavailable", "details": str(exc)}), 503

    rows = db_fetchall("SELECT * FROM jobs WHERE id=?", (job_id,))
    return jsonify({"ok": True, "job": serialize_job(rows[0])})


@app.get("/api/jobs/recent")
def recent_jobs() -> any:
    try:
        limit = int(request.args.get("limit", "10"))
    except ValueError:
        limit = 10
    limit = max(1, min(limit, 50))
    rows = db_fetchall("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,))
    return jsonify({"ok": True, "jobs": [serialize_job(row) for row in rows]})


@app.get("/api/jobs/<int:job_id>")
def get_job(job_id: int) -> any:
    rows = db_fetchall("SELECT * FROM jobs WHERE id=?", (job_id,))
    if not rows:
        return jsonify({"ok": False, "error": "job not found"}), 404
    return jsonify({"ok": True, "job": serialize_job(rows[0])})


@app.delete("/api/jobs/<int:job_id>")
def delete_job(job_id: int) -> any:
    rows = db_fetchall("SELECT * FROM jobs WHERE id=?", (job_id,))
    if not rows:
        return jsonify({"ok": False, "error": "job not found"}), 404

    job = rows[0]
    if job["status"] in ("queued", "analyzing", "creating"):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "job is in progress and cannot be deleted yet",
                }
            ),
            409,
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()
    finally:
        conn.close()

    delete_job_files(job_id)
    return jsonify({"ok": True, "deleted_job_id": job_id})


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=False)
