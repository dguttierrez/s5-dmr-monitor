"""S5 DMR Monitor — BrandMeister Last Heard metadata collector."""
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import socketio
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("s5monitor")
DB = os.getenv("DATABASE_PATH", "/data/monitor.db")
GROUPS = set(int(v.strip()) for v in os.getenv("TALKGROUPS", "293,29320").split(",") if v.strip())
REFRESH = max(30, min(60, int(os.getenv("REFRESH_SECONDS", "45"))))
RETENTION = max(1, int(os.getenv("RETENTION_DAYS", "30")))
BM_URL = os.getenv("BM_URL", "https://api.brandmeister.network")
BM_PATH = os.getenv("BM_SOCKET_PATH", "lh/socket.io").lstrip("/")
status_lock = threading.Lock()
status = {"connected": False, "last_event_utc": None, "last_error": None}
shutdown = threading.Event()


def connect_db():
    con = sqlite3.connect(DB, timeout=20)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=20000")
    con.execute("PRAGMA journal_mode=WAL")
    return con


def prepare_db():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with connect_db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS transmissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg INTEGER NOT NULL, callsign TEXT NOT NULL,
            source_name TEXT, source_id INTEGER,
            repeater_id INTEGER, start_ts INTEGER NOT NULL,
            stop_ts INTEGER NOT NULL, duration INTEGER NOT NULL,
            UNIQUE(tg, source_id, repeater_id, start_ts, stop_ts, callsign)
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_transmissions_stop ON transmissions(stop_ts DESC)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_transmissions_tg_stop ON transmissions(tg,stop_ts DESC)")


def as_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def ingest(payload, now=None):
    """Store completed transmissions for configured talkgroups."""
    now = int(time.time()) if now is None else now
    try:
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            return False
        tg = as_int(payload.get("DestinationID"))
        if tg not in GROUPS or payload.get("Event") != "Session-Stop":
            return False
        call = str(payload.get("SourceCall") or "").strip().upper()[:24]
        start = as_int(payload.get("Start"))
        stop = as_int(payload.get("Stop"))
        if not call or start <= 0 or stop < start or stop > now + 120 or stop < now - 3600:
            return False
        duration = stop - start
        if duration > 3600:
            return False
        source_id = as_int(payload.get("SourceID"))
        repeater_id = as_int(payload.get("RepeaterID") or payload.get("Repeater"))
        source_name = str(payload.get("SourceName") or "")[:120]
        with connect_db() as con:
            cur = con.execute("""INSERT OR IGNORE INTO transmissions
                (tg,callsign,source_name,source_id,repeater_id,start_ts,stop_ts,duration)
                VALUES (?,?,?,?,?,?,?,?)""",
                (tg,call,source_name,source_id,repeater_id,start,stop,duration))
        return cur.rowcount == 1
    except (ValueError, TypeError, sqlite3.Error) as ex:
        log.warning("Rejected incoming event: %s", ex)
        return False


def listener():
    sio = socketio.Client(reconnection=False, logger=False, engineio_logger=False, request_timeout=15)

    @sio.event
    def connect():
        with status_lock:
            status["connected"] = True
            status["last_error"] = None
        log.info("Connected to BrandMeister Last Heard")
        sio.emit("join", "everything")

    @sio.event
    def disconnect():
        with status_lock:
            status["connected"] = False
        log.warning("BrandMeister Last Heard disconnected")

    @sio.on("mqtt")
    def on_mqtt(data):
        try:
            payload = data.get("payload") if isinstance(data, dict) else data
            with status_lock:
                status["last_event_utc"] = datetime.now(timezone.utc).isoformat()
            ingest(payload)
        except Exception:
            log.exception("Failed handling MQTT event")

    while not shutdown.is_set():
        try:
            sio.connect(BM_URL, socketio_path=BM_PATH, transports=["websocket"], wait_timeout=20)
            while sio.connected and not shutdown.wait(1):
                pass
        except Exception as ex:
            with status_lock:
                status["connected"] = False
                status["last_error"] = str(ex)[:200]
            log.warning("BrandMeister connection failed: %s", ex)
        finally:
            try:
                sio.disconnect()
            except Exception:
                pass
        shutdown.wait(15)


def janitor():
    while not shutdown.wait(3600):
        try:
            with connect_db() as con:
                con.execute("DELETE FROM transmissions WHERE stop_ts < ?", (int(time.time()) - RETENTION * 86400,))
        except sqlite3.Error:
            log.exception("Retention cleanup failed")


@asynccontextmanager
async def lifespan(app):
    prepare_db()
    shutdown.clear()
    threading.Thread(target=listener, name="brandmeister", daemon=True).start()
    threading.Thread(target=janitor, name="retention", daemon=True).start()
    yield
    shutdown.set()


app = FastAPI(title="S5 DMR Monitor", lifespan=lifespan, docs_url=None, redoc_url=None)


@app.get("/")
def homepage():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/status")
def api_status():
    with status_lock:
        s = status.copy()
    return {**s, "talkgroups": sorted(GROUPS), "refresh_seconds": REFRESH,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()}


@app.get("/api/activity")
def api_activity(tg: int | None = None, limit: int = Query(100, ge=1, le=500)):
    if tg is not None and tg not in GROUPS:
        return {"items": [], "total": 0}
    sql = "SELECT tg,callsign,source_name,source_id,repeater_id,start_ts,stop_ts,duration FROM transmissions"
    args = []
    if tg is not None:
        sql += " WHERE tg=?"
        args.append(tg)
    sql += " ORDER BY stop_ts DESC,id DESC LIMIT ?"
    args.append(limit)
    with connect_db() as con:
        rows = [dict(r) for r in con.execute(sql, args)]
    return {"items": rows, "total": len(rows)}


@app.get("/api/stats")
def api_stats():
    since = int(time.time()) - 86400
    with connect_db() as con:
        total = con.execute("SELECT COUNT(*) FROM transmissions WHERE stop_ts>=?", (since,)).fetchone()[0]
        breakdown = [dict(r) for r in con.execute(
            "SELECT tg,COUNT(*) AS transmissions,COUNT(DISTINCT callsign) AS operators,"
            "COALESCE(SUM(duration),0) AS seconds FROM transmissions WHERE stop_ts>=? GROUP BY tg ORDER BY tg", (since,))]
    return {"last_24h": total, "by_tg": breakdown}
