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
HISTORY_INTERVAL = max(120, int(os.getenv("HISTORY_INTERVAL_SECONDS", "180")))
HISTORY_AMOUNT = max(10, min(200, int(os.getenv("HISTORY_AMOUNT", "100"))))
WATCH_CALL = os.getenv("WATCH_CALL", "S51DG").strip().upper()
BM_URL = os.getenv("BM_URL", "https://api.brandmeister.network")
BM_PATH = os.getenv("BM_SOCKET_PATH", "lh/socket.io").lstrip("/")
status_lock = threading.Lock()
status = {"connected": False, "last_event_utc": None, "last_error": None,
          "events_received": 0, "events_target_tg": 0, "events_saved": 0,
          "last_target_event": None, "last_rejection": None,
          "sample_event": None, "destination_samples": [], "parse_errors": 0,
          "destination_counts": {}, "event_counts": {}, "last_target_seen_utc": None,
          "history_queries_sent": 0, "last_history_query_utc": None,
          "watch_call": WATCH_CALL, "watch_call_events": 0, "watch_call_recent": [],
          "mqtt_topics": {}, "recent_target_events": [],
          "filtered_connections": {str(tg): {"connected": False, "messages": 0, "matches": 0,
              "last_topic": None, "last_destination": None, "last_callsign": None, "history_queries": 0,
              "last_event_utc": None, "last_error": None} for tg in GROUPS}}
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
        if isinstance(payload, dict) and "payload" in payload and "DestinationID" not in payload:
            payload = payload["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
        if not isinstance(payload, dict):
            return False
        tg = as_int(payload.get("DestinationID"))
        if tg not in GROUPS:
            return False
        with status_lock:
            status["events_target_tg"] += 1
            status["last_target_seen_utc"] = datetime.now(timezone.utc).isoformat()
            status["last_target_event"] = {"tg": tg, "event": str(payload.get("Event")),
                                            "start": payload.get("Start"), "stop": payload.get("Stop")}
            items = status["recent_target_events"]
            items.append({"call": str(payload.get("SourceCall")), "tg": tg,
                          "event": str(payload.get("Event")), "start": payload.get("Start"),
                          "stop": payload.get("Stop")})
            del items[:-15]
        # History replies may carry LH-Startup or Session-Update rather than Session-Stop.
        # A strictly positive Stop timestamp determines whether the QSO is complete.
        if as_int(payload.get("Stop")) <= 0:
            return False
        call = str(payload.get("SourceCall") or "").strip().upper()[:24]
        start = as_int(payload.get("Start"))
        stop = as_int(payload.get("Stop"))
        if not call or start <= 0 or stop < start or stop > now + 120 or stop < now - RETENTION * 86400:
            with status_lock:
                status["last_rejection"] = f"Invalid timestamps/call: call={bool(call)}, start={start}, stop={stop}, now={now}"
            return False
        duration = stop - start
        if duration > 3600:
            with status_lock:
                status["last_rejection"] = f"Duration out of range: {duration}"
            return False
        source_id = as_int(payload.get("SourceID"))
        repeater_id = as_int(payload.get("RepeaterID") or payload.get("Repeater"))
        source_name = str(payload.get("SourceName") or "")[:120]
        with connect_db() as con:
            cur = con.execute("""INSERT OR IGNORE INTO transmissions
                (tg,callsign,source_name,source_id,repeater_id,start_ts,stop_ts,duration)
                VALUES (?,?,?,?,?,?,?,?)""",
                (tg,call,source_name,source_id,repeater_id,start,stop,duration))
        if cur.rowcount:
            with status_lock:
                status["events_saved"] += 1
                status["last_rejection"] = None
            log.info("Saved TG %s from %s (%ss)", tg, call, duration)
        return cur.rowcount == 1
    except (ValueError, TypeError, sqlite3.Error) as ex:
        with status_lock:
            status["last_rejection"] = str(ex)[:160]
        log.warning("Rejected incoming event: %s", ex)
        return False


def request_history(sio):
    # BrandMeister's own Last Heard dashboard issues searchHistory with a query object.
    for tg in sorted(GROUPS):
        sio.emit("searchHistory", {"query": {"DestinationID": tg}, "amount": HISTORY_AMOUNT})
    if WATCH_CALL:
        sio.emit("searchHistory", {"query": {"SourceCall": WATCH_CALL}, "amount": HISTORY_AMOUNT})
    with status_lock:
        status["history_queries_sent"] += len(GROUPS) + bool(WATCH_CALL)
        status["last_history_query_utc"] = datetime.now(timezone.utc).isoformat()
    log.info("Requested filtered Last Heard history: groups=%s call=%s amount=%d", sorted(GROUPS), WATCH_CALL, HISTORY_AMOUNT)


def listener():
    sio = socketio.Client(reconnection=False, logger=False, engineio_logger=False, request_timeout=15)

    @sio.event
    def connect():
        with status_lock:
            status["connected"] = True
            status["last_error"] = None
        log.info("Connected to BrandMeister Last Heard")
        sio.emit("join", "everything")
        log.info("Subscribed to Last Heard (join/everything)")

    @sio.event
    def disconnect():
        with status_lock:
            status["connected"] = False
        log.warning("BrandMeister Last Heard disconnected")

    @sio.on("mqtt")
    def on_mqtt(data):
        try:
            payload = data.get("payload") if isinstance(data, dict) else data
            topic = str(data.get("topic", "<missing>")) if isinstance(data, dict) else "<not-dict>"
            with status_lock:
                topics = status["mqtt_topics"]
                topics[topic] = topics.get(topic, 0) + 1
                status["last_event_utc"] = datetime.now(timezone.utc).isoformat()
                status["events_received"] += 1
            try:
                decoded = json.loads(payload) if isinstance(payload, str) else payload
                if isinstance(decoded, dict) and "payload" in decoded and "DestinationID" not in decoded:
                    decoded = decoded["payload"]
                    decoded = json.loads(decoded) if isinstance(decoded, str) else decoded
                if isinstance(decoded, dict):
                    with status_lock:
                        if status["sample_event"] is None:
                            status["sample_event"] = {k: str(v)[:120] for k, v in decoded.items()}
                            log.info("First Last Heard payload fields: %s", list(decoded))
                        dst = str(decoded.get("DestinationID", "<missing>"))
                        counts = status["destination_counts"]
                        counts[dst] = counts.get(dst, 0) + 1
                        kind = str(decoded.get("Event", "<missing>"))
                        events = status["event_counts"]
                        events[kind] = events.get(kind, 0) + 1
                        if dst not in status["destination_samples"] and len(status["destination_samples"]) < 25:
                            status["destination_samples"].append(dst)
                        if str(decoded.get("SourceCall", "")).upper() == WATCH_CALL:
                            status["watch_call_events"] += 1
                            recent = status["watch_call_recent"]
                            recent.append({"tg": dst, "event": kind, "start": decoded.get("Start"),
                                           "stop": decoded.get("Stop"), "topic": topic})
                            del recent[:-15]
                    ingest(decoded)
                else:
                    with status_lock:
                        status["parse_errors"] += 1
                    log.warning("Unexpected mqtt decoded type: %s", type(decoded).__name__)
            except (ValueError, TypeError) as ex:
                with status_lock:
                    status["parse_errors"] += 1
                log.warning("Invalid mqtt JSON: %s", ex)
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



def filtered_listener(tg):
    """One independent Socket.IO connection per talkgroup, as in the LH UI."""
    sio = socketio.Client(reconnection=False, logger=False, engineio_logger=False, request_timeout=15)
    key = str(tg)

    @sio.event
    def connect():
        with status_lock:
            status["filtered_connections"][key]["connected"] = True
            status["filtered_connections"][key]["last_error"] = None
        # Subscribe to live traffic and query history; inspect both separately.
        sio.emit("join", "everything")
        sio.emit("searchHistory", {"query": {"DestinationID": str(tg)}, "amount": HISTORY_AMOUNT})
        with status_lock:
            status["filtered_connections"][key]["history_queries"] += 1
        log.info("TG %s dedicated Last Heard searchHistory request sent", tg)

    @sio.event
    def disconnect():
        with status_lock:
            status["filtered_connections"][key]["connected"] = False

    @sio.on("mqtt")
    def filtered_mqtt(data):
        try:
            payload = data.get("payload") if isinstance(data, dict) else data
            event = json.loads(payload) if isinstance(payload, str) else payload
            if isinstance(event, dict) and "payload" in event and "DestinationID" not in event:
                event = event["payload"]
                event = json.loads(event) if isinstance(event, str) else event
            with status_lock:
                stats = status["filtered_connections"][key]
                stats["messages"] += 1
                stats["last_topic"] = str(data.get("topic", "")) if isinstance(data, dict) else ""
                stats["last_event_utc"] = datetime.now(timezone.utc).isoformat()
                if isinstance(event, dict):
                    seen_tg = str(event.get("DestinationID", "<missing>"))
                    stats["last_destination"] = seen_tg
                    stats["last_callsign"] = str(event.get("SourceCall", ""))[:24]
                    if as_int(event.get("DestinationID")) == tg:
                        stats["matches"] += 1
            if isinstance(event, dict):
                ingest(event)
        except (ValueError, TypeError, sqlite3.Error):
            log.exception("Invalid TG %s filtered message", tg)

    while not shutdown.is_set():
        try:
            sio.connect(BM_URL, socketio_path=BM_PATH, transports=["websocket"], wait_timeout=20)
            # Reconnect periodically, refreshing the filtered history request.
            deadline = time.monotonic() + HISTORY_INTERVAL
            while sio.connected and not shutdown.wait(1) and time.monotonic() < deadline:
                pass
        except Exception as exc:
            with status_lock:
                status["filtered_connections"][key]["last_error"] = str(exc)[:200]
            log.warning("TG %s filtered connection failure: %s", tg, exc)
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
    for tg in GROUPS:
        threading.Thread(target=filtered_listener, args=(tg,), name=f"tg-{tg}", daemon=True).start()
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
    counts = s.pop("destination_counts", {})
    s["top_destinations"] = [{"tg": k, "count": v} for k, v in sorted(counts.items(), key=lambda x: -x[1])[:25]]
    s["configured_tg_event_counts"] = {str(g): counts.get(str(g), 0) for g in sorted(GROUPS)}
    s["distinct_destinations"] = len(counts)
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
