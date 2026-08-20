"""Azure relay server.

Mobile phones connect to this server on Azure Container Instances.
The local simulation (server.py) connects outbound to /source.
All vehicle logic stays on the local machine — this server is a pure
message router with no knowledge of vehicle data.

Environment variables:
    RELAY_SECRET   Shared secret that local server must supply (default: demo-secret-change-me)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

log = logging.getLogger("relay")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SECRET = os.environ.get("RELAY_SECRET", "demo-secret-change-me")
MOBILE_HTML_PATH = Path(__file__).parent / "mobile.html"
# "ev" is a synthetic pseudo-type routed to the simulation, which tracks any
# electric vehicle regardless of its real body type.
_VALID_VTYPES = {"2wheeler", "car", "bus", "truck", "person", "ev"}

app = FastAPI(title="Mobile Relay")

# ── Global state ─────────────────────────────────────────────────────────────
# Only one source (local simulation) is expected at a time.
_source_ws: WebSocket | None = None
# client_id → WebSocket for each connected phone.
_mobile_clients: dict[str, WebSocket] = {}
# client_id → {"vtype", "color"} so we can re-announce phones to the source
# whenever the simulation (re)connects.
_mobile_meta: dict[str, dict] = {}
# Seconds between app-level keepalive pings sent to the source. Must be well
# below the simulation's 120s idle watchdog so the connection never churns.
_KEEPALIVE_SECONDS = 30


async def _to_source(msg: dict) -> None:
    """Forward a message to the local simulation. Silently drops if not connected."""
    if _source_ws is not None:
        try:
            await _source_ws.send_json(msg)
        except Exception:
            pass


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "source_connected": _source_ws is not None,
        "mobile_clients": len(_mobile_clients),
    }


# ── Serve mobile.html ─────────────────────────────────────────────────────────
@app.get("/mobile/{vehicle_type}")
async def serve_mobile(vehicle_type: str):
    if vehicle_type not in _VALID_VTYPES:
        return HTMLResponse("Not found", status_code=404)
    html = MOBILE_HTML_PATH.read_text(encoding="utf-8")
    html = html.replace("__VEHICLE_TYPE__", vehicle_type)
    return HTMLResponse(html)


# ── Source WebSocket (/source?secret=...) ─────────────────────────────────────
@app.websocket("/source")
async def source_endpoint(ws: WebSocket, secret: str = ""):
    global _source_ws
    if secret != SECRET:
        await ws.close(code=4001)
        log.warning("Source connection rejected: wrong secret")
        return

    await ws.accept()
    _source_ws = ws
    log.info("Local simulation connected (%d mobile clients already waiting)",
             len(_mobile_clients))

    # Re-announce every phone that is already connected so the simulation
    # rebuilds their sessions after any (re)connect. Without this, phones that
    # were connected before the source reconnected would freeze forever.
    for cid, meta in list(_mobile_meta.items()):
        await _to_source({
            "event": "new_client",
            "client_id": cid,
            "vtype": meta.get("vtype", "car"),
            "color": meta.get("color", "#e74c3c"),
        })

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            # msg: {"to": "<client_id>" | "*", "data": {...}}
            target = msg.get("to")
            data_str = json.dumps(msg.get("data", {}), default=str)

            if target == "*":
                dead = []
                for cid, cws in list(_mobile_clients.items()):
                    try:
                        await cws.send_text(data_str)
                    except Exception:
                        dead.append(cid)
                for cid in dead:
                    _mobile_clients.pop(cid, None)

            elif target and target in _mobile_clients:
                try:
                    await _mobile_clients[target].send_text(data_str)
                except Exception:
                    _mobile_clients.pop(target, None)

    except (WebSocketDisconnect, Exception) as e:
        log.info("Local simulation disconnected: %s", e)
    finally:
        _source_ws = None


# ── Mobile WebSocket (/ws/mobile/{type}?color=...) ────────────────────────────
@app.websocket("/ws/mobile/{vehicle_type}")
async def mobile_endpoint(ws: WebSocket, vehicle_type: str, color: str = "#e74c3c"):
    if vehicle_type not in _VALID_VTYPES:
        await ws.close(code=1008)
        return

    client_id = str(uuid.uuid4())
    await ws.accept()
    _mobile_clients[client_id] = ws
    _mobile_meta[client_id] = {"vtype": vehicle_type, "color": color}
    log.info("Mobile %s connected type=%s color=%s (%d total)",
             client_id[:8], vehicle_type, color, len(_mobile_clients))

    # Tell the local simulation a new phone has arrived
    await _to_source({
        "event": "new_client",
        "client_id": client_id,
        "vtype": vehicle_type,
        "color": color,
    })

    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                continue
            # Forward phone messages (accidents, etc.) to local simulation
            await _to_source({
                "event": "client_msg",
                "client_id": client_id,
                "data": data,
            })
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _mobile_clients.pop(client_id, None)
        _mobile_meta.pop(client_id, None)
        await _to_source({
            "event": "client_disconnected",
            "client_id": client_id,
        })
        log.info("Mobile %s disconnected (%d remaining)",
                 client_id[:8], len(_mobile_clients))


# ── Keepalive ───────────────────────────────────────────────────────────
def _register_keepalive() -> None:
    async def _keepalive_loop():
        while True:
            await asyncio.sleep(_KEEPALIVE_SECONDS)
            # A real message on the wire resets the simulation's idle watchdog,
            # keeping the source connection stable so phones never get dropped.
            await _to_source({"event": "ping"})

    @app.on_event("startup")
    async def _start_keepalive():
        asyncio.ensure_future(_keepalive_loop())


_register_keepalive()
