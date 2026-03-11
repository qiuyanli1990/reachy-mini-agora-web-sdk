"""Web server for Agora Web SDK RTC mode (Reachy devices only)."""

from __future__ import annotations
import os
import logging
import threading
import webbrowser
import urllib.request
from typing import Any
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from pydantic import Field, BaseModel, ConfigDict
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from reachy_mini_agora_web_sdk.web_motion_bridge import (
    AudioChunkPayload,
    SessionStatePayload,
    WebSpeechMotionBridge,
)
from reachy_mini_agora_web_sdk.web_datastream_processor import WebDatastreamProcessor
from reachy_mini_agora_web_sdk.web_session_service import WebSessionService


logger = logging.getLogger(__name__)


class DatastreamMessagePayload(BaseModel):
    """Datastream message payload forwarded by web frontend."""

    model_config = ConfigDict(populate_by_name=True)
    uid: int | str
    streamId: int
    text: str = ""
    json_data: dict[str, Any] | None = Field(default=None, alias="json")
    ts: int | None = None


_speech_motion_bridge = WebSpeechMotionBridge()
_datastream_processor = WebDatastreamProcessor(_speech_motion_bridge)
_session_service = WebSessionService()

def _set_daemon_motor_mode(mode: str) -> bool:
    """Best-effort daemon motor mode switch."""
    req = urllib.request.Request(
        f"http://localhost:8000/api/motors/set_mode/{mode}",
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=0.8):
            return True
    except Exception:
        return False


def create_web_rtc_app() -> FastAPI:
    """Create FastAPI app that serves static WebRTC UI + Agora session config."""
    app_root = Path(__file__).resolve().parents[2]
    static_root = app_root / "static" / "web_rtc"
    if not static_root.exists():
        raise FileNotFoundError(f"Static directory not found: {static_root}")

    app = FastAPI(title="Reachy Mini Agora WebRTC Server")
    app.mount("/static", StaticFiles(directory=str(static_root)), name="static")

    @app.get("/api/agora/session")
    def get_agora_session() -> dict[str, Any]:
        return _session_service.get_session_payload()

    @app.post("/api/agora/agent/start")
    def start_agora_agent() -> dict[str, Any]:
        return _session_service.start_agent()

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(str(static_root / "index.html"))

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/motion/session")
    def motion_session(payload: SessionStatePayload) -> dict[str, Any]:
        _speech_motion_bridge.update_session_state(payload.active)
        return {"ok": True, "enabled": _speech_motion_bridge.enabled}

    @app.post("/api/motion/audio-chunk")
    def motion_audio_chunk(payload: AudioChunkPayload) -> dict[str, Any]:
        if payload.pcm_b64:
            _speech_motion_bridge.feed_audio_chunk(
                payload.pcm_b64,
                payload.level,
                payload.sample_rate,
            )
        return {"ok": True, "enabled": _speech_motion_bridge.enabled}

    @app.post("/api/datastream/message")
    async def datastream_message(payload: DatastreamMessagePayload) -> dict[str, Any]:
        logger.debug(
            "WEB_DATASTREAM uid=%s streamId=%s text=%s json=%s",
            payload.uid,
            payload.streamId,
            payload.text,
            payload.json_data,
        )
        return await _datastream_processor.process(payload.text, payload.json_data)

    @app.on_event("shutdown")
    def on_shutdown() -> None:
        _session_service.shutdown()
        _speech_motion_bridge.stop()

    return app


def run_web_rtc_server() -> None:
    """Run the FastAPI server hosting the WebRTC UI and token endpoint."""
    host = str(os.getenv("AGORA_WEB_SERVER_HOST", "0.0.0.0")).strip() or "0.0.0.0"
    port = int(str(os.getenv("AGORA_WEB_SERVER_PORT", "8780")).strip() or "8780")
    auto_open_browser = str(os.getenv("AGORA_AUTO_OPEN_BROWSER", "true")).strip().lower() in {"1", "true", "yes", "on"}
    url = f"http://localhost:{port}"

    if auto_open_browser:
        def _open_browser() -> None:
            try:
                opened = webbrowser.open(url, new=1, autoraise=True)
                if opened:
                    logger.info("Opened browser: %s", url)
                else:
                    logger.warning("Browser did not auto-open. Open manually: %s", url)
            except Exception as exc:
                logger.warning("Failed to auto-open browser: %s. URL: %s", exc, url)

        threading.Timer(1.0, _open_browser).start()

    if _set_daemon_motor_mode("enabled"):
        logger.info("Daemon motor mode set to enabled (web-rtc-server startup).")
    else:
        logger.warning("Could not auto-enable daemon motor mode at startup.")

    logger.info("Starting WebRTC server at http://%s:%s", host, port)
    uvicorn.run(
        create_web_rtc_app(),
        host=host,
        port=port,
        log_level="info",
        access_log=False,
    )
