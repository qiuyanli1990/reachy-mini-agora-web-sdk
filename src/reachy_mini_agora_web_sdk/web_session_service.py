"""Session/token/agent lifecycle service for web rtc server."""

from __future__ import annotations

import os
import json
import logging
import threading
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from reachy_mini_agora_web_sdk.agent_manager import AgentManager
from reachy_mini_agora_web_sdk.token_builder import TokenGenerator


logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


class WebSessionService:
    """Build Agora session payload and own auto-started agent lifecycle."""

    def __init__(self) -> None:
        self._agent_start_lock = threading.Lock()
        self._agent_started = False
        self._agent_manager: AgentManager | None = None
        self._last_session: dict[str, Any] = {}

    def _resolve_agent_config_path(self) -> Path:
        app_root = Path(__file__).resolve().parents[2]
        return app_root / "agent_config.json"

    def _resolve_agent_uid(self, agent_cfg: Path) -> int:
        try:
            data = agent_cfg.read_text(encoding="utf-8")
            payload = json.loads(data)
            props = payload.get("properties", {}) if isinstance(payload, dict) else {}
            uid = props.get("agent_rtc_uid")
            if uid is None:
                return 0
            return int(uid)
        except Exception:
            return 0

    def get_session_payload(self) -> dict[str, Any]:
        app_id = str(os.getenv("AGORA_APP_ID", "")).strip()
        if not app_id:
            raise HTTPException(status_code=400, detail="AGORA_APP_ID is required")

        channel_name = str(os.getenv("AGORA_CHANNEL_NAME", "reachy_conversation")).strip() or "reachy_conversation"
        uid = _env_int("AGORA_Reachy_mini_USER_ID", 0)
        if uid <= 0:
            raise HTTPException(status_code=400, detail="AGORA_Reachy_mini_USER_ID is required")

        generator = TokenGenerator(
            app_id=app_id,
            app_certificate=str(os.getenv("AGORA_APP_CERTIFICATE", "")),
        )
        token = ""
        if generator.is_certificate_enabled():
            token = generator.generate_token_for_user(channel_name=channel_name, uid=uid, expire_time=3600)
        agent_token = ""
        if generator.is_certificate_enabled():
            agent_uid = self._resolve_agent_uid(self._resolve_agent_config_path())
            agent_token = generator.generate_token_for_agent(
                channel_name=channel_name,
                agent_uid=agent_uid,
                expire_time=3600,
            )

        keywords_raw = str(os.getenv("AGORA_REACHY_DEVICE_KEYWORDS", "Reachy,USB,Pollen"))
        device_keywords = [s.strip() for s in keywords_raw.split(",") if s.strip()]
        strict_reachy_devices = _env_bool("AGORA_STRICT_REACHY_DEVICES", True)
        playback_volume = _env_float("PLAYBACK_VOLUME", 1.0)
        playback_volume = min(max(playback_volume, 0.0), 1.0)

        payload = {
            "appId": app_id,
            "channel": channel_name,
            "uid": uid,
            "token": token,
            "agentToken": agent_token,
            "strictReachyDevices": strict_reachy_devices,
            "deviceKeywords": device_keywords,
            "playbackVolume": playback_volume,
        }
        self._last_session = payload
        return payload

    def start_agent(self) -> dict[str, Any]:
        """Start agent after web client has joined RTC channel."""
        with self._agent_start_lock:
            if self._agent_started:
                return {"ok": True, "started": False, "reason": "already_running"}

            session = self._last_session or self.get_session_payload()
            app_id = str(session.get("appId", "")).strip()
            channel_name = str(session.get("channel", "")).strip()
            uid = int(session.get("uid", 0))
            agent_token = str(session.get("agentToken", ""))
            api_key = str(os.getenv("AGORA_API_KEY", "")).strip()
            api_secret = str(os.getenv("AGORA_API_SECRET", "")).strip()
            agent_cfg = self._resolve_agent_config_path()

            if not api_key or not api_secret:
                return {"ok": False, "started": False, "error": "AGORA_API_KEY/AGORA_API_SECRET missing"}
            if not agent_cfg.exists():
                return {"ok": False, "started": False, "error": f"missing {agent_cfg}"}

            manager = AgentManager(
                app_id=app_id,
                api_key=api_key,
                api_secret=api_secret,
                config_file=str(agent_cfg),
            )
            started = manager.start_agent_from_config(
                channel_name=channel_name,
                user_uid=uid,
                token=agent_token,
            )
            self._agent_started = bool(started)
            if self._agent_started:
                self._agent_manager = manager
                logger.info("Agent start succeeded (triggered after web join).")
                return {"ok": True, "started": True}
            return {"ok": False, "started": False, "error": "agent start failed"}

    def shutdown(self) -> None:
        if self._agent_manager is not None and self._agent_manager.is_agent_running():
            logger.info("Stopping auto-started agent during web server shutdown...")
            stopped = self._agent_manager.stop_agent()
            if stopped:
                logger.info("Auto-started agent stopped successfully.")
            else:
                logger.warning("Failed to stop auto-started agent during shutdown.")
        self._agent_started = False
        self._agent_manager = None
