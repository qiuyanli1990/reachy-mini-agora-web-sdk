"""Datastream decode and local tool dispatch for web rtc server."""

from __future__ import annotations

import json
import base64
import logging
from typing import Any, Protocol

from reachy_mini_agora_web_sdk.tools.core_tools import dispatch_tool_call


logger = logging.getLogger(__name__)


class MotionBridgeLike(Protocol):
    """Minimal bridge interface required by datastream processor."""

    def update_conversation_state(self, state: str) -> None: ...

    def get_tool_deps(self) -> Any | None: ...


class WebDatastreamProcessor:
    """Decode packed datastream and dispatch local actions."""

    def __init__(self, bridge: MotionBridgeLike) -> None:
        self._bridge = bridge

    def _decode_packed_datastream_text(self, text: str) -> dict[str, Any] | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        parts = raw.split("|")
        if len(parts) < 4:
            return None
        b64_payload = parts[-1]
        try:
            decoded = base64.b64decode(b64_payload)
            payload = json.loads(decoded.decode("utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            return None
        return None

    def _coerce_action_payload(self, content: Any) -> dict[str, Any] | None:
        payload: Any = content
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return None

        if not isinstance(payload, dict):
            return None

        wrapped_content = payload.get("content")
        if "action_type" not in payload and wrapped_content is not None:
            if isinstance(wrapped_content, str):
                try:
                    unwrapped = json.loads(wrapped_content)
                    if isinstance(unwrapped, dict):
                        payload = unwrapped
                except json.JSONDecodeError:
                    return None
            elif isinstance(wrapped_content, dict):
                payload = wrapped_content

        if not isinstance(payload, dict):
            return None
        if not str(payload.get("action_type", "")).strip():
            return None
        return payload

    def _map_action_to_tool(self, action_type: str, payload: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
        if action_type in {"display_emotion", "play_emotion", "emotion"}:
            emotion = payload.get("emotion_type") or payload.get("emotion")
            if emotion:
                return "play_emotion", {"emotion": str(emotion)}
            return None, {}

        if action_type == "move_head":
            direction = payload.get("direction")
            if direction:
                return "move_head", {"direction": str(direction)}
            return None, {}

        if action_type == "dance":
            return "dance", {"move": payload.get("move", "random"), "repeat": int(payload.get("repeat", 1))}

        if action_type == "stop_dance":
            return "stop_dance", {}

        if action_type == "stop_emotion":
            return "stop_emotion", {"dummy": True}

        if action_type == "head_tracking":
            return "head_tracking", {"enabled": bool(payload.get("enabled", True))}

        return None, {}

    async def _dispatch_action_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        action_type = str(payload.get("action_type", "")).strip()
        tool_name, tool_args = self._map_action_to_tool(action_type, payload)
        if tool_name is None:
            return {"ok": False, "error": f"unsupported action_type: {action_type}"}

        deps = self._bridge.get_tool_deps()
        if deps is None:
            return {"ok": False, "error": "tool deps unavailable (robot bridge not started)"}

        logger.info(
            "WEB_TOOL_TRACE dispatching tool=%s args=%s from action_type=%s",
            tool_name,
            json.dumps(tool_args, ensure_ascii=False),
            action_type,
        )
        result = await dispatch_tool_call(tool_name, json.dumps(tool_args), deps)
        if isinstance(result, dict) and result.get("error"):
            logger.warning("WEB_TOOL_TRACE tool execution failed: tool=%s error=%s", tool_name, result.get("error"))
            return {"ok": False, "tool": tool_name, "result": result}
        logger.info("WEB_TOOL_TRACE tool execution succeeded: tool=%s result=%s", tool_name, result)
        return {"ok": True, "tool": tool_name, "result": result}

    async def process(self, text: str, json_data: dict[str, Any] | None) -> dict[str, Any]:
        decoded_payload = None
        if text:
            decoded_payload = self._decode_packed_datastream_text(text)
            if decoded_payload is not None:
                if decoded_payload.get("object") == "message.state":
                    state = str(decoded_payload.get("state", "")).strip().lower()
                    if state:
                        self._bridge.update_conversation_state(state)
                if decoded_payload.get("object") == "message.user":
                    content = decoded_payload.get("content")
                    if isinstance(content, str):
                        try:
                            content_obj = json.loads(content)
                            logger.info(
                                "WEB_DATASTREAM_MESSAGE_USER: %s",
                                json.dumps(content_obj, ensure_ascii=False),
                            )
                        except Exception:
                            logger.info("WEB_DATASTREAM_MESSAGE_USER: %s", content)

        action_source: Any = json_data
        if action_source is None and decoded_payload is not None:
            action_source = decoded_payload
        if isinstance(action_source, dict) and action_source.get("object") == "message.state":
            state = str(action_source.get("state", "")).strip().lower()
            if state:
                self._bridge.update_conversation_state(state)
        if action_source is None:
            action_source = text

        action_payload = self._coerce_action_payload(action_source)
        if action_payload is None:
            return {"ok": True, "dispatched": False}
        result = await self._dispatch_action_payload(action_payload)
        return {"ok": True, "dispatched": True, "dispatchResult": result}
