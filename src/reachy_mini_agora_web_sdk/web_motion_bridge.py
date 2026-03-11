"""Web RTC speech motion bridge for Reachy."""

from __future__ import annotations

import os
import time
import logging
import threading
import base64

from pydantic import BaseModel
import numpy as np

from reachy_mini_agora_web_sdk.tools.core_tools import ToolDependencies


logger = logging.getLogger(__name__)


class SessionStatePayload(BaseModel):
    """Web RTC session state payload from frontend."""

    active: bool = False


class AudioChunkPayload(BaseModel):
    """Audio chunk payload for web speech wobble."""

    pcm_b64: str = ""
    level: float = 0.0
    sample_rate: int = 24000


class WebSpeechMotionBridge:
    """Drive Reachy speech wobble from web-reported audio activity."""

    def __init__(self) -> None:
        """Initialize motion bridge state and runtime flags."""
        self._enabled = True
        self._running = False
        self._robot = None
        self._movement_manager = None
        self._head_wobbler = None
        self._last_level = 0.0
        self._last_speaking = False
        self._vad_speaking = False
        self._last_voice_ts = 0.0
        self._conversation_state = "idle"
        self._assistant_speaking = False
        self._state_seen = False
        self._session_active = False
        self._session_activated_ts = 0.0
        self._state_grace_s = 1.2
        self._startup_energy_mode = False
        self._vad_on_level = 0.028
        self._vad_off_level = 0.016
        self._vad_release_s = 0.55
        self._gate_lock = threading.Lock()
        self._start_lock = threading.Lock()
        self._prime_b64 = self._build_prime_chunk_b64()

    @property
    def enabled(self) -> bool:
        """Return True when motion bridge is active and can accept updates."""
        return self._enabled and self._movement_manager is not None and self._head_wobbler is not None

    def start(self) -> None:
        """Initialize robot motion pipeline and start wobble loop."""
        if self._running:
            return
        with self._start_lock:
            if self._running:
                return
            self._start_impl()

    def _start_impl(self) -> None:
        """Internal startup path guarded by _start_lock."""
        try:
            from reachy_mini import ReachyMini
            from reachy_mini_agora_web_sdk.moves import MovementManager
            from reachy_mini_agora_web_sdk.audio.head_wobbler import HeadWobbler
        except Exception as exc:
            logger.warning("Unable to import Reachy motion modules for web wobble: %s", exc)
            return

        try:
            robot_name = str(os.getenv("AGORA_ROBOT_NAME", "")).strip() or None
            robot_kwargs = {"robot_name": robot_name} if robot_name else {}
            self._robot = ReachyMini(**robot_kwargs)
            self._robot.enable_motors()
            self._movement_manager = MovementManager(current_robot=self._robot, camera_worker=None)
            self._movement_manager.start()
            self._movement_manager.set_listening(True)
            self._head_wobbler = HeadWobbler(set_speech_offsets=self._movement_manager.set_speech_offsets)
            self._head_wobbler.start()
            self._running = True
            logger.info("Web speech wobble bridge started (HeadWobbler pipeline)")
        except Exception as exc:
            self._running = False
            self._movement_manager = None
            self._head_wobbler = None
            if self._robot is not None:
                try:
                    self._robot.client.disconnect()
                except Exception:
                    pass
                self._robot = None
            logger.warning("Failed to start web speech wobble bridge: %s", exc)

    def _apply_vad_gate(self, level: float, now: float) -> bool:
        """Apply VAD hysteresis and update listening gate."""
        level = min(max(float(level), 0.0), 1.0)
        with self._gate_lock:
            self._last_level = level

            if level >= self._vad_on_level:
                self._last_voice_ts = now
                level_speaking = True
            elif level <= self._vad_off_level:
                level_speaking = (now - self._last_voice_ts) <= self._vad_release_s
            else:
                level_speaking = self._vad_speaking

            vad_speaking = bool(level_speaking)
            if not self._session_active:
                vad_speaking = False
            if self._state_seen:
                if self._startup_energy_mode:
                    # Startup first utterance: rely on audio energy, not state.
                    pass
                else:
                    vad_speaking = bool(vad_speaking and self._assistant_speaking)
                # Datastream state can lag the first TTS syllables; allow a
                # short post-join grace so greeting starts with visible wobble.
                in_grace = (now - self._session_activated_ts) <= self._state_grace_s
                if not self._startup_energy_mode and not self._assistant_speaking and not in_grace:
                    vad_speaking = False
            should_reset_wobbler = bool(self._head_wobbler is not None and self._last_speaking and not vad_speaking)
            self._last_speaking = vad_speaking
            self._vad_speaking = level_speaking

        if not vad_speaking:
            if self._movement_manager is not None:
                self._movement_manager.set_speech_offsets((0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
            if should_reset_wobbler and self._head_wobbler is not None:
                self._head_wobbler.reset()
        return vad_speaking

    def _build_prime_chunk_b64(self) -> str:
        """Build a tiny synthetic PCM chunk to kick-start first speaking wobble."""
        sr = 16_000
        dur_s = 0.04  # 40ms
        t = np.arange(int(sr * dur_s), dtype=np.float32) / float(sr)
        tone = 0.08 * np.sin(2.0 * np.pi * 170.0 * t)
        pcm = np.clip(tone * 32767.0, -32768, 32767).astype(np.int16)
        return base64.b64encode(pcm.tobytes()).decode("utf-8")

    def update_session_state(self, active: bool) -> None:
        """Enable motion only when frontend confirms RTC session is active."""
        active = bool(active)
        if active and not self._running:
            self.start()
        now = time.monotonic()
        with self._gate_lock:
            self._session_active = active
            if active:
                self._session_activated_ts = now
                self._startup_energy_mode = True
                self._state_seen = False
                self._assistant_speaking = False
                self._conversation_state = "idle"
        if self._movement_manager is not None:
            self._movement_manager.set_listening(not active)
        if not active:
            if self._movement_manager is not None:
                self._movement_manager.set_speech_offsets((0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
            if self._head_wobbler is not None:
                self._head_wobbler.reset()

    def feed_audio_chunk(self, pcm_b64: str, level: float, sample_rate: int) -> None:
        """Feed PCM chunk (base64 int16) to HeadWobbler for energy-based motion."""
        if not self.enabled or self._head_wobbler is None:
            return
        speaking_now = self._apply_vad_gate(level, time.monotonic())
        if not speaking_now:
            return
        self._head_wobbler.feed_with_sample_rate(pcm_b64, int(sample_rate))

    def update_conversation_state(self, state: str) -> None:
        """Mirror python-sdk message.state gate for robust speech motion control."""
        if not self.enabled:
            return
        state = str(state or "").strip().lower()
        if not state:
            return
        is_speaking = state == "speaking"
        with self._gate_lock:
            prev_state = self._conversation_state
            prev_speaking = self._assistant_speaking
            self._conversation_state = state
            self._assistant_speaking = is_speaking
            self._state_seen = True
            if self._startup_energy_mode and prev_speaking and not is_speaking:
                # First startup utterance finished; switch back to strict
                # message.state speaking gate for subsequent utterances.
                self._startup_energy_mode = False
                logger.info("WEB_MOTION_GATE startup energy mode disabled; using strict state gate.")
        if prev_state != state:
            logger.info("WEB_MOTION_GATE state transition: %s -> %s", prev_state, state)
        if self._movement_manager is not None:
            try:
                self._movement_manager.set_listening(state == "listening")
            except Exception:
                pass
        if is_speaking and not prev_speaking and self._head_wobbler is not None:
            # Kick-start motion on first syllable before real audio chunks arrive.
            self._head_wobbler.feed_with_sample_rate(self._prime_b64, 16_000)
        if not is_speaking and prev_speaking:
            if self._movement_manager is not None:
                self._movement_manager.set_speech_offsets((0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
            if self._head_wobbler is not None:
                self._head_wobbler.reset()

    def stop(self) -> None:
        """Stop wobble loop and clean up robot connection."""
        if not self._running:
            return
        if self._head_wobbler is not None:
            try:
                self._head_wobbler.reset()
                self._head_wobbler.stop()
            except Exception as exc:
                logger.debug("Error stopping head wobbler: %s", exc)
        self._head_wobbler = None

        if self._movement_manager is not None:
            try:
                self._movement_manager.set_listening(False)
                self._movement_manager.stop()
            except Exception as exc:
                logger.debug("Error stopping movement manager: %s", exc)
        self._movement_manager = None

        if self._robot is not None:
            try:
                self._robot.client.disconnect()
            except Exception as exc:
                logger.debug("Error disconnecting robot client: %s", exc)
        self._robot = None
        self._running = False
        with self._gate_lock:
            self._last_speaking = False
            self._vad_speaking = False
            self._last_voice_ts = 0.0
            self._conversation_state = "idle"
            self._assistant_speaking = False
            self._state_seen = False
            self._session_active = False
            self._session_activated_ts = 0.0
            self._startup_energy_mode = False
        logger.info("Web speech wobble bridge stopped")

    def get_tool_deps(self) -> ToolDependencies | None:
        """Expose tool dependencies for local tool dispatch from web datastream."""
        if self._robot is None or self._movement_manager is None:
            return None
        return ToolDependencies(
            reachy_mini=self._robot,
            movement_manager=self._movement_manager,
            camera_worker=None,
            vision_manager=None,
            head_wobbler=None,
        )
