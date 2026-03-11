"""Entrypoint for Web SDK RTC server mode."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from reachy_mini import ReachyMini, ReachyMiniApp


def _load_env_from_app_root() -> None:
    """Load .env from app root."""
    if str(os.getenv("REACHY_MINI_SKIP_DOTENV", "")).strip().lower() in {"1", "true", "yes", "on"}:
        return
    app_root = Path(__file__).resolve().parents[2]
    dotenv_path = app_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=str(dotenv_path), override=True)
    else:
        logging.getLogger(__name__).warning("No .env file found at %s", dotenv_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Reachy Mini Agora WebRTC Server")
    parser.add_argument(
        "--web-rtc-server",
        action="store_true",
        default=False,
        help="Run FastAPI server for Agora Web SDK RTC.",
    )
    parser.add_argument("--debug", action="store_true", default=False, help="Enable debug logging.")
    args, _ = parser.parse_known_args()
    return args


def _setup_logger(debug: bool) -> logging.Logger:
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d | %(message)s",
    )
    return logging.getLogger(__name__)


def main() -> None:
    """Run web rtc server mode."""
    _load_env_from_app_root()
    args = _parse_args()
    logger = _setup_logger(args.debug)
    if not args.web_rtc_server:
        logger.warning("Non-web mode has been removed; forcing --web-rtc-server.")
    logger.info("Starting Agora WebRTC server mode")
    from reachy_mini_agora_web_sdk.web_rtc_server import run_web_rtc_server

    run_web_rtc_server()


class ReachyMiniAgoraConversationApp(ReachyMiniApp):  # type: ignore[misc]
    """Reachy Mini Apps entry point."""

    custom_app_url = "http://0.0.0.0:8780/"
    dont_start_webserver = False

    def run(self, reachy_mini: ReachyMini, stop_event) -> None:  # noqa: ARG002
        """Run app in web rtc server mode."""
        _ = reachy_mini
        _ = stop_event
        main()


if __name__ == "__main__":
    main()
