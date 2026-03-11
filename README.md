# reachy-mini-agora-web-sdk

## 1. Project Overview
This project is a **Reachy Mini + Agora Conversational AI** conversation app, and now keeps only Web SDK RTC mode:

- Audio/video transmission is handled by Agora Web SDK in the browser.
- The page automatically joins/leaves the channel.
- Only Reachy Mini mic/cam/speaker are allowed (keyword matching; strict mode enabled by default).

## 2. Solution Architecture
![Solution Architecture](presentation/ppt_scheme_from_sketch.svg)

## 3. Directory Structure (Core)
```text
reachy-mini-agora-web-sdk/
├── .env
├── agent_config.json
├── prompt.txt
├── README_CN.md / README.md
├── src/reachy_mini_agora_web_sdk/
│   ├── main.py
│   ├── web_rtc_server.py
│   ├── web_session_service.py
│   ├── web_datastream_processor.py
│   ├── web_motion_bridge.py
│   ├── moves.py
│   └── tools/
└── static/web_rtc/
    ├── index.html
    └── app.js
```

## 4. Configuration

### 4.1 Required environment variables in `.env`
- `AGORA_APP_ID`
- `AGORA_API_KEY`
- `AGORA_API_SECRET`
- `AGORA_APP_CERTIFICATE` (if set, backend signs RTC token)
- `AGORA_CHANNEL_NAME` (default: `reachy_conversation`)
- `AGORA_Reachy_mini_USER_ID` (required Web join UID)

Valid `.env` path:
- `.env` in the project root
- You can start from `.env.example`.

### 4.2 `agent_config.json` setup
- File path: `agent_config.json` in the project root
- Content requirement: provide a Start Body JSON that matches Agora ConvoAI `/join` schema.
- Recommended checks:
  - `properties.agent_rtc_uid` must not conflict with the user UID.
  - `properties.remote_rtc_uids` should include the Web join UID (this project uses `AGORA_Reachy_mini_USER_ID`).
  - If using an external prompt file, you can set `"{{prompt.txt}}"` as a placeholder (replaced at runtime).
- Reference:
  - https://docs.agora.io/en/conversational-ai/rest-api/agent/join
- Refer to the link above for these details.

## 5. Run Steps (Web SDK mode)

1. Prepare the Python environment first
Follow the official Reachy Mini installation guide before running this project. A minimal setup on macOS/Linux is:

On macOS, make sure the Python build matches your machine architecture. Check it with:
```bash
python3 -c "import platform; print(platform.machine())"
```

Possible outputs:
- `arm64` for Apple Silicon
- `x86_64` for Intel or Rosetta

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.12 --default
```

Make sure Git and Git LFS are installed on your OS first, then run:

```bash
git lfs install
uv venv reachy_mini_env --python 3.12
source reachy_mini_env/bin/activate
uv pip install "reachy-mini"
```

2. Clone the project into your apps directory (for example `<apps-dir>`)
```bash
cd <apps-dir>
git clone <repo-url> reachy-mini-agora-web-sdk
```

3. Start daemon (Terminal A)
```bash
source /path/to/venv/bin/activate
reachy-mini-daemon
```

4. Start app (Terminal B)
```bash
source /path/to/venv/bin/activate
cd /path/to/reachy-mini-agora-web-sdk
pip install -e .
python -m reachy_mini_agora_web_sdk.main --web-rtc-server
```

5. Open `http://localhost:8780` in browser
- The page auto-joins after loading; no manual click required.

Alternative startup path:
- If the app has already been installed with `pip install -e .`, you can also open `http://localhost:8000`, find `reachy_mini_agora_web_sdk` in the Applications list, and turn it on from the Reachy Mini dashboard.
- That dashboard entry starts the same WebRTC server flow.

## 6. Runtime Behavior in Web Mode (Current)
- The page automatically requests media permission and enumerates devices.
- Reachy mic/cam/speaker are automatically bound.
- Channel join is automatic.
- **`/api/agora/agent/start` is triggered only after join succeeds**, to avoid first greeting before frontend is in channel.
- Remote audio is chunked by frontend and uploaded to `/api/motion/audio-chunk` for first-utterance and speaking head wobble.
- Datastream enters backend via `/api/datastream/message`, then matches `message.state` and tool-action pipeline.

## 7. Stop and Exit
- Press `Ctrl+C` in server terminal:
  - Stops web server.
  - Stops the agent started by this service (if running).
- Browser tab cannot be force-closed by server (browser security policy).
- Frontend detects backend disconnect and auto-leaves, stopping local tracks and RTC publishing.

## 8. Official References
- Reachy Mini SDK installation:
  - https://huggingface.co/docs/reachy_mini/SDK/installation
- Agora ConvoAI REST `/join`:
  - https://docs.agora.io/en/conversational-ai/rest-api/agent/join
- Agora account and authentication:
  - https://docs.agora.io/en/conversational-ai/get-started/manage-agora-account
  - https://docs.agora.io/en/conversational-ai/rest-api/restful-authentication
