# reachy-mini-agora-web-sdk 项目档案

## 1. 项目概述
本项目是一个 **Reachy Mini + Agora Conversational AI** 对话应用，当前仅保留 Web SDK RTC 模式：

- 音视频收发由浏览器中的 Agora Web SDK 完成。
- 页面自动 join/leave 频道。
- 只允许使用 Reachy Mini 的 mic/cam/speaker（按关键词匹配，严格模式默认开启）。

## 2. 方案架构图
![Solution Architecture](presentation/ppt_scheme_from_sketch.svg)

## 3. 目录结构（核心）
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

## 4. 配置说明

### 4.1 .env必填环境变量
- `AGORA_APP_ID`
- `AGORA_API_KEY`
- `AGORA_API_SECRET`
- `AGORA_APP_CERTIFICATE`（填了就由后端签 RTC token）
- `AGORA_CHANNEL_NAME`（默认 `reachy_conversation`）
- `AGORA_Reachy_mini_USER_ID`（Web 端 join UID，必填）
- 参考最后的链接获取这些信息。

`.env` 有效路径：
- 项目根目录下的 `.env`
- 可先复制 `.env.example` 作为起始模板。

### 4.2 `agent_config.json` 填写说明
- 文件路径：项目根目录下的 `agent_config.json`
- 内容要求：填写符合 Agora ConvoAI `/join` 规范的 Start Body JSON。
- 建议检查项：
  - `properties.agent_rtc_uid` 不能与用户 UID 冲突。
  - `properties.remote_rtc_uids` 应包含 Web 端 join 使用的 UID（当前项目使用 `AGORA_Reachy_mini_USER_ID`）。
  - 如使用外部 prompt 文件，占位可写 `"{{prompt.txt}}"`（由应用在运行时替换）。
- 参考文档：
  - https://docs.agora.io/en/conversational-ai/rest-api/agent/join

## 5. 运行步骤（Web SDK 模式）

1. 先准备 Python 运行环境
运行本项目之前，请先按 Reachy Mini 官方安装文档完成环境准备。macOS / Linux 最小步骤如下：

注意：在 macOS 上安装 Python 时，要确认 Python 架构与机器芯片一致。可先执行：
```bash
python3 -c "import platform; print(platform.machine())"
```

输出可能是：
- `arm64`：Apple Silicon
- `x86_64`：Intel 或 Rosetta

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.12 --default
```

请先确保系统里已经安装 Git 和 Git LFS，然后继续执行：

```bash
git lfs install
uv venv reachy_mini_env --python 3.12
source reachy_mini_env/bin/activate
uv pip install "reachy-mini"
```

2. 将项目代码 clone 到你的 apps 目录（例如 `<apps-dir>`）
```bash
cd <apps-dir>
git clone <repo-url> reachy-mini-agora-web-sdk
```

3. 启动 daemon（终端 A）
```bash
source /path/to/venv/bin/activate
reachy-mini-daemon
```

4. 启动应用（终端 B）
```bash
source /path/to/venv/bin/activate
cd /path/to/reachy-mini-agora-web-sdk
pip install -e .
python -m reachy_mini_agora_web_sdk.main --web-rtc-server
```

5. 浏览器打开 `http://localhost:8780`
- 页面加载后会自动 join，无需手动点击。

另一种启动方式：
- 如果项目已经执行过 `pip install -e .`，也可以打开 `http://localhost:8000`，在 Applications 列表中找到 `reachy_mini_agora_web_sdk`，然后通过 Reachy Mini 控制面板的 `Turn On` 开关启动。
- 这条控制面板启动链路最终也是同一个 WebRTC server。

## 6. Web 模式运行时行为（当前实现）
- 页面自动申请媒体权限并枚举设备。
- 自动绑定 Reachy mic/cam/speaker。
- 自动 join 频道。
- **join 成功后才触发 `/api/agora/agent/start`**，避免首句 greeting 发生在前端未入会阶段。
- 远端音频被前端切块上传到 `/api/motion/audio-chunk`，用于首句和说话时头部律动。
- datastream 通过 `/api/datastream/message` 进入后端，匹配 `message.state` 与工具动作链路。

## 7. 停止与退出
- 在服务端终端按 `Ctrl+C`：
  - 停止 Web server。
  - 停止本服务启动的 agent（若在运行）。
- 浏览器标签页不会被服务端强制关闭（浏览器安全限制）。
- 前端会检测后端不可达并自动 leave，停止本地轨道与 RTC 发布。

## 8. 官方文档参考
- Agora ConvoAI REST `/join`：
  - https://docs.agora.io/en/conversational-ai/rest-api/agent/join
- Agora 账号与鉴权：
  - https://docs.agora.io/en/conversational-ai/get-started/manage-agora-account
  - https://docs.agora.io/en/conversational-ai/rest-api/restful-authentication
- Reachy Mini SDK 安装：
  - https://huggingface.co/docs/reachy_mini/SDK/installation
