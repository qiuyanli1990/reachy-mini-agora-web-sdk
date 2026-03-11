const statusEl = document.getElementById("status");
const logEl = document.getElementById("log");

let client = null;
let localAudioTrack = null;
let localVideoTrack = null;
let selectedSpeaker = null;
let session = null;
let heartbeatTimer = null;
let leaveInProgress = false;
let remoteAnalyserContext = null;
let remoteAudioSource = null;
let remoteScriptProcessor = null;
let remoteMuteGain = null;
let remotePlaybackVolume = 100;
let remoteAudioSampleRate = 24000;
let resampleCarry = new Float32Array(0);
let resamplePos = 0;
let pcm16Carry = new Int16Array(0);

const TARGET_SR = 16000;
const CHUNK_SAMPLES = 320; // 20ms at 16kHz for faster wobble response

function log(message, extra = null) {
  const line = `[${new Date().toISOString()}] ${message}`;
  logEl.textContent += `${line}\n`;
  if (extra) {
    logEl.textContent += `${JSON.stringify(extra, null, 2)}\n`;
  }
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(text) {
  statusEl.textContent = text;
  log(text);
}

function decodeStreamMessage(data) {
  try {
    if (typeof data === "string") {
      return data;
    }
    if (data instanceof Uint8Array) {
      return new TextDecoder().decode(data);
    }
    if (data instanceof ArrayBuffer) {
      return new TextDecoder().decode(new Uint8Array(data));
    }
    if (ArrayBuffer.isView(data)) {
      return new TextDecoder().decode(new Uint8Array(data.buffer));
    }
    return String(data);
  } catch (err) {
    return `[decode-error] ${err.message}`;
  }
}

function decodePackedDatastreamText(rawText) {
  const text = String(rawText || "");
  const parts = text.split("|");
  if (parts.length < 4) return null;
  const b64Payload = parts[parts.length - 1];
  try {
    const jsonText = atob(b64Payload);
    const obj = JSON.parse(jsonText);
    return obj;
  } catch (_) {
    return null;
  }
}

function parseStreamMessageArgs(args) {
  // Handle possible SDK callback signatures:
  // 1) (uid, streamId, data)
  // 2) (uid, data)
  // 3) ({ uid, streamId, data })
  if (args.length === 1 && typeof args[0] === "object" && args[0] !== null) {
    const evt = args[0];
    return {
      uid: evt.uid ?? "unknown",
      streamId: evt.streamId ?? evt.stream_id ?? -1,
      data: evt.data ?? evt.payload ?? evt.message ?? "",
    };
  }
  if (args.length >= 3) {
    return { uid: args[0], streamId: args[1], data: args[2] };
  }
  if (args.length === 2) {
    return { uid: args[0], streamId: -1, data: args[1] };
  }
  return { uid: "unknown", streamId: -1, data: "" };
}

function matchesKeywords(label, keywords) {
  const lc = (label || "").toLowerCase();
  return keywords.some((k) => lc.includes(k.toLowerCase()));
}

function pickReachyDevice(devices, keywords, kind, strict) {
  const device = devices.find((d) => matchesKeywords(d.label, keywords));
  if (device) {
    return device;
  }
  if (!strict && devices.length > 0) {
    log(`Reachy ${kind} not found, fallback to first available device`, devices[0]);
    return devices[0];
  }
  throw new Error(`Reachy ${kind} device not found, strict mode blocks fallback.`);
}

async function fetchSession() {
  const res = await fetch("/api/agora/session");
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to fetch /api/agora/session: ${res.status} ${text}`);
  }
  return res.json();
}

async function postStartAgent() {
  const res = await fetch("/api/agora/agent/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to start agent: ${res.status} ${text}`);
  }
  const data = await res.json();
  if (!data.ok) {
    throw new Error(data.error || "Failed to start agent");
  }
  return data;
}

async function postSessionState(active) {
  try {
    await fetch("/api/motion/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ active: !!active }),
      keepalive: true,
    });
  } catch (_) {
    // Best effort only.
  }
}

function int16ToBase64(samples) {
  const bytes = new Uint8Array(samples.buffer);
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    const sub = bytes.subarray(i, i + chunk);
    binary += String.fromCharCode(...sub);
  }
  return btoa(binary);
}

function concatFloat32(a, b) {
  if (a.length === 0) return b;
  if (b.length === 0) return a;
  const out = new Float32Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

function concatInt16(a, b) {
  if (a.length === 0) return b;
  if (b.length === 0) return a;
  const out = new Int16Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

function resampleToTarget(input, inputRate) {
  if (!input || input.length === 0) return new Float32Array(0);
  if (inputRate <= 0) return input;

  const data = concatFloat32(resampleCarry, input);
  if (inputRate === TARGET_SR) {
    resampleCarry = new Float32Array(0);
    resamplePos = 0;
    return data;
  }

  const step = inputRate / TARGET_SR;
  const out = [];
  let pos = resamplePos;
  while (pos + 1 < data.length) {
    const i0 = Math.floor(pos);
    const frac = pos - i0;
    const s0 = data[i0];
    const s1 = data[i0 + 1];
    out.push(s0 + (s1 - s0) * frac);
    pos += step;
  }

  const keepFrom = Math.max(0, Math.floor(pos) - 1);
  resampleCarry = data.slice(keepFrom);
  resamplePos = pos - keepFrom;
  return Float32Array.from(out);
}

async function postAudioChunk(pcmB64, level = 0) {
  try {
    await fetch("/api/motion/audio-chunk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      // Chunk is already resampled to TARGET_SR.
      body: JSON.stringify({ pcm_b64: pcmB64, level, sample_rate: TARGET_SR }),
      keepalive: true,
    });
  } catch (_) {
    // Best effort only.
  }
}

async function postDatastreamMessage(payload) {
  try {
    await fetch("/api/datastream/message", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      keepalive: true,
    });
  } catch (_) {
    // Best effort only.
  }
}

function stopRemoteAudioAnalysis() {
  if (remoteScriptProcessor) {
    try {
      remoteScriptProcessor.disconnect();
    } catch (_) {}
    remoteScriptProcessor.onaudioprocess = null;
    remoteScriptProcessor = null;
  }
  if (remoteAudioSource) {
    try {
      remoteAudioSource.disconnect();
    } catch (_) {}
    remoteAudioSource = null;
  }
  if (remoteMuteGain) {
    try {
      remoteMuteGain.disconnect();
    } catch (_) {}
    remoteMuteGain = null;
  }
  if (remoteAnalyserContext) {
    remoteAnalyserContext.close().catch(() => {});
    remoteAnalyserContext = null;
  }
  remoteAudioSampleRate = 24000;
  resampleCarry = new Float32Array(0);
  resamplePos = 0;
  pcm16Carry = new Int16Array(0);
}

function startRemoteAudioAnalysis(audioTrack) {
  stopRemoteAudioAnalysis();
  if (!audioTrack?.getMediaStreamTrack) {
    return;
  }

  try {
    const mediaStreamTrack = audioTrack.getMediaStreamTrack();
    const stream = new MediaStream([mediaStreamTrack]);
    remoteAnalyserContext = new (window.AudioContext || window.webkitAudioContext)();
    remoteAudioSampleRate = remoteAnalyserContext.sampleRate || 24000;
    remoteAudioSource = remoteAnalyserContext.createMediaStreamSource(stream);
    // Smaller processing buffer reduces start latency for first TTS syllable.
    remoteScriptProcessor = remoteAnalyserContext.createScriptProcessor(512, 1, 1);
    remoteMuteGain = remoteAnalyserContext.createGain();
    remoteMuteGain.gain.value = 0.0;
    if (remoteAnalyserContext.state === "suspended") {
      remoteAnalyserContext.resume().catch(() => {});
    }
    remoteAudioSource.connect(remoteScriptProcessor);
    remoteScriptProcessor.connect(remoteMuteGain);
    remoteMuteGain.connect(remoteAnalyserContext.destination);

    remoteScriptProcessor.onaudioprocess = (event) => {
      const input = event.inputBuffer.getChannelData(0);
      if (!input || input.length === 0) return;
      const resampled = resampleToTarget(input, remoteAudioSampleRate);
      if (!resampled || resampled.length === 0) return;

      const pcm16 = new Int16Array(resampled.length);
      for (let i = 0; i < resampled.length; i += 1) {
        const s = resampled[i];
        pcm16[i] = Math.max(-32768, Math.min(32767, Math.round(s * 32767)));
      }
      pcm16Carry = concatInt16(pcm16Carry, pcm16);

      while (pcm16Carry.length >= CHUNK_SAMPLES) {
        const chunk = pcm16Carry.slice(0, CHUNK_SAMPLES);
        pcm16Carry = pcm16Carry.slice(CHUNK_SAMPLES);

        let sum = 0;
        for (let i = 0; i < chunk.length; i += 1) {
          const n = chunk[i] / 32767;
          sum += n * n;
        }
        const rms = Math.sqrt(sum / chunk.length);
        const level = Math.min(Math.max(rms * 4.0, 0), 1);
        postAudioChunk(int16ToBase64(chunk), level);
      }
    };
  } catch (err) {
    log(`Remote audio analysis init failed: ${err.message}`);
  }
}

async function prepareDevices() {
  // Permission must be requested before labels become available.
  await navigator.mediaDevices.getUserMedia({ audio: true, video: true });

  const [mics, cams, speakers] = await Promise.all([
    AgoraRTC.getMicrophones(),
    AgoraRTC.getCameras(),
    AgoraRTC.getPlaybackDevices(),
  ]);
  log("Detected devices", { mics, cams, speakers });

  const strict = session.strictReachyDevices !== false;
  const keywords = session.deviceKeywords || ["Reachy", "USB", "Pollen"];

  const mic = pickReachyDevice(mics, keywords, "microphone", strict);
  const cam = pickReachyDevice(cams, keywords, "camera", strict);
  selectedSpeaker = pickReachyDevice(speakers, keywords, "speaker", strict);

  [localAudioTrack, localVideoTrack] = await Promise.all([
    AgoraRTC.createMicrophoneAudioTrack({ microphoneId: mic.deviceId }),
    AgoraRTC.createCameraVideoTrack({ cameraId: cam.deviceId }),
  ]);

  localVideoTrack.play("local-video");
  log("Selected Reachy devices", {
    microphone: mic.label,
    camera: cam.label,
    speaker: selectedSpeaker.label,
  });
}

async function join() {
  if (client) {
    return;
  }
  session = await fetchSession();
  remotePlaybackVolume = Math.round((session.playbackVolume ?? 1.0) * 100);
  remotePlaybackVolume = Math.min(Math.max(remotePlaybackVolume, 0), 100);
  setStatus("Connecting...");

  client = AgoraRTC.createClient({ mode: "rtc", codec: "vp8" });
  client.on("stream-message", async (...args) => {
    const { uid, streamId, data } = parseStreamMessageArgs(args);
    const text = decodeStreamMessage(data);
    let parsed = null;
    try {
      parsed = JSON.parse(text);
    } catch (_) {
      // Non-JSON payload is still valid.
    }
    const packed = decodePackedDatastreamText(text);
    const effective = packed || parsed;
    if (effective && effective.object === "message.user") {
      if (typeof effective.content === "string") {
        try {
          const nested = JSON.parse(effective.content);
          log("message.user", nested);
        } catch (_) {
          log("message.user", effective.content);
        }
      } else {
        log("message.user", effective);
      }
    }

    await postDatastreamMessage({
      uid,
      streamId,
      text,
      json_data: effective || parsed,
      ts: Date.now(),
    });
  });
  client.on("user-published", async (user, mediaType) => {
    await client.subscribe(user, mediaType);
    log(`Subscribed to remote ${mediaType}`, { uid: user.uid });
    if (mediaType === "audio") {
      if (user.audioTrack?.setVolume) {
        user.audioTrack.setVolume(remotePlaybackVolume);
      }
      if (user.audioTrack?.setPlaybackDevice && selectedSpeaker?.deviceId) {
        await user.audioTrack.setPlaybackDevice(selectedSpeaker.deviceId);
      }
      startRemoteAudioAnalysis(user.audioTrack);
      user.audioTrack.play();
    }
    if (mediaType === "video") {
      log("Remote video subscribed but hidden from UI", { uid: user.uid });
    }
  });

  client.on("user-unpublished", (user, mediaType) => {
    log(`Remote ${mediaType} unpublished`, { uid: user.uid });
    if (mediaType === "audio") {
      stopRemoteAudioAnalysis();
    }
  });

  await prepareDevices();
  await client.join(session.appId, session.channel, session.token || null, session.uid || null);
  postSessionState(true);
  await postStartAgent();
  await client.publish([localAudioTrack, localVideoTrack]);
  setStatus(`Joined channel ${session.channel} as uid ${session.uid}`);
}

async function leave() {
  if (leaveInProgress) {
    return;
  }
  leaveInProgress = true;
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  postSessionState(false);
  if (localAudioTrack) {
    localAudioTrack.stop();
    localAudioTrack.close();
    localAudioTrack = null;
  }
  if (localVideoTrack) {
    localVideoTrack.stop();
    localVideoTrack.close();
    localVideoTrack = null;
  }
  if (client) {
    await client.leave();
    client.removeAllListeners();
    client = null;
  }
  stopRemoteAudioAnalysis();
  setStatus("Left channel");
  leaveInProgress = false;
}

setStatus("Ready");

async function checkBackendHealth() {
  try {
    const res = await fetch("/api/health", { cache: "no-store" });
    if (!res.ok) {
      throw new Error(`health status ${res.status}`);
    }
  } catch (_) {
    setStatus("Backend disconnected, leaving RTC...");
    await leave();
    // Best effort only; browsers may refuse unless tab was script-opened.
    window.close();
  }
}

window.addEventListener("load", async () => {
  try {
    await join();
    heartbeatTimer = setInterval(() => {
      checkBackendHealth();
    }, 1500);
  } catch (err) {
    setStatus(`Auto-join failed: ${err.message}`);
    console.error(err);
  }
});

window.addEventListener("beforeunload", () => {
  // Best-effort local cleanup when tab/window closes.
  postSessionState(false);
  stopRemoteAudioAnalysis();
  leave();
});
