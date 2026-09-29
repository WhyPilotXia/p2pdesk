#!/usr/bin/env python3
"""
p2pdesk client (single script, both roles)
  python p2pdesk.py share              : be controlled (register + wait)
  python p2pdesk.py watch <peer_id>    : connect & control remote peer
  python p2pdesk.py term <peer_id>     : remote ConPTY terminal
  python p2pdesk.py list               : list online peers

Transport preference:
  1) aiortc DataChannel P2P (screen frames + control events + terminal)
  2) fallback: server binary relay over the same ws
  On P2P failure: keep retrying renegotiation with fresh ICE ports while
  relaying through the server, so the session never dies on transient holes.

Frame format (both transports): [u32 len][port u8][payload]
  port 0x10 = jpeg screen frame, 0x20 = json control event
  port 0x30 = terminal ansi output chunk, 0x31 = terminal stdin/control json
Env: P2PDESK_SERVER, P2PDESK_TOKEN, P2PDESK_ID, P2PDESK_FPS,
     P2PDESK_Q, P2PDESK_P2P_TIMEOUT, P2PDESK_RENEG_RETRY, P2PDESK_ICE
"""
import asyncio
import json
import os
import struct
import subprocess
import sys
import threading
import time

import websockets

# ------------------------- .env loading -------------------------

def load_env():
    """Load KEY=VALUE pairs from .env next to this script (env vars win)."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception as e:
        print(f"[!] .env load failed: {e}", flush=True)


load_env()

# ------------------------- config -------------------------
SERVER_WS = os.environ.get("P2PDESK_SERVER", "ws://118.31.105.6:9000")
TOKEN = os.environ.get("P2PDESK_TOKEN", "")
SELF_ID = os.environ.get("P2PDESK_ID") or os.environ.get("USERNAME") or os.environ.get("COMPUTERNAME") or "pc"

if not TOKEN:
    print("[!] P2PDESK_TOKEN not set. Copy client/.env.example to client/.env and fill it in.")
    sys.exit(1)

FPS = int(os.environ.get("P2PDESK_FPS", "10"))
JPEG_Q = int(os.environ.get("P2PDESK_Q", "50"))
P2P_TIMEOUT = int(os.environ.get("P2PDESK_P2P_TIMEOUT", "25"))
RENEG_RETRY = float(os.environ.get("P2PDESK_RENEG_RETRY", "5"))     # renegotiate interval
RENEG_MAX = int(os.environ.get("P2PDESK_RENEG_MAX", "0"))          # 0 = forever
ICE_SERVERS = os.environ.get(
    "P2PDESK_ICE",
    "stun:stun.miwifi.com:3478,stun:stun.chat.bilibili.com:3478",
)

try:
    import cv2
    import mss
    import numpy as np
    import pyautogui

    HAVE_GUI_LIBS = True
except Exception as _e:
    HAVE_GUI_LIBS = False

try:
    from aiortc import (RTCIceServer, RTCConfiguration, RTCPeerConnection,
                        RTCSessionDescription)

    HAVE_AIORTC = True
except Exception:
    HAVE_AIORTC = False

try:
    from pywinpty import PtyConnection  # ConPTY real terminal

    HAVE_CONPTY = True
except Exception:
    HAVE_CONPTY = False

CMD_PORT = 0x20
FRAME_PORT = 0x10
SHELL_PORT = 0x30       # terminal stdout/ansi stream host -> controller
SHELLCTL_PORT = 0x31    # terminal stdin / control json controller -> host


def die(msg):
    print(f"[!] {msg}", flush=True)
    sys.exit(1)


def require(name, ok):
    if not ok:
        die(f"missing dependency: {name}  ->  pip install {name}")


def pack_frame(port, payload: bytes) -> bytes:
    return struct.pack("<I", len(payload) + 1) + bytes([port]) + payload


def unpack_frame(buf: bytes):
    if len(buf) < 5:
        return None, None
    ln = struct.unpack("<I", buf[:4])[0]
    if len(buf) < 4 + ln:
        return None, None
    return buf[4], buf[5:4 + ln]


# ------------------------- signaling -------------------------

class Signaling:
    def __init__(self):
        self.ws = None
        self.on_msg = None
        self.on_bin = None
        self.on_disconnect = None
        self.closed = False

    async def connect(self):
        backoff = 1
        while True:
            try:
                self.ws = await websockets.connect(SERVER_WS, max_size=16 * 1024 * 1024, ping_interval=20)
                await self.ws.send(json.dumps({"type": "register", "id": SELF_ID, "token": TOKEN}))
                resp = json.loads(await asyncio.wait_for(self.ws.recv(), 10))
                if resp.get("type") != "registered":
                    die(f"register failed: {resp}")
                print(f"[i] registered as '{SELF_ID}' @ {SERVER_WS}", flush=True)
                asyncio.ensure_future(self._reader())
                return
            except Exception as e:
                print(f"[!] connect failed: {e}, retry in {backoff}s ...", flush=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def send(self, msg: dict):
        if self.ws:
            await self.ws.send(json.dumps(msg, ensure_ascii=False))

    async def send_bin_to(self, target: str, payload: bytes):
        if self.ws:
            await self.ws.send(b"\x01" + target.encode() + b"\x00" + payload)

    async def _reader(self):
        try:
            async for raw in self.ws:
                if isinstance(raw, bytes) and self.on_bin:
                    await self.on_bin(raw)
                elif isinstance(raw, str) and self.on_msg:
                    await self.on_msg(json.loads(raw))
        except Exception:
            pass
        finally:
            if not self.closed:
                print("[!] signaling lost, reconnecting ...", flush=True)
                if self.on_disconnect:
                    try:
                        self.on_disconnect()
                    except Exception:
                        pass
                await self.connect()


# ------------------------- p2p channel -------------------------

class P2PChannel:
    def __init__(self):
        self.pc = None
        self.dc = None
        self.ready = asyncio.Event()
        self.on_frame = None
        self.on_open = None
        self.on_state = None

    def _mk_pc(self):
        require("aiortc", HAVE_AIORTC)
        ice = [RTCIceServer(urls=u.strip()) for u in ICE_SERVERS.split(",") if u.strip()]
        self.pc = RTCPeerConnection(RTCConfiguration(iceServers=ice))
        self.pc.on("connectionstatechange", self._on_state)
        self.pc.on("datachannel", self._on_remote_dc)

    def _on_state(self):
        state = self.pc.connectionState
        print(f"    [p2p] state: {state}", flush=True)
        if self.on_state:
            try:
                self.on_state(state)
            except Exception:
                pass

    def _on_remote_dc(self, channel):
        print(f"    [p2p] remote datachannel '{channel.label}'", flush=True)
        self._bind(channel)

    def _bind(self, channel):
        self.dc = channel
        channel.on("open", self._dc_open)

        @channel.on("message")
        def on_message(msg):
            if isinstance(msg, bytes):
                asyncio.ensure_future(self._handle(msg))

        if channel.readyState == "open":
            self._dc_open()

    def _dc_open(self):
        print("    [p2p] datachannel OPEN", flush=True)
        self.ready.set()
        if self.on_open:
            asyncio.ensure_future(self.on_open())

    async def _handle(self, msg: bytes):
        port, payload = unpack_frame(msg)
        if port is not None and self.on_frame:
            await self.on_frame(port, payload)

    def is_open(self):
        return self.dc is not None and self.dc.readyState == "open"

    def reset(self):
        """Drop current dc/pc refs for re-negotiation (same object reused)."""
        self.ready.clear()
        self.dc = None

    async def send(self, port: int, payload: bytes):
        if self.is_open():
            self.dc.sendMessage(pack_frame(port, payload))
            return True
        return False

    async def close(self):
        if not self.pc:
            return
        pc, self.pc = self.pc, None
        self.dc = None
        try:
            await asyncio.wait_for(pc.close(), 5)
        except Exception:
            # aiortc close may hang on half-open connections; abandon it
            print("    [p2p] close timeout, abandoned", flush=True)


# ------------------------- shared session -------------------------

class Session:
    """Common: routing between p2p / relay + frame dispatch."""

    def __init__(self, sig: Signaling, peer_id: str):
        self.sig = sig
        self.peer = peer_id
        self.p2p = P2PChannel()
        self.use_relay = False
        self.on_event = None   # controller input events (host side)
        self.on_frame = None   # screen frames (controller side)
        self.on_shell = None   # terminal output chunks (controller side)
        self.on_shellctl = None  # terminal stdin/control (host side)
        self.closed = asyncio.Event()
        # queue outgoing frames while p2p is renegotiating, so no data lost
        self._outq = asyncio.Queue(maxsize=4096)
        self._pump_task = None
        self._send_lock = asyncio.Lock()

    def start_pump(self):
        if not self._pump_task or self._pump_task.done():
            self._pump_task = asyncio.ensure_future(self._pump())

    async def _pump(self):
        """Continuously send queued frames via p2p (or relay fallback).
        Avoids blocking producers when the dc buffer is temporarily full."""
        while not self.closed.is_set():
            try:
                port, payload = await self._outq.get()
                if self.p2p.is_open():
                    await self.p2p.send(port, payload)
                elif self.use_relay:
                    await self.sig.send_bin_to(self.peer, pack_frame(port, payload))
            except asyncio.CancelledError:
                return
            except Exception:
                await asyncio.sleep(0.05)

    async def close(self):
        self.closed.set()
        if self._pump_task and not self._pump_task.done():
            self._pump_task.cancel()
        await self.p2p.close()

    async def route_send(self, port: int, payload: bytes):
        if self.p2p.is_open():
            try:
                self._outq.put_nowait((port, payload))
            except asyncio.QueueFull:
                # drop oldest to make room (screen frames tolerate loss)
                try:
                    self._outq.get_nowait()
                    self._outq.put_nowait((port, payload))
                except Exception:
                    pass
            self.start_pump()
        elif self.use_relay:
            await self.sig.send_bin_to(self.peer, pack_frame(port, payload))

    async def _dispatch(self, port: int, payload: bytes):
        if port == CMD_PORT and self.on_event:
            try:
                await self.on_event(json.loads(payload))
            except Exception:
                pass
        elif port == FRAME_PORT and self.on_frame:
            self.on_frame(payload)
        elif port == SHELL_PORT and self.on_shell:
            self.on_shell(payload)
        elif port == SHELLCTL_PORT and self.on_shellctl:
            try:
                await self.on_shellctl(json.loads(payload))
            except Exception:
                pass

    # relay frames from server: [0x01][src][0x00][frame]
    async def on_relay_bin(self, raw: bytes):
        try:
            if len(raw) < 3 or raw[0] != 0x01:
                return
            z = raw.index(b"\x00", 1)
            if raw[1:z].decode() != self.peer:
                return
            port, payload = unpack_frame(raw[z + 1:])
            if port is not None:
                await self._dispatch(port, payload)
        except Exception:
            pass


# ------------------------- p2p (re)negotiation helpers -------------------------

async def build_and_offer(sess: Session, sig: Signaling, gen: int) -> bool:
    """(Re)create pc + datachannel and send a fresh offer (new ICE ports)."""
    try:
        p2p = sess.p2p
        await p2p.close()
        p2p.reset()
        p2p.on_frame = sess._dispatch
        p2p._mk_pc()
        p2p.dc = p2p.pc.createDataChannel("p2pdesk")
        p2p._bind(p2p.dc)
        await p2p.pc.setLocalDescription(await p2p.pc.createOffer())
        await sig.send({"type": "offer", "to": sess.peer,
                        "sdp": p2p.pc.localDescription.sdp,
                        "sdpType": p2p.pc.localDescription.type,
                        "gen": gen})
        return True
    except Exception as e:
        print(f"[!] offer failed: {e}", flush=True)
        return False


class Renegotiator:
    """While P2P is down: keep re-offering with fresh ICE ports every
    RENEG_RETRY seconds, session stays alive on server relay meanwhile."""

    def __init__(self, sess: Session, sig: Signaling):
        self.sess = sess
        self.sig = sig
        self.gen = 0
        self._task = None

    async def offer(self):
        self.gen += 1
        return await build_and_offer(self.sess, self.sig, self.gen)

    def start(self):
        if not self._task or self._task.done():
            self._task = asyncio.ensure_future(self._loop())

    async def _loop(self):
        n = 0
        while not self.sess.closed.is_set():
            await asyncio.sleep(RENEG_RETRY)
            if self.sess.closed.is_set() or self.sess.p2p.is_open():
                if self.sess.p2p.is_open():
                    print("[i] P2P recovered", flush=True)
                return
            n += 1
            if RENEG_MAX and n > RENEG_MAX:
                print("[i] P2P retries exhausted, staying on server relay", flush=True)
                return
            print(f"[*] P2P retry #{n}: new offer with fresh ICE ports ...", flush=True)
            await self.offer()


# ------------------------- conpty terminal (host side) -------------------------

class TerminalService:
    """Real Windows terminal (ConPTY via pywinpty) attached to a session.
    Lazy: spawned on first use, survives P2P renegotiation (tied to session)."""

    def __init__(self, sess: Session):
        self.sess = sess
        self.pty = None
        self.cols, self.rows = 120, 30
        self.loop = None
        self._warned = False

    def _open(self):
        if self.pty is not None:
            return True
        if not HAVE_CONPTY:
            if not self._warned:
                print("[!] pywinpty missing -> remote terminal unavailable "
                      "(pip install pywinpty)", flush=True)
                self._warned = True
            return False
        cwd = os.getcwd()
        cmdline = 'cmd.exe /K "chcp 65001>nul"'
        for args in (cmdline, cwd, None, self.cols, self.rows), \
                    (["cmd.exe", "/K", "chcp 65001>nul"], cwd, None, self.cols, self.rows):
            try:
                self.pty = PtyConnection.spawn(*args)
                break
            except TypeError:
                continue
            except Exception as e:
                print(f"[!] terminal spawn failed: {e}", flush=True)
                self.pty = None
                return False
        if self.pty is None:
            print("[!] terminal spawn failed: unsupported pywinpty API", flush=True)
            return False
        self.loop = asyncio.get_event_loop()
        threading.Thread(target=self._reader, daemon=True).start()
        print("[i] remote terminal opened (ConPTY)", flush=True)
        return True

    def _write(self, text: str):
        data = text.encode("utf-8")
        try:
            self.pty.write(data)
        except TypeError:
            self.pty.write(text)
        except Exception as e:
            print(f"[!] terminal write error: {e}", flush=True)

    def _reader(self):
        """Thread: pump pty output to SHELL_PORT. On pty death notify ctrl."""
        while self.pty is not None:
            try:
                data = self.pty.read(4096)
            except Exception:
                break
            if data:
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.sess.route_send(SHELL_PORT, bytes(data)), self.loop).result(5)
                except Exception:
                    return
            else:
                time.sleep(0.02)
        if self.pty is not None:
            self.pty = None
            try:
                asyncio.run_coroutine_threadsafe(
                    self.sess.route_send(
                        SHELL_PORT, b"\x00" + json.dumps({"t": "closed"}).encode()),
                    self.loop).result(3)
            except Exception:
                pass

    async def on_ctl(self, msg: dict):
        t = msg.get("t")
        if t == "open":
            if self.pty is None:
                self._open()
            return
        if t == "resize":
            try:
                self.cols, self.rows = int(msg["cols"]), int(msg["rows"])
            except Exception:
                return
            if self.pty is not None:
                try:
                    self.pty.set_size(self.cols, self.rows)
                except Exception:
                    pass
            return
        if t == "in":
            if self.pty is None and not self._open():
                return
            self._write(msg.get("d", ""))

    def close(self):
        pty, self.pty = self.pty, None
        if pty is not None:
            try:
                pty.close()
            except Exception:
                pass


# ------------------------- host (be controlled) -------------------------

class Host:
    def __init__(self, sig: Signaling):
        self.sig = sig
        self.sess = None
        self.stream_task = None
        self.term = None
        self.reneg = None
        self._neg_lock = asyncio.Lock()
        self.sig.on_disconnect = self._on_sig_lost

    async def run(self):
        require("opencv-python/mss/pyautogui", HAVE_GUI_LIBS)
        self.sig.on_bin = self._on_bin
        self.sig.on_msg = self._on_msg
        print(f"[i] sharing desktop as '{SELF_ID}', waiting for controller ...", flush=True)
        while True:
            await asyncio.sleep(3600)

    def _on_sig_lost(self):
        asyncio.ensure_future(self._stop(quiet=True))

    async def _on_msg(self, msg):
        t = msg.get("type")
        if t == "offer":
            print(f"[*] incoming connection from '{msg['from']}' (gen {msg.get('gen', '?')})", flush=True)
            async with self._neg_lock:
                if self.sess and self.sess.peer == msg["from"] and not self.sess.closed.is_set():
                    # renegotiation offer from same controller: keep session alive
                    await self._accept_offer(msg, renegotiate=True)
                    return
                await self._stop(quiet=True)
                self._new_session(msg["from"])
                await self._accept_offer(msg)
                asyncio.ensure_future(self._wait_transport())
        elif t == "bye":
            await self._stop()
        elif t == "peer-offline" and self.sess and msg.get("peer") == self.sess.peer:
            await self._stop()

    def _new_session(self, peer):
        self.sess = Session(self.sig, peer)
        self.sess.on_event = self.exec_event
        self.sess.on_shellctl = None  # set once terminal service created
        self.sess.p2p.on_state = self._on_p2p_state
        self.sig.on_bin = self._on_bin
        self.term = TerminalService(self.sess)
        self.sess.on_shellctl = self.term.on_ctl
        self.reneg = Renegotiator(self.sess, self.sig)

    def _on_p2p_state(self, state):
        if state in ("failed", "closed", "disconnected"):
            if self.sess and not self.sess.closed.is_set():
                print("[!] P2P down -> server relay, renegotiation in background", flush=True)
                self.sess.use_relay = True
                self.sess.start_pump()
                if self.reneg:
                    self.reneg.start()
                # do NOT stop session: relay keeps it alive

    async def _accept_offer(self, msg, renegotiate=False):
        p2p = self.sess.p2p
        p2p.on_frame = self.sess._dispatch
        if not renegotiate:
            p2p._mk_pc()
        else:
            # fresh pc with fresh ICE ports, same P2PChannel object
            await p2p.close()
            p2p.reset()
            p2p._mk_pc()
        await p2p.pc.setRemoteDescription(RTCSessionDescription(sdp=msg["sdp"], type=msg["sdpType"]))
        await p2p.pc.setLocalDescription(await p2p.pc.createAnswer())
        await self.sig.send({"type": "answer", "to": msg["from"],
                             "sdp": p2p.pc.localDescription.sdp,
                             "sdpType": p2p.pc.localDescription.type,
                             "gen": msg.get("gen")})
        print("    [p2p] answer sent", flush=True)

    async def _wait_transport(self):
        # wait p2p open; on timeout switch to relay (session stays alive)
        try:
            await asyncio.wait_for(self.sess.p2p.ready.wait(), P2P_TIMEOUT)
            self._start_stream()
            return
        except asyncio.TimeoutError:
            pass
        print("[!] P2P failed/timeout -> server relay mode", flush=True)
        self.sess.use_relay = True
        self.sess.start_pump()
        if self.reneg:
            self.reneg.start()
        self._start_stream()

    async def _stop(self, quiet=False):
        if not self.sess:
            return
        if not quiet:
            print("[*] session closed by peer", flush=True)
        sess, self.sess = self.sess, None
        term, self.term = self.term, None
        if term:
            term.close()
        task = self.stream_task
        self.stream_task = None
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, 5)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass
        await sess.p2p.close()
        print("[i] session stopped, waiting for next connection ...", flush=True)

    async def _on_bin(self, raw):
        if self.sess:
            await self.sess.on_relay_bin(raw)

    async def _screen_loop(self):
        print(f"[i] streaming {FPS}fps q={JPEG_Q}", flush=True)
        sct = mss.mss()
        mon = sct.monitors[1]
        interval = 1.0 / FPS
        while True:
            t0 = time.time()
            try:
                sess = self.sess
                if sess is None:
                    return
                img = np.array(sct.grab(mon))
                frame = cv2.cvtColor(img, cv2.COLOR_BGRA2BGR)
                ok, jpeg = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_Q])
                if ok:
                    await sess.route_send(FRAME_PORT, jpeg.tobytes())
            except asyncio.CancelledError:
                return
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(max(0.0, interval - (time.time() - t0)))

    async def exec_event(self, ev):
        pyautogui.FAILSAFE = False
        try:
            k = ev.get("kind")
            if k == "move":
                pyautogui.moveTo(ev["x"], ev["y"])
            elif k == "down":
                pyautogui.mouseDown(ev["x"], ev["y"], ev.get("b", "left"))
            elif k == "up":
                pyautogui.mouseUp(ev["x"], ev["y"], ev.get("b", "left"))
            elif k == "scroll":
                pyautogui.scroll(-ev.get("v", 0))
            elif k == "key":
                if ev.get("down"):
                    pyautogui.keyDown(ev["key"])
                else:
                    pyautogui.keyUp(ev["key"])
            elif k == "text":
                pyautogui.typewrite(ev["text"], interval=0.01)
        except Exception as e:
            print(f"[!] event error: {e}", flush=True)


# ------------------------- controller -------------------------

class Controller:
    def __init__(self, sig: Signaling, peer_id: str):
        self.sig = sig
        self.sess = Session(sig, peer_id)
        self.win_size = None  # (w,h) of displayed image
        self.reneg = None

    async def run(self):
        require("opencv-python/mss/pyautogui", HAVE_GUI_LIBS)
        cv2.namedWindow("p2pdesk", cv2.WINDOW_NORMAL)
        cv2.resizeWindow("p2pdesk", 1280, 720)
        cv2.setMouseCallback("p2pdesk", self._on_mouse)
        self.sess.on_frame = self._show_frame
        self.sess.p2p.on_state = self._on_p2p_state
        self.sig.on_bin = self.sess.on_relay_bin
        self.sig.on_msg = self._on_msg
        self.reneg = Renegotiator(self.sess, self.sig)
        # outgoing p2p offer
        p2p = self.sess.p2p
        p2p.on_frame = self.sess._dispatch
        p2p._mk_pc()
        p2p.dc = p2p.pc.createDataChannel("p2pdesk")
        p2p._bind(p2p.dc)
        await p2p.pc.setLocalDescription(await p2p.pc.createOffer())
        await self.sig.send({"type": "offer", "to": self.sess.peer,
                             "sdp": p2p.pc.localDescription.sdp,
                             "sdpType": p2p.pc.localDescription.type})
        print("    [p2p] offer sent", flush=True)
        asyncio.ensure_future(self._wait_transport())
        print("[i] connecting ... ESC/q or close window to quit", flush=True)
        while True:
            await asyncio.sleep(0.02)
            if self.sess.closed.is_set():
                break
            key = cv2.waitKey(1) & 0xFF
            if key in (27, ord("q")):
                break
            # window closed via X -> imshow would recreate it, so detect and quit
            try:
                if cv2.getWindowProperty("p2pdesk", cv2.WND_PROP_VISIBLE) < 1:
                    break
            except cv2.error:
                break
            if 32 <= key < 127:
                asyncio.ensure_future(self.sess.route_send(
                    CMD_PORT, json.dumps({"kind": "text", "text": chr(key)}).encode()))
        await self._quit()

    def _on_p2p_state(self, state):
        if state in ("failed", "closed", "disconnected"):
            if self.sess and not self.sess.closed.is_set():
                print("[!] P2P down -> server relay, renegotiation in background", flush=True)
                self.sess.use_relay = True
                self.sess.start_pump()
                if self.reneg:
                    self.reneg.start()

    async def _wait_transport(self):
        try:
            await asyncio.wait_for(self.sess.p2p.ready.wait(), P2P_TIMEOUT)
            print("[i] transport: P2P direct", flush=True)
            return
        except asyncio.TimeoutError:
            pass
        print("[i] transport: server relay", flush=True)
        self.sess.use_relay = True
        self.sess.start_pump()
        if self.reneg:
            self.reneg.start()

    async def _on_msg(self, msg):
        t = msg.get("type")
        if t == "answer":
            try:
                await self.sess.p2p.pc.setRemoteDescription(
                    RTCSessionDescription(sdp=msg["sdp"], type=msg["sdpType"]))
                print(f"    [p2p] answer accepted (gen {msg.get('gen', '?')})", flush=True)
            except Exception as e:
                print(f"    [p2p] answer error: {e}", flush=True)
        elif t == "offer":
            # host-initiated renegotiation (host is also a controller-capable peer)
            await self._accept_re_offer(msg)
        elif t == "bye":
            print("[*] remote closed", flush=True)
            self.sess.closed.set()
        elif t == "peer-offline" and msg.get("peer") == self.sess.peer:
            print("[*] remote host went offline", flush=True)
            self.sess.closed.set()

    async def _accept_re_offer(self, msg):
        try:
            p2p = self.sess.p2p
            await p2p.close()
            p2p.reset()
            p2p.on_frame = self.sess._dispatch
            p2p._mk_pc()
            await p2p.pc.setRemoteDescription(
                RTCSessionDescription(sdp=msg["sdp"], type=msg["sdpType"]))
            await p2p.pc.setLocalDescription(await p2p.pc.createAnswer())
            await self.sig.send({"type": "answer", "to": msg["from"],
                                 "sdp": p2p.pc.localDescription.sdp,
                                 "sdpType": p2p.pc.localDescription.type,
                                 "gen": msg.get("gen")})
            print(f"    [p2p] re-offer accepted (gen {msg.get('gen', '?')})", flush=True)
        except Exception as e:
            print(f"    [p2p] re-offer error: {e}", flush=True)

    async def _show_frame(self, payload: bytes):
        img = cv2.imdecode(np.frombuffer(payload, np.uint8), cv2.IMREAD_COLOR)
        if img is None:
            return
        self.win_size = (img.shape[1], img.shape[0])
        cv2.imshow("p2pdesk", img)

    def _map(self, x, y):
        if not self.win_size:
            return x, y
        wr, hr = self.win_size
        rect = cv2.getWindowImageRect("p2pdesk")
        if rect is None or rect[2] <= 0:
            return x, y
        rx = int(x * wr / rect[2])
        ry = int(y * hr / rect[3])
        return max(0, min(wr - 1, rx)), max(0, min(hr - 1, ry))

    def _on_mouse(self, event, x, y, flags, param):
        rx, ry = self._map(x, y)
        ev = None
        if event == cv2.EVENT_MOUSEMOVE:
            ev = {"kind": "move", "x": rx, "y": ry}
        elif event == cv2.EVENT_LBUTTONDOWN:
            ev = {"kind": "down", "x": rx, "y": ry, "b": "left"}
        elif event == cv2.EVENT_LBUTTONUP:
            ev = {"kind": "up", "x": rx, "y": ry, "b": "left"}
        elif event == cv2.EVENT_RBUTTONDOWN:
            ev = {"kind": "down", "x": rx, "y": ry, "b": "right"}
        elif event == cv2.EVENT_RBUTTONUP:
            ev = {"kind": "up", "x": rx, "y": ry, "b": "right"}
        elif event == cv2.EVENT_MBUTTONDOWN:
            ev = {"kind": "down", "x": rx, "y": ry, "b": "middle"}
        elif event == cv2.EVENT_MBUTTONUP:
            ev = {"kind": "up", "x": rx, "y": ry, "b": "middle"}
        elif event == cv2.EVENT_MOUSEWHEEL:
            v = flags if flags < 2 ** 15 else flags - 2 ** 16
            ev = {"kind": "scroll", "v": v}
        if ev:
            asyncio.ensure_future(self.sess.route_send(CMD_PORT, json.dumps(ev).encode()))

    async def _quit(self):
        try:
            await asyncio.wait_for(self.sig.send({"type": "bye", "to": self.sess.peer}), 3)
        except Exception:
            pass
        try:
            cv2.destroyAllWindows()
        except Exception:
            pass
        await self.sess.close()


# ------------------------- terminal controller -------------------------

class TerminalController:
    """Remote ConPTY terminal over the same session/transport stack.
    Raw keystrokes are forwarded; ANSI output is printed as-is (Windows
    Terminal / VSCode / modern consoles render it correctly)."""

    def __init__(self, sig: Signaling, peer_id: str):
        self.sig = sig
        self.sess = Session(sig, peer_id)
        self.reneg = None
        self.alive = True
        self._ctl_lock = asyncio.Lock()

    @staticmethod
    def _enable_vt():
        # enable ANSI escape processing on classic conhost too
        try:
            import ctypes
            k32 = ctypes.windll.kernel32
            h = k32.GetStdHandle(-11)
            mode = ctypes.c_uint32()
            if k32.GetConsoleMode(h, ctypes.byref(mode)):
                k32.SetConsoleMode(h, mode.value | 0x0004)
        except Exception:
            pass

    async def run(self):
        self._enable_vt()
        self.sess.on_shell = self._on_out
        self.sess.p2p.on_state = self._on_p2p_state
        self.sig.on_bin = self.sess.on_relay_bin
        self.sig.on_msg = self._on_msg
        self.reneg = Renegotiator(self.sess, self.sig)
        p2p = self.sess.p2p
        p2p.on_frame = self.sess._dispatch
        p2p._mk_pc()
        p2p.dc = p2p.pc.createDataChannel("p2pdesk")
        p2p._bind(p2p.dc)
        await p2p.pc.setLocalDescription(await p2p.pc.createOffer())
        await self.sig.send({"type": "offer", "to": self.sess.peer,
                             "sdp": p2p.pc.localDescription.sdp,
                             "sdpType": p2p.pc.localDescription.type})
        print("    [p2p] offer sent", flush=True)
        asyncio.ensure_future(self._wait_transport())
        print("[i] connecting to terminal ... type 'exit' or press Ctrl+C to quit", flush=True)

        # ask host to spawn the pty
        await self._send_ctl({"t": "open"})
        # keystroke loop: read console keys in a thread, forward immediately
        loop = asyncio.get_event_loop()
        while self.alive and not self.sess.closed.is_set():
            data = await loop.run_in_executor(None, self._read_keys)
            if data is None:
                break
            if data:
                await self._send_ctl({"t": "in", "d": data})

        await self._quit()

    @staticmethod
    def _read_keys():
        """Non-canonical key reader on Windows (msvcrt); falls back to lines elsewhere."""
        try:
            import msvcrt
        except ImportError:
            try:
                return input() + "\r\n"
            except (EOFError, KeyboardInterrupt):
                return None
        out = []
        while True:
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch == "\x03":  # Ctrl+C -> quit session
                    return None
                if ch in ("\x00", "\xe0"):  # special key prefix
                    ext = msvcrt.getwch()
                    out.append({"\x00K": "\x1b[D", "\x00M": "\x1b[C", "\x00H": "\x1b[A",
                                "\x00P": "\x1b[B", "\x00G": "\x1b[H", "\x00O": "\x1b[F",
                                "\x00S": "\x1b[3~", "\x00R": "\x1b[2~"}.get(ch + ext, ""))
                    continue
                out.append(ch)
                continue
            if out:
                time.sleep(0.02)
                return "".join(out)
            time.sleep(0.02)
    async def _send_ctl(self, obj: dict):
        async with self._ctl_lock:
            try:
                await self.sess.route_send(SHELLCTL_PORT, json.dumps(obj).encode())
            except Exception:
                pass

    def _on_out(self, payload: bytes):
        if not payload:
            return
        if payload[0:1] == b"\x00":
            try:
                msg = json.loads(payload[1:])
                if msg.get("t") == "closed":
                    print("\n[*] remote terminal closed", flush=True)
                    self.alive = False
            except Exception:
                pass
            return
        try:
            sys.stdout.write(payload.decode("utf-8", errors="replace"))
            sys.stdout.flush()
        except Exception:
            pass

    def _on_p2p_state(self, state):
        if state in ("failed", "closed", "disconnected"):
            if self.sess and not self.sess.closed.is_set():
                print("\n[!] P2P down -> server relay, renegotiation in background", flush=True)
                self.sess.use_relay = True
                self.sess.start_pump()
                if self.reneg:
                    self.reneg.start()

    async def _wait_transport(self):
        try:
            await asyncio.wait_for(self.sess.p2p.ready.wait(), P2P_TIMEOUT)
            print("[i] transport: P2P direct", flush=True)
            return
        except asyncio.TimeoutError:
            pass
        print("[i] transport: server relay", flush=True)
        self.sess.use_relay = True
        self.sess.start_pump()
        if self.reneg:
            self.reneg.start()

    async def _on_msg(self, msg):
        t = msg.get("type")
        if t == "answer":
            try:
                await self.sess.p2p.pc.setRemoteDescription(
                    RTCSessionDescription(sdp=msg["sdp"], type=msg["sdpType"]))
                print(f"    [p2p] answer accepted (gen {msg.get('gen', '?')})", flush=True)
            except Exception as e:
                print(f"    [p2p] answer error: {e}", flush=True)
        elif t == "bye":
            print("[*] remote closed", flush=True)
            self.sess.closed.set()
            self.alive = False
        elif t == "peer-offline" and msg.get("peer") == self.sess.peer:
            print("[*] remote host went offline", flush=True)
            self.sess.closed.set()
            self.alive = False

    async def _quit(self):
        self.alive = False
        try:
            await asyncio.wait_for(self.sig.send({"type": "bye", "to": self.sess.peer}), 3)
        except Exception:
            pass
        await self.sess.close()


# ------------------------- main -------------------------

async def do_list():
    sig = Signaling()

    async def on_msg(msg):
        if msg.get("type") == "peers":
            ids = msg["peers"]
            print("online peers:" if ids else "no other peers online.", flush=True)
            for p in ids:
                print(f"  - {p}", flush=True)

    sig.on_msg = on_msg
    await sig.connect()
    await sig.send({"type": "list"})
    await asyncio.sleep(2)


async def amain():
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return
    cmd = args[0]
    if cmd == "list":
        await do_list()
        return
    sig = Signaling()
    await sig.connect()
    try:
        if cmd == "share":
            await Host(sig).run()
        elif cmd == "watch":
            if len(args) < 2:
                die("usage: python p2pdesk.py watch <peer_id>")
            await Controller(sig, args[1]).run()
        elif cmd == "term":
            if len(args) < 2:
                die("usage: python p2pdesk.py term <peer_id>")
            await TerminalController(sig, args[1]).run()
        else:
            die(f"unknown command {cmd}")
    finally:
        try:
            await sig.ws.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("\nbye", flush=True)
    finally:
        # ensure the process really exits even if aiortc/dtls tasks linger
        os._exit(0)
