#!/usr/bin/env python3
"""
p2pdesk signaling server (ws only, no TLS needed)
- peer register / list / offer / answer / ice / bye relay
- binary relay channel as fallback when P2P (WebRTC) fails
Binary frame format: [0x01][target_id utf8][0x00][payload]
Forwarded frame:     [0x01][src_id utf8][0x00][payload]
Run:  python signaling_server.py   (env: P2PDESK_PORT, P2PDESK_TOKEN)
"""
import asyncio
import json
import os
import sys

import websockets


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

PORT = int(os.environ.get("P2PDESK_PORT", "9000"))
TOKEN = os.environ.get("P2PDESK_TOKEN", "")

if not TOKEN:
    print("[!] P2PDESK_TOKEN not set. Copy server/.env.example to server/.env and fill it in.")
    raise SystemExit(1)

peers = {}  # id -> websocket


def j(obj):
    return json.dumps(obj, ensure_ascii=False)


async def handler(ws):
    peer_id = None
    try:
        async for raw in ws:
            # ---- binary: relay fallback channel ----
            if isinstance(raw, bytes):
                if peer_id is None or len(raw) < 3 or raw[0] != 0x01:
                    continue
                try:
                    z = raw.index(b"\x00", 1)
                    target = raw[1:z].decode()
                    payload = raw[z + 1:]
                except ValueError:
                    continue
                tws = peers.get(target)
                if tws is not None and tws is not ws:
                    try:
                        await tws.send(b"\x01" + peer_id.encode() + b"\x00" + payload)
                    except Exception:
                        pass
                continue
            # ---- text: JSON signaling ----
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("type")
            if t == "register":
                if msg.get("token") != TOKEN:
                    await ws.send(j({"type": "error", "msg": "token invalid"}))
                    continue
                pid = str(msg.get("id", "")).strip()
                if not pid:
                    await ws.send(j({"type": "error", "msg": "id empty"}))
                    continue
                if pid in peers and peers[pid] is not ws:
                    await ws.send(j({"type": "error", "msg": "id already taken"}))
                    continue
                peer_id = pid
                peers[pid] = ws
                await ws.send(j({"type": "registered", "ok": True}))
                print(f"[+] online: {pid} ({len(peers)} peers)", flush=True)
            elif t == "list":
                await ws.send(j({"type": "peers", "peers": [p for p in peers if p != peer_id]}))
            elif t in ("offer", "answer", "ice", "bye"):
                target = msg.get("to")
                tws = peers.get(target)
                if tws is None:
                    await ws.send(j({"type": "error", "msg": f"peer '{target}' not online"}))
                else:
                    msg["from"] = peer_id
                    try:
                        await tws.send(j(msg))
                    except Exception:
                        pass
            elif t == "ping":
                await ws.send(j({"type": "pong"}))
            else:
                await ws.send(j({"type": "error", "msg": f"unknown type {t}"}))
    except websockets.ConnectionClosed:
        pass
    finally:
        if peer_id and peers.get(peer_id) is ws:
            del peers[peer_id]
            print(f"[-] offline: {peer_id} ({len(peers)} peers)", flush=True)
            for w in list(peers.values()):
                try:
                    await w.send(j({"type": "peer-offline", "peer": peer_id}))
                except Exception:
                    pass


async def main():
    print(f"p2pdesk signaling server listening on ws://0.0.0.0:{PORT}", flush=True)
    print(f"auth token: {TOKEN}", flush=True)
    async with websockets.serve(handler, "0.0.0.0", PORT, max_size=16 * 1024 * 1024, ping_interval=20):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("bye", flush=True)
        sys.exit(0)
