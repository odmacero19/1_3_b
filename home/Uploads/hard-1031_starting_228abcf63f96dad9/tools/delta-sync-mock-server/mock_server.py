#!/usr/bin/env python3
"""
Mock server for the delta sync protocol (see HttpDeltaSyncService). Standard library only.

    python3 mock_server.py [--host 127.0.0.1] [--port 8080] [--reject-substring reject] [--mode per-record|whole-batch]

Point the app at it with DeltaSyncPreferences.endpoint = http://<host>:<port>/sync/v1
(the default, http://10.0.2.2:8080/sync/v1, is this server as seen from the Android emulator).

  POST /sync/v1/deltas   push. Records whose url contains --reject-substring are refused:
                           per-record  -> 200 with a DeltaPushResponse naming the refused records, the rest is stored
                           whole-batch -> 422 and nothing is stored when the batch contains a refused record
  GET  /sync/v1/deltas   pull. Returns what other devices pushed, `cursor` is a sequence number.
  GET  /_admin/state     JSON: stored records and a log of requests
  POST /_admin/fail?next=N&code=503   make the next N pushes fail with that status
  POST /_admin/ghost?next=N           answer the next N pushes with 200 and a rejection naming a chapter that was
                                      never sent (nothing is stored), i.e. a misbehaving server
  POST /_admin/reset

Bodies are protobuf; only the few fields needed here are decoded by hand.
"""
import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def read_varint(buf, i):
    shift = result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def signed64(v):
    return v - (1 << 64) if v >= 1 << 63 else v


def fields(buf):
    """Yields (field_number, wire_type, value) where value is int for varint, bytes for length-delimited."""
    i = 0
    while i < len(buf):
        key, i = read_varint(buf, i)
        num, wt = key >> 3, key & 7
        if wt == 0:
            v, i = read_varint(buf, i)
        elif wt == 2:
            n, i = read_varint(buf, i)
            v, i = buf[i:i + n], i + n
        elif wt == 1:
            v, i = buf[i:i + 8], i + 8
        elif wt == 5:
            v, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f"unsupported wire type {wt}")
        yield num, wt, v


def varint(v):
    v &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def ld(num, payload):
    return varint(num << 3 | 2) + varint(len(payload)) + payload


def vi(num, value):
    return varint(num << 3) + varint(value)


class Record:
    def __init__(self, seq, device, kind, raw, source, manga_url, chapter_url):
        self.seq, self.device, self.kind, self.raw = seq, device, kind, raw
        self.source, self.manga_url, self.chapter_url = source, manga_url, chapter_url

    def describe(self):
        return {"seq": self.seq, "device": self.device, "kind": self.kind, "manga": self.manga_url, "chapter": self.chapter_url}


class State:
    def __init__(self, reject_substring, mode):
        self.lock = threading.Lock()
        self.reject_substring, self.mode = reject_substring, mode
        self.reset()

    def reset(self):
        self.records, self.log, self.seq = [], [], 0
        self.fail_next, self.fail_code, self.ghost_next = 0, 503, 0


def parse_push(body):
    device = batch = ""
    items = []  # (kind, raw, source, manga_url, chapter_url)
    for num, _, v in fields(body):
        if num == 1:
            device = v.decode()
        elif num == 2:
            batch = v.decode()
        elif num == 3:  # DeltaManga { 1: BackupManga { 1: source, 2: url } }
            source = url = None
            for n2, _, v2 in fields(v):
                if n2 == 1:
                    for n3, _, v3 in fields(v2):
                        if n3 == 1:
                            source = signed64(v3)
                        elif n3 == 2:
                            url = v3.decode()
            items.append((1, v, source, url, ""))
        elif num == 4:  # DeltaChapter { 1: mangaSource, 2: mangaUrl, 3: BackupChapter { 1: url } }
            source = murl = curl = None
            for n2, _, v2 in fields(v):
                if n2 == 1:
                    source = signed64(v2)
                elif n2 == 2:
                    murl = v2.decode()
                elif n2 == 3:
                    for n3, _, v3 in fields(v2):
                        if n3 == 1:
                            curl = v3.decode()
            items.append((2, v, source, murl, curl))
    return device, batch, items


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def reply(self, code, body=b"", ctype="application/x-protobuf"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            url = urlparse(self.path)
            n = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(n)

            if url.path == "/_admin/reset":
                with state.lock:
                    state.reset()
                return self.reply(204)
            if url.path == "/_admin/fail":
                q = parse_qs(url.query)
                with state.lock:
                    state.fail_next = int(q.get("next", ["1"])[0])
                    state.fail_code = int(q.get("code", ["503"])[0])
                return self.reply(204)
            if url.path == "/_admin/ghost":
                with state.lock:
                    state.ghost_next = int(parse_qs(url.query).get("next", ["1"])[0])
                return self.reply(204)
            if url.path != "/sync/v1/deltas":
                return self.reply(404)

            try:
                device, batch, items = parse_push(body)
            except Exception as e:  # noqa: BLE001
                return self.reply(400, str(e).encode(), "text/plain")

            with state.lock:
                bad = [it for it in items if state.reject_substring in (it[4] or it[3] or "")]
                entry = {"batch": batch, "device": device, "count": len(items), "bad": len(bad)}
                if state.fail_next > 0:
                    state.fail_next -= 1
                    entry["status"] = state.fail_code
                    state.log.append(entry)
                    return self.reply(state.fail_code)
                if state.ghost_next > 0:
                    state.ghost_next -= 1
                    entry.update(status=200, ghost=True)
                    state.log.append(entry)
                    return self.reply(200, ld(1, vi(1, 2) + vi(2, 1) + ld(3, b"/ghost") + ld(4, b"/ghost/c1") + ld(5, b"nope")))
                if bad and state.mode == "whole-batch":
                    entry["status"] = 422
                    state.log.append(entry)
                    return self.reply(422)
                resp = b""
                for kind, raw, source, murl, curl in items:
                    if (kind, raw, source, murl, curl) in bad:
                        reason = f"refused {curl or murl}".encode()
                        resp += ld(1, vi(1, kind) + vi(2, source) + ld(3, murl.encode()) +
                                   (ld(4, curl.encode()) if curl else b"") + ld(5, reason))
                        continue
                    state.seq += 1
                    state.records.append(Record(state.seq, device, kind, raw, source, murl, curl))
                entry["status"] = 200
                state.log.append(entry)
            self.reply(200, resp)

        def do_GET(self):
            url = urlparse(self.path)
            q = parse_qs(url.query)
            if url.path == "/_admin/state":
                with state.lock:
                    out = {"records": [r.describe() for r in state.records], "log": state.log}
                return self.reply(200, json.dumps(out).encode(), "application/json")
            if url.path != "/sync/v1/deltas":
                return self.reply(404)

            device = self.headers.get("X-Device-Id", "")
            cursor = int((q.get("cursor") or ["0"])[0] or 0)
            limit = int((q.get("limit") or ["100"])[0])
            with state.lock:
                todo = [r for r in state.records if r.seq > cursor and r.device != device]
                page, more = todo[:limit], len(todo) > limit
                last = page[-1].seq if page else max([cursor] + [r.seq for r in state.records])
            body = ld(1, str(last).encode()) + (vi(2, 1) if more else b"")
            for r in page:
                body += ld(3 if r.kind == 1 else 4, r.raw)
            self.reply(200, body)

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--reject-substring", default="reject")
    ap.add_argument("--mode", choices=["per-record", "whole-batch"], default="per-record")
    a = ap.parse_args()
    server = ThreadingHTTPServer((a.host, a.port), make_handler(State(a.reject_substring, a.mode)))
    print(f"listening on http://{a.host}:{server.server_address[1]}/sync/v1", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
