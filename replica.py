"""
A single inventory replica, run as its own HTTP server process.

    python replica.py --role primary   --port 8001 --peer http://127.0.0.1:8002
    python replica.py --role secondary --port 8002

Primary
    * Accepts writes (POST /stock).
    * Applies the write locally, gives it a new version number, and puts it on
      a replication queue.
    * A background replication thread ships each queued update to the
      Secondary (POST /replicate), retrying until the Secondary acknowledges it.
    * Replication can be paused and resumed (POST /admin/pause, /admin/resume)
      to simulate a slow or partitioned link to the Secondary.

Secondary
    * Rejects client writes (it is read-only to clients).
    * Applies updates pushed by the Primary, ignoring any whose version is not
      newer than what it already has (so retries/duplicates are harmless).

Both
    * GET /stock    -> {"replica", "stock", "version"}
    * GET /status   -> the above plus replication details
"""

import argparse
import json
import logging
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INITIAL_STOCK = 10


class ReplicaState:
    def __init__(self, role, peer_url=None, replication_delay=0.0):
        self.role = role
        self.peer_url = peer_url
        self.replication_delay = replication_delay  # simulated network lag (s)
        self.stock = INITIAL_STOCK
        self.version = 1
        self.lock = threading.Lock()

        # Primary-only replication machinery
        self.queue = queue.Queue()
        self.running = threading.Event()  # set = replication allowed
        self.running.set()
        self.replicated_version = 1  # highest version the Secondary has acked

    # -------- reads ---------------------------------------------------------
    def snapshot(self):
        with self.lock:
            return {"replica": self.role, "stock": self.stock, "version": self.version}

    def status(self):
        s = self.snapshot()
        if self.role == "primary":
            s.update(
                replication="running" if self.running.is_set() else "PAUSED",
                # updates written on the primary but not yet acked by the secondary
                pending_updates=s["version"] - self.replicated_version,
                secondary_acked_version=self.replicated_version,
                replication_delay_s=self.replication_delay,
            )
        return s

    # -------- writes --------------------------------------------------------
    def client_write(self, new_stock):
        with self.lock:
            old = self.stock
            self.version += 1
            self.stock = new_stock
            update = {"stock": self.stock, "version": self.version}
        log.info("WRITE   stock %s -> %s (v%s) applied on primary", old, new_stock, update["version"])
        self.queue.put(update)
        log.info("QUEUE   v%s queued for replication (replication=%s)",
                 update["version"],
                 "running" if self.running.is_set() else "PAUSED")
        return update

    def apply_replicated(self, stock, version):
        with self.lock:
            if version <= self.version:
                log.info("REPL-IN v%s ignored (already at v%s)", version, self.version)
                return False
            old = self.stock
            self.stock, self.version = stock, version
        log.info("REPL-IN stock %s -> %s (v%s) applied on secondary", old, stock, version)
        return True

    # -------- replication thread (primary only) ----------------------------
    def replication_loop(self):
        while True:
            update = self.queue.get()  # blocks until there is something to send
            while True:
                if not self.running.is_set():
                    log.info("REPL    paused - holding v%s until replication resumes", update["version"])
                    self.running.wait()
                    log.info("REPL    resumed - sending held v%s", update["version"])
                if self.replication_delay:
                    time.sleep(self.replication_delay)
                if not self.running.is_set():
                    continue  # paused while we were "on the wire"; hold it
                try:
                    post_json(self.peer_url + "/replicate", update)
                    with self.lock:
                        self.replicated_version = max(self.replicated_version, update["version"])
                    log.info("REPL    v%s (stock=%s) acknowledged by secondary",
                             update["version"], update["stock"])
                    break
                except (urllib.error.URLError, OSError) as exc:
                    log.warning("REPL    v%s send failed (%s); retrying in 0.5s", update["version"], exc)
                    time.sleep(0.5)
            self.queue.task_done()


def post_json(url, body, timeout=5):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read() or b"{}")


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n) or b"{}")

        def log_message(self, *args):  # silence default access log
            pass

        def do_GET(self):
            if self.path == "/stock":
                snap = state.snapshot()
                log.info("READ    client read stock=%s (v%s)", snap["stock"], snap["version"])
                self._send(200, snap)
            elif self.path == "/status":
                self._send(200, state.status())
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path == "/stock":
                if state.role != "primary":
                    log.info("REJECT  client write refused - secondary is read-only")
                    return self._send(403, {"error": "writes must go to the primary"})
                new_stock = int(self._body()["stock"])
                self._send(200, state.client_write(new_stock))
            elif self.path == "/replicate" and state.role == "secondary":
                b = self._body()
                applied = state.apply_replicated(int(b["stock"]), int(b["version"]))
                self._send(200, {"applied": applied, **state.snapshot()})
            elif self.path == "/admin/pause" and state.role == "primary":
                state.running.clear()
                log.info("ADMIN   replication to secondary PAUSED")
                self._send(200, state.status())
            elif self.path == "/admin/resume" and state.role == "primary":
                state.running.set()
                log.info("ADMIN   replication to secondary RESUMED")
                self._send(200, state.status())
            else:
                self._send(404, {"error": "not found"})

    return Handler


def main():
    global log
    p = argparse.ArgumentParser(description="Inventory replica")
    p.add_argument("--role", choices=["primary", "secondary"], required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--peer", help="Secondary base URL (primary only)")
    p.add_argument("--delay", type=float, default=1.0,
                   help="simulated replication lag in seconds (primary only, default 1.0)")
    p.add_argument("--log-dir", default="logs")
    a = p.parse_args()
    if a.role == "primary" and not a.peer:
        p.error("--peer is required for the primary")

    os.makedirs(a.log_dir, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d [%(name)-9s] %(message)s", "%H:%M:%S")
    log = logging.getLogger(a.role.upper())
    log.setLevel(logging.INFO)
    for h in (logging.StreamHandler(),
              logging.FileHandler(os.path.join(a.log_dir, f"{a.role}.log"), mode="w", encoding="utf-8")):
        h.setFormatter(fmt)
        log.addHandler(h)

    state = ReplicaState(a.role, a.peer, a.delay if a.role == "primary" else 0.0)
    if a.role == "primary":
        threading.Thread(target=state.replication_loop, daemon=True).start()

    server = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(state))
    log.info("START   %s listening on 127.0.0.1:%s with stock=%s (v%s)%s",
             a.role, a.port, state.stock, state.version,
             f", replicating to {a.peer} with {a.delay}s lag" if a.role == "primary" else "")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


log = logging.getLogger("replica")

if __name__ == "__main__":
    main()
