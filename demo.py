"""
End-to-end demo of replication, a stale read, and recovery/convergence.

    python demo.py           # run all three tests straight through
    python demo.py --step    # clear the screen before each test and wait for
                             # Enter after it (handy for presenting/screenshots)

Starts the Primary (port 8001) and Secondary (port 8002) as two separate
processes, then drives them with the client:

    Test 1  Replication          10 -> 12 on the Primary, Secondary catches up
    Test 2  Stale read           replication paused, Primary 10 -> 9,
                                 Secondary still answers 10
    Test 3  Recovery/convergence replication resumed, Secondary reaches 9

Every check is asserted; the run fails loudly if any requirement is not met.
Client output goes to the terminal and to logs/demo.log; each replica writes
its own log to logs/primary.log and logs/secondary.log.
"""

import os
import subprocess
import sys
import time
import urllib.error
from datetime import datetime

import client

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(HERE, "logs")
REPLICATION_LAG = 1.0  # seconds of simulated network delay per replicated update

_log_file = None


def say(msg=""):
    line = f"{datetime.now():%H:%M:%S.%f}"[:-3] + f"  {msg}" if msg else ""
    print(line, flush=True)
    _log_file.write(line + "\n")
    _log_file.flush()


STEP = "--step" in sys.argv


def banner(title):
    if STEP:
        os.system("cls" if os.name == "nt" else "clear")
    say()
    say("=" * 66)
    say(title)
    say("=" * 66)


def pause_for_enter():
    if STEP:
        input("\n[press Enter for the next test]")


def check(cond, msg):
    if not cond:
        say(f"FAIL  {msg}")
        raise SystemExit(1)
    say(f"PASS  {msg}")


def show(replica):
    r = client.read(replica)
    say(f"  read {replica:<9} -> stock={r['stock']}  (v{r['version']})")
    return r


def wait_until_secondary(expected, timeout=10.0):
    """Poll the Secondary until it returns `expected`; print every poll."""
    start = time.time()
    while time.time() - start < timeout:
        r = show("secondary")
        if r["stock"] == expected:
            return time.time() - start
        time.sleep(0.25)
    raise SystemExit(f"Secondary never reached {expected}")


def start_replicas():
    os.makedirs(LOG_DIR, exist_ok=True)
    py = sys.executable
    secondary = subprocess.Popen(
        [py, os.path.join(HERE, "replica.py"), "--role", "secondary", "--port", "8002",
         "--log-dir", LOG_DIR],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    primary = subprocess.Popen(
        [py, os.path.join(HERE, "replica.py"), "--role", "primary", "--port", "8001",
         "--peer", client.SECONDARY, "--delay", str(REPLICATION_LAG), "--log-dir", LOG_DIR],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for name in ("primary", "secondary"):
        for _ in range(50):
            try:
                client.read(name)
                break
            except (urllib.error.URLError, OSError):
                time.sleep(0.1)
        else:
            raise SystemExit(f"{name} did not start - is port 8001/8002 already in use?")
    return primary, secondary


def main():
    global _log_file
    os.makedirs(LOG_DIR, exist_ok=True)
    _log_file = open(os.path.join(LOG_DIR, "demo.log"), "w", encoding="utf-8")

    procs = start_replicas()
    try:
        say(f"Primary   running at {client.PRIMARY}  (pid {procs[0].pid})")
        say(f"Secondary running at {client.SECONDARY}  (pid {procs[1].pid})")
        say(f"Simulated replication lag: {REPLICATION_LAG}s per update")

        # ------------------------------------------------------------------
        banner("TEST 1 - REPLICATION (Primary -> Secondary)")
        say("Initial state:")
        p, s = show("primary"), show("secondary")
        check(p["stock"] == s["stock"] == 10, "both replicas start at stock=10")

        say("Client tries to write to the Secondary (should be refused):")
        r = client.write(99, replica="secondary")
        say(f"  write secondary  -> HTTP {r.get('http_status')}: {r.get('error')}")
        check(r.get("http_status") == 403, "Secondary rejects client writes; all writes go to the Primary")

        say("Client writes stock=12 to the Primary:")
        w = client.write(12)
        say(f"  write primary    -> accepted stock={w['stock']} (v{w['version']})")
        p = show("primary")
        check(p["stock"] == 12, "Primary has the new value 12 immediately")

        say("Polling the Secondary until the update arrives:")
        took = wait_until_secondary(12)
        check(True, f"Secondary eventually received 12 (after {took:.2f}s)")

        p, s = show("primary"), show("secondary")
        check(p["stock"] == s["stock"] == 12 and p["version"] == s["version"],
              f"both replicas return stock=12 at the same version v{p['version']} - replication complete")

        say("Reset to stock=10 (also replicated) so the next test starts from 10:")
        client.write(10)
        wait_until_secondary(10)
        p, s = show("primary"), show("secondary")
        check(p["stock"] == s["stock"] == 10, "both replicas back at stock=10")

        pause_for_enter()

        # ------------------------------------------------------------------
        banner("TEST 2 - STALE READ (replication to Secondary delayed)")
        st = client.pause()
        say(f"Replication paused -> replication={st['replication']}")

        say("Client writes stock=9 to the Primary:")
        w = client.write(9)
        say(f"  write primary    -> accepted stock={w['stock']} (v{w['version']})")

        say("Reading both replicas several times over 3 seconds (3x the normal lag):")
        for i in range(4):
            p, s = show("primary"), show("secondary")
            check(p["stock"] == 9, f"[{i + 1}/4] Primary returns 9 (latest value)")
            check(s["stock"] == 10, f"[{i + 1}/4] Secondary returns 10 -> STALE READ "
                                    f"(v{s['version']} vs primary v{p['version']})")
            if i < 3:
                time.sleep(1.0)

        st = client.status("primary")
        say(f"  primary status   -> replication={st['replication']}, "
            f"pending_updates={st['pending_updates']}, "
            f"secondary_acked_version=v{st['secondary_acked_version']}")
        check(st["pending_updates"] == 1, "the update to 9 is still queued on the Primary, not lost")

        pause_for_enter()

        # ------------------------------------------------------------------
        banner("TEST 3 - RECOVERY AND CONVERGENCE (replication resumed)")
        st = client.resume()
        say(f"Replication resumed -> replication={st['replication']}")

        say("Polling the Secondary until it catches up:")
        took = wait_until_secondary(9)
        check(True, f"Secondary eventually updated to 9 (after {took:.2f}s)")

        p, s = show("primary"), show("secondary")
        check(p["stock"] == s["stock"] == 9 and p["version"] == s["version"],
              f"both replicas return stock=9 at version v{p['version']} - CONVERGED")

        st = client.status("primary")
        say(f"  primary status   -> replication={st['replication']}, "
            f"pending_updates={st['pending_updates']}, "
            f"secondary_acked_version=v{st['secondary_acked_version']}")
        check(st["pending_updates"] == 0 and st["secondary_acked_version"] == p["version"],
              "replication queue drained; Secondary acknowledged the latest version")

        say()
        say("=" * 66)
        say("ALL TESTS PASSED")
        say("=" * 66)
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait()
        _log_file.close()


if __name__ == "__main__":
    main()
