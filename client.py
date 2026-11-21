"""
Client for the inventory replicas.

    python client.py read primary        # read from the Primary
    python client.py read secondary      # read from the Secondary
    python client.py read both           # read from both, side by side
    python client.py write 9             # write stock=9 (always sent to the Primary)
    python client.py pause               # pause replication Primary -> Secondary
    python client.py resume              # resume replication
    python client.py status              # replication status of both replicas
"""

import json
import os
import sys
import urllib.error
import urllib.request

PRIMARY = os.environ.get("PRIMARY_URL", "http://127.0.0.1:8001")
SECONDARY = os.environ.get("SECONDARY_URL", "http://127.0.0.1:8002")
REPLICAS = {"primary": PRIMARY, "secondary": SECONDARY}


def _request(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="GET" if body is None else "POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"http_status": e.code, **json.loads(e.read() or b"{}")}


def read(replica):
    """Read the current stock from one replica ('primary' or 'secondary')."""
    return _request(REPLICAS[replica] + "/stock")


def write(stock, replica="primary"):
    """Write a new stock value. Clients should always write to the primary."""
    return _request(REPLICAS[replica] + "/stock", {"stock": stock})


def status(replica):
    return _request(REPLICAS[replica] + "/status")


def pause():
    return _request(PRIMARY + "/admin/pause", {})


def resume():
    return _request(PRIMARY + "/admin/resume", {})


def main(argv):
    if not argv:
        print(__doc__)
        return 1
    cmd = argv[0]
    if cmd == "read":
        targets = ["primary", "secondary"] if argv[1:] in ([], ["both"]) else [argv[1]]
        for t in targets:
            r = read(t)
            print(f"{t:<9} stock={r['stock']}  (version v{r['version']})")
    elif cmd == "write":
        r = write(int(argv[1]))
        print(f"primary   accepted write: stock={r['stock']} (version v{r['version']})")
    elif cmd in ("pause", "resume"):
        r = pause() if cmd == "pause" else resume()
        print(f"replication is now {r['replication']} (pending updates: {r['pending_updates']})")
    elif cmd == "status":
        for t in ("primary", "secondary"):
            print(f"{t:<9} {json.dumps(status(t))}")
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
