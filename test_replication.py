"""
Automated tests for the three lab requirements.

    python -m unittest -v

Each test starts a fresh Primary/Secondary pair on ports 8101/8102 so the tests
do not clash with a demo that may be running on 8001/8002.
"""

import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error

import client

HERE = os.path.dirname(os.path.abspath(__file__))
P_URL, S_URL = "http://127.0.0.1:8101", "http://127.0.0.1:8102"


class ReplicationTests(unittest.TestCase):
    def setUp(self):
        client.REPLICAS.update(primary=P_URL, secondary=S_URL)
        client.PRIMARY, client.SECONDARY = P_URL, S_URL
        self.logdir = tempfile.mkdtemp()
        py, rep = sys.executable, os.path.join(HERE, "replica.py")
        quiet = dict(stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs = [
            subprocess.Popen([py, rep, "--role", "secondary", "--port", "8102",
                              "--log-dir", self.logdir], **quiet),
            subprocess.Popen([py, rep, "--role", "primary", "--port", "8101", "--peer", S_URL,
                              "--delay", "0.3", "--log-dir", self.logdir], **quiet),
        ]
        for name in ("primary", "secondary"):
            for _ in range(50):
                try:
                    client.read(name)
                    break
                except (urllib.error.URLError, OSError):
                    time.sleep(0.1)

    def tearDown(self):
        for p in self.procs:
            p.terminate()
            p.wait()

    def wait_secondary(self, expected, timeout=5):
        end = time.time() + timeout
        while time.time() < end:
            if client.read("secondary")["stock"] == expected:
                return True
            time.sleep(0.05)
        return False

    def test_1_replication(self):
        self.assertEqual(client.read("primary")["stock"], 10)
        self.assertEqual(client.read("secondary")["stock"], 10)
        client.write(12)
        self.assertEqual(client.read("primary")["stock"], 12)
        self.assertTrue(self.wait_secondary(12), "Secondary never received 12")
        self.assertEqual(client.read("primary"), {**client.read("secondary"), "replica": "primary"})

    def test_secondary_rejects_writes(self):
        self.assertEqual(client.write(5, replica="secondary").get("http_status"), 403)
        self.assertEqual(client.read("secondary")["stock"], 10)

    def test_2_stale_read_then_3_convergence(self):
        client.pause()
        client.write(9)
        time.sleep(1.0)  # well past the 0.3s lag: only the pause can explain staleness
        self.assertEqual(client.read("primary")["stock"], 9)
        self.assertEqual(client.read("secondary")["stock"], 10)  # stale read
        self.assertEqual(client.status("primary")["pending_updates"], 1)

        client.resume()
        self.assertTrue(self.wait_secondary(9), "Secondary never converged to 9")
        p, s = client.read("primary"), client.read("secondary")
        self.assertEqual((p["stock"], p["version"]), (s["stock"], s["version"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
