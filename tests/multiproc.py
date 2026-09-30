"""
    Helpers to run real node / signalling server processes for integration tests.
    Every node is a separate `python start_peer.py ...` process; tests talk to nodes only
    through their HTTP API and observe behaviour, never by calling internals.
"""
import json, os, signal, subprocess, sys, time

import requests

from conftest import ROOT, free_port


class Proc:
    def __init__(self, args, log_path, env=None):
        self.log_path = log_path
        self.log = open(log_path, "w")
        self.proc = subprocess.Popen([sys.executable] + args, cwd=ROOT, stdout=self.log, stderr=subprocess.STDOUT,
                                     env=env or os.environ.copy(), stdin=subprocess.DEVNULL)

    def alive(self):
        return self.proc.poll() is None

    def stop(self, sig=signal.SIGTERM, timeout=20):
        if self.alive():
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()
        return self.proc.returncode

    def tail(self, n=60):
        try:
            with open(self.log_path) as f:
                return "".join(f.readlines()[-n:])
        except OSError:
            return ""


class SignallingProc(Proc):
    def __init__(self, tmp_path, port=None):
        self.port = port or free_port()
        super().__init__(["-m", "signalling.server", "--host", "127.0.0.1", "--port", str(self.port)],
                         str(tmp_path / f"signalling-{self.port}-{time.time_ns()}.log"))
        wait_until(lambda: ws_ping(self.port), 15, "signalling server did not start")

    @property
    def url(self):
        return f"ws://127.0.0.1:{self.port}"


class NodeProc(Proc):
    def __init__(self, tmp_path, consensus, name, *extra, port=None, web_port=None, storage=None):
        self.name = name
        self.port = port or free_port()
        self.web_port = web_port or free_port()
        env = os.environ.copy()
        env["BLOCKCHAIN_STORAGE_DIR"] = str(storage or (tmp_path / "storage"))
        args = ["start_peer.py", "--consensus", consensus, "--host", "127.0.0.1", "--port", str(self.port),
                "--name", name, "--web-port", str(self.web_port), "--headless", *extra]
        super().__init__(args, str(tmp_path / f"{name}-{time.time_ns()}.log"), env)
        wait_until(lambda: self.try_get("/api/node") is not None, 30, f"{name} did not start:\n{self.tail()}")

    @property
    def base(self):
        return f"http://127.0.0.1:{self.web_port}"

    def try_get(self, path):
        try:
            r = requests.get(self.base + path, timeout=5)
            return r.json() if r.status_code == 200 else None
        except (requests.RequestException, ValueError):
            return None

    def get(self, path):
        r = requests.get(self.base + path, timeout=15)
        r.raise_for_status()
        return r.json()

    def post(self, path, payload):
        r = requests.post(self.base + path, json=payload, timeout=30)
        return r.status_code, r.json()

    def node(self):
        return self.get("/api/node")

    def network(self):
        return self.get("/api/network")

    def blocks(self):
        return self.get("/api/blocks")["blocks"]

    def transactions(self):
        return self.get("/api/transactions")["transactions"]

    def connected_to(self, other):
        return any(p["node_id"] == other.node()["node_id"] and p["connected"] for p in self.network()["peers"])

    def stake(self, amount, deadline=60):
        """Stakes through the node's own mechanism; retries while the registration window is closed."""
        end = time.time() + deadline
        while time.time() < end:
            status, reply = self.post("/api/stake", {"amount": amount})
            if status == 201:
                return reply
            if status != 409:
                raise AssertionError(f"stake failed: {reply}")
            time.sleep(min(reply.get("retry_after") or 0.5, 2))
        raise AssertionError("stake registration window never opened")


def ws_ping(port):
    from websockets.sync.client import connect
    try:
        with connect(f"ws://127.0.0.1:{port}", open_timeout=2) as ws:
            ws.send(json.dumps({"type": "ping", "ref": "probe"}))
            return json.loads(ws.recv(timeout=2)).get("type") == "pong"
    except (OSError, TimeoutError, Exception):
        return False


def wait_until(cond, timeout, message="condition not met", interval=0.2):
    """Polls cond(); the timeout only converts a hang into a test failure."""
    end = time.time() + timeout
    last_error = None
    while time.time() < end:
        try:
            if cond():
                return True
        except Exception as e:  # node not ready yet / transient HTTP error
            last_error = e
        time.sleep(interval)
    raise AssertionError(f"{message} (last error: {last_error})")
