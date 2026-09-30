import asyncio, json, os, socket, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run(coro):
    return asyncio.run(coro)


async def wait_for(condition, timeout=20.0, interval=0.05):
    """Polls `condition` until true; the timeout only turns a hang into a test failure."""
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if condition():
            return True
        await asyncio.sleep(interval)
    return bool(condition())


class FakeWebSocket:
    """Captures what a node sends on a connection, for handler level tests."""

    def __init__(self, remote=("127.0.0.1", 40000)):
        self.sent = []
        self.remote_address = remote
        self.closed = False

    async def send(self, data):
        self.sent.append(json.loads(data))

    async def close(self):
        self.closed = True

    def of_type(self, t):
        return [m for m in self.sent if m.get("type") == t]
