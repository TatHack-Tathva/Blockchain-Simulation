"""
    Phase 3: web interface. Nodes run for real (P2P over TCP, real signalling server) on a
    background event loop; the Flask test client talks to them through the node API.
"""
import asyncio, json, socket, threading, time
import pytest

from conftest import free_port
from multiproc import SignallingProc, NodeProc, wait_until
from consensus.pow.p2p import Peer as PowPeer
from consensus.pos.p2p import Peer as PosPeer
from signalling.server import SignallingServer
from web.app import create_app


class LoopThread:
    """An asyncio loop in a background thread that hosts nodes and a signalling server."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def run(self, coro, timeout=30):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def close(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


@pytest.fixture
def env():
    lt = LoopThread()
    state = {"nodes": [], "server": None}

    async def start_server():
        state["server"] = await SignallingServer("127.0.0.1", 0).start()
    lt.run(start_server())
    url = f"ws://127.0.0.1:{state['server'].port}"

    def make_node(name, cls=PowPeer, room=None, create=False, **kw):
        async def build():
            if cls is PowPeer:
                node = PowPeer("127.0.0.1", free_port(), name, kw.pop("miner", True), "n", "n",
                               difficulty=1, block_interval=kw.pop("block_interval", 0.3), **kw)
            else:
                node = PosPeer("127.0.0.1", free_port(), name, True, "n", "n", epoch_time=2, **kw)
            await node.start_network(signalling_url=url, create_room=room if create else None,
                                     join_room=None if create else room)
            return node
        node = lt.run(build())
        state["nodes"].append(node)
        return node, create_app(node).test_client()

    yield lt, make_node, url
    for node in state["nodes"]:
        lt.run(node.stop())
    lt.run(state["server"].stop())
    lt.close()


def two_nodes(make_node):
    alice, a = make_node("alice", room="web", create=True)
    bob, b = make_node("bob", room="web")
    wait_until(lambda: any(p["connected"] for p in a.get("/api/network").json["peers"])
               and b.get("/api/node").json["height"] == 1, 20, "nodes did not connect")
    return (alice, a), (bob, b)


def test_dashboard_and_static_assets_load(env):
    lt, make_node, url = env
    node, client = make_node("alice", room="r1", create=True)
    r = client.get("/")
    assert r.status_code == 200 and b"Node Dashboard" in r.data and b"dashboard.js" in r.data
    assert client.get("/static/dashboard.js").status_code == 200
    assert client.get("/static/dashboard.css").status_code == 200
    assert "Content-Security-Policy" in r.headers


def test_node_information(env):
    lt, make_node, url = env
    node, client = make_node("alice", room="r2", create=True)
    info = client.get("/api/node").json
    for key in ("node_id", "name", "host", "port", "consensus", "role", "room", "height", "state", "signalling"):
        assert key in info
    assert info["name"] == "alice" and info["consensus"] == "pow" and info["role"] == "honest"
    assert info["room"] == "r2" and info["height"] == 1 and info["state"] == "running"
    assert info["port"] == node.port and info["node_id"] == node.node_id and info["balance"] == 50


def test_network_blockchain_block_and_transaction_information(env):
    lt, make_node, url = env
    (alice, a), (bob, b) = two_nodes(make_node)
    net = a.get("/api/network").json
    peer = net["peers"][0]
    assert peer["name"] == "bob" and peer["port"] == bob.port and peer["in_room"] and peer["connected"]
    assert net["room_members"][0]["node_id"] == bob.node_id
    blocks = b.get("/api/blocks").json["blocks"]
    assert len(blocks) == 1 and blocks[0]["height"] == 0 and blocks[0]["hash"] == alice.chain.genesis_hash
    block = b.get("/api/blocks/0").json
    assert block["transactions"][0]["type"] == "genesis" and block["transactions"][0]["status"] == "confirmed"
    assert b.get("/api/blocks/99").status_code == 404
    txs = a.get("/api/transactions").json["transactions"]
    assert txs[0]["amount"] == 50 and txs[0]["block_height"] == 0


def test_valid_transaction_submission_uses_node_validation_and_propagates(env):
    lt, make_node, url = env
    (alice, a), (bob, b) = two_nodes(make_node)
    calls = []
    original = alice.process_new_tx_message

    async def spy(msg, websocket=None):
        calls.append(msg["type"])
        return await original(msg, websocket)
    alice.process_new_tx_message = spy
    r = a.post("/api/transactions", json={"receiver": "bob", "amount": 12.5})
    assert r.status_code == 201, r.json
    assert calls == ["new_tx"]  # the exact path used for transactions from peers
    tx_id = r.json["transaction"]["id"]
    wait_until(lambda: any(t["id"] == tx_id for t in b.get("/api/transactions").json["transactions"]), 20,
               "transaction not propagated to bob")
    # 10. updates after a new block
    wait_until(lambda: b.get("/api/node").json["height"] == 2, 30, "block not produced")
    confirmed = [t for t in b.get("/api/transactions").json["transactions"] if t["id"] == tx_id]
    assert confirmed and confirmed[0]["status"] == "confirmed" and confirmed[0]["block_height"] == 1
    assert b.get("/api/blocks").json["blocks"][0]["tx_count"] == 1


@pytest.mark.parametrize("payload,fragment", [
    ({"receiver": "bob", "amount": 1000}, "balance"),
    ({"receiver": "bob", "amount": -3}, "positive"),
    ({"receiver": "bob", "amount": 0}, "positive"),
    ({"receiver": "nobody", "amount": 1}, "no known peer"),
    ({"receiver": "deploy", "amount": 1}, "coin transfers"),
    ({"receiver": "invoke", "amount": 1}, "coin transfers"),
    ({"receiver": "alice", "amount": 1}, "differ"),
])
def test_invalid_transaction_submission_rejected(env, payload, fragment):
    lt, make_node, url = env
    (alice, a), (bob, b) = two_nodes(make_node)
    r = a.post("/api/transactions", json=payload)
    assert r.status_code == 400 and fragment in r.json["error"]
    assert alice.mem_pool == []


def test_malformed_transaction_submission_rejected(env):
    lt, make_node, url = env
    node, client = make_node("alice", room="r3", create=True)
    for kwargs in [dict(data="not json", content_type="application/json"), dict(json=[1, 2]), dict(json={}),
                   dict(json={"receiver": "bob"}), dict(json={"receiver": "bob", "amount": "10"}),
                   dict(json={"receiver": 5, "amount": 1}), dict(json={"receiver": "bob", "amount": True}),
                   dict(data="x", content_type="text/plain")]:
        assert client.post("/api/transactions", **kwargs).status_code == 400
    big = client.post("/api/transactions", data="x" * (64 * 1024), content_type="application/json")
    assert big.status_code == 413
    assert client.get("/api/transactions").status_code == 200  # still healthy
    assert node.mem_pool == []


def test_updates_after_peer_changes_and_multiple_nodes(env):
    lt, make_node, url = env
    (alice, a), (bob, b) = two_nodes(make_node)
    carol, c = make_node("carol", room="web")
    wait_until(lambda: {p["name"] for p in a.get("/api/network").json["peers"] if p["connected"]} == {"bob", "carol"},
               20, "carol not visible")
    # all three dashboards end up showing the same network (carol syncs the chain after connecting)
    wait_until(lambda: len({cl.get("/api/node").json["genesis_hash"] for cl in (a, b, c)}) == 1
               and c.get("/api/node").json["has_chain"], 20, "carol did not sync the chain")
    lt.run(carol.stop())
    wait_until(lambda: {p["name"] for p in a.get("/api/network").json["peers"]} == {"bob"}, 20, "carol not removed")


def test_create_room_and_join_room_via_api(env):
    lt, make_node, url = env

    async def bare(name):
        node = PowPeer("127.0.0.1", free_port(), name, True, "n", "n", difficulty=1, block_interval=0.3)
        await node.start_network(signalling_url=url)  # connected to signalling, no room yet
        return node
    alice = lt.run(bare("alice"))
    bob = lt.run(bare("bob"))
    env_nodes = [alice, bob]
    try:
        a, b = create_app(alice).test_client(), create_app(bob).test_client()
        assert a.get("/api/node").json["has_chain"] is False
        r = a.post("/api/rooms/create", json={"room": "made-in-browser"})
        assert r.status_code == 200 and r.json["room"] == "made-in-browser"
        assert a.get("/api/node").json["height"] == 1  # the room creator starts the chain
        assert a.post("/api/rooms/create", json={"room": "made-in-browser"}).json["code"] in ("room_exists", "already_in_room")
        assert b.post("/api/rooms/join", json={"room": "missing"}).json["code"] == "no_such_room"
        assert b.post("/api/rooms/join", json={"room": "bad room!"}).status_code == 400
        r = b.post("/api/rooms/join", json={"room": "made-in-browser"})
        assert r.status_code == 200 and r.json["members"] == 1
        wait_until(lambda: b.get("/api/node").json["height"] == 1 and
                   any(p["connected"] for p in b.get("/api/network").json["peers"]), 20, "join did not connect")
        rooms = a.get("/api/rooms").json["rooms"]
        assert rooms == [{"room": "made-in-browser", "consensus": "pow", "params": {"difficulty": 1},
                          "genesis": alice.chain.genesis_hash, "members": 2}]
    finally:
        for n in env_nodes:
            lt.run(n.stop())


def collect_responses(client):
    paths = ["/", "/api/node", "/api/network", "/api/blocks", "/api/blocks/0", "/api/transactions", "/api/rooms",
             "/static/dashboard.js"]
    return [client.get(p).get_data(as_text=True) for p in paths]


def test_private_keys_are_not_exposed(env):
    lt, make_node, url = env
    (alice, a), (bob, b) = two_nodes(make_node)
    a.post("/api/transactions", json={"receiver": "bob", "amount": 1})
    for body in collect_responses(a) + collect_responses(b):
        assert "PRIVATE KEY" not in body
        for node in (alice, bob):
            assert node.wallet.private_key_pem not in body
            secret = node.wallet.private_key.to_string().hex()
            assert secret not in body
    assert "PRIVATE" not in repr(alice.wallet)


def test_forbidden_execution_and_filesystem_access_unavailable(env):
    lt, make_node, url = env
    node, client = make_node("alice", room="r4", create=True)
    allowed = {"/", "/favicon.ico", "/static/<path:name>", "/api/node", "/api/network", "/api/blocks", "/api/blocks/<int:height>",
               "/api/transactions", "/api/stake", "/api/rooms/create", "/api/rooms/join", "/api/rooms"}
    assert {r.rule for r in client.application.url_map.iter_rules()} == allowed
    for path in ["/api/exec", "/api/shell", "/api/eval", "/api/file", "/api/files?path=/etc/passwd", "/api/key",
                 "/api/contracts/deploy", "/static/../start_peer.py", "/static/..%2fstorage%2fpow%2fkeys.json",
                 "/static/../../../../etc/passwd", "/static/dashboard.html/../../app.py"]:
        assert client.get(path).status_code in (404, 405), path
        assert b"import" not in client.get(path).data
    r = client.post("/api/transactions", json={"receiver": "deploy", "amount": 1, "code": "import os"})
    assert r.status_code == 400
    assert client.post("/api/stake", json={"amount": 1}).status_code == 400  # not a PoS node
    assert node.mem_pool == [] and node.contractsDB.contracts == {}


def test_pos_stake_through_web_api(env):
    lt, make_node, url = env
    alice, a = make_node("alice", cls=PosPeer, room="posweb", create=True)
    bob, b = make_node("bob", cls=PosPeer, room="posweb")
    wait_until(lambda: b.get("/api/node").json["height"] == 1, 20, "bob has no chain")
    assert a.post("/api/transactions", json={"receiver": "bob", "amount": 5}).status_code == 201
    assert a.post("/api/stake", json={"amount": "5"}).status_code == 400
    deadline = time.time() + 20
    while True:
        r = a.post("/api/stake", json={"amount": 10})
        if r.status_code == 201 or time.time() > deadline:
            break
        assert r.status_code == 409 and r.json["retry_after"] is not None
        time.sleep(0.3)
    assert r.status_code == 201
    wait_until(lambda: b.get("/api/node").json["height"] == 2, 20, "PoS block not propagated")
    block = b.get("/api/blocks/1").json
    assert block["creator"] == alice.wallet.public_key_pem and block["staked_amt"] == 10


def test_browser_disconnect_does_not_stop_backend(tmp_path):
    """Real node process: clients that vanish mid-request must not affect the node."""
    sig = SignallingProc(tmp_path)
    alice = bob = None
    try:
        common = ["--signalling", sig.url, "--difficulty", "2", "--block-interval", "1"]
        alice = NodeProc(tmp_path, "pow", "alice", *common, "--create-room", "bd")
        bob = NodeProc(tmp_path, "pow", "bob", *common, "--join-room", "bd", "--no-miner")
        wait_until(lambda: bob.node()["height"] == 1 and alice.connected_to(bob), 30, "nodes did not connect")
        for _ in range(20):
            # half-sent request, then the "browser" goes away
            s = socket.create_connection(("127.0.0.1", alice.web_port))
            s.sendall(b"GET /api/node HTTP/1.1\r\nHost: x\r\n")
            s.close()
            # full request, closed before reading the response
            s = socket.create_connection(("127.0.0.1", alice.web_port))
            s.sendall(b"GET /api/blocks HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            s.close()
        status, reply = alice.post("/api/transactions", {"receiver": "bob", "amount": 3})
        assert status == 201
        wait_until(lambda: bob.node()["height"] == 2, 60, "backend stopped producing / propagating blocks")
        assert alice.alive() and bob.alive()
    finally:
        for p in (bob, alice, sig):
            if p is not None:
                p.stop()
