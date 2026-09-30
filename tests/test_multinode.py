"""
    Phase 2: REAL multi-process integration. Every node is a separate OS process started
    with `start_peer.py`; no node has a bootstrap peer configured - they find each other
    only through the signalling server and then talk directly over P2P websockets.
    The tests observe behaviour only through each node's HTTP API.
"""
import time
import pytest

from multiproc import SignallingProc, NodeProc, wait_until

EPOCH = "3"  # PoS epoch length (seconds) used by the test network


@pytest.fixture
def procs():
    started = []
    yield started
    failed = False
    for p in reversed(started):
        p.stop()
    for p in started:
        print(f"\n===== {p.log_path} =====\n{p.tail(40)}")


def by_name(node, name):
    return next(p for p in node.network()["peers"] if p["name"] == name)


def test_two_process_pos_flow_without_bootstrap(tmp_path, procs):
    sig = SignallingProc(tmp_path)
    procs.append(sig)
    alice = NodeProc(tmp_path, "pos", "alice", "--signalling", sig.url, "--create-room", "demo", "--epoch-time", EPOCH)
    procs.append(alice)
    bob = NodeProc(tmp_path, "pos", "bob", "--signalling", sig.url, "--join-room", "demo", "--epoch-time", EPOCH)
    procs.append(bob)
    a_id, b_id = alice.node()["node_id"], bob.node()["node_id"]

    # 1-2. discovery through signalling, then a direct P2P connection
    wait_until(lambda: alice.connected_to(bob) and bob.connected_to(alice), 30, "nodes did not connect")
    assert alice.node()["room"] == bob.node()["room"] == "demo"
    assert {m["node_id"] for m in alice.network()["room_members"]} == {b_id}
    # bob had no chain and got alice's (same genesis) over P2P
    wait_until(lambda: bob.node()["genesis_hash"] == alice.node()["genesis_hash"], 30, "bob did not sync the chain")

    # 4-5. transaction propagation
    status, reply = alice.post("/api/transactions", {"receiver": "bob", "amount": 10})
    assert status == 201, reply
    tx_id = reply["transaction"]["id"]
    wait_until(lambda: any(t["id"] == tx_id and t["status"] == "pending" for t in bob.transactions()), 20,
               "transaction did not reach bob")

    # 6-9 + PoS: alice stakes through the node, wins (sole staker), bob validates and appends
    alice.stake(20)
    wait_until(lambda: bob.node()["height"] == 2, 30, "bob did not receive alice's block")
    block = bob.get("/api/blocks/1")
    assert block["creator"] == alice.node()["public_key"]
    assert [s["amount"] for s in block["stakers"]] == [20] and block["staked_amt"] == 20
    assert any(t["id"] == tx_id and t["status"] == "confirmed" for t in bob.transactions())
    assert bob.node()["balance"] == 10
    assert alice.blocks()[0]["hash"] == bob.blocks()[0]["hash"]
    assert bob.node()["recent_rejections"] == []

    # multi-node PoS: now bob stakes and creates a block that alice validates
    status, reply = bob.post("/api/transactions", {"receiver": "alice", "amount": 3})
    assert status == 201, reply
    wait_until(lambda: len([t for t in alice.transactions() if t["status"] == "pending"]) == 1, 20, "tx to alice")
    bob.stake(5)
    wait_until(lambda: alice.node()["height"] == 3, 30, "alice did not receive bob's block")
    assert alice.get("/api/blocks/2")["creator"] == bob.node()["public_key"]
    assert alice.blocks()[0]["hash"] == bob.blocks()[0]["hash"]

    # 3. direct connection is independent of signalling: stop the signalling server
    sig.stop()
    wait_until(lambda: alice.node()["signalling"]["state"] != "joined", 20, "signalling loss not detected")
    status, reply = alice.post("/api/transactions", {"receiver": "bob", "amount": 1})
    assert status == 201
    wait_until(lambda: any(t["id"] == reply["transaction"]["id"] for t in bob.transactions()), 20,
               "P2P propagation failed without signalling")
    assert alice.connected_to(bob)

    # reconnect + automatic rejoin when the signalling server comes back (same port)
    sig2 = SignallingProc(tmp_path, port=sig.port)
    procs.append(sig2)
    wait_until(lambda: alice.node()["signalling"]["state"] == "joined" and bob.node()["signalling"]["state"] == "joined",
               30, "clients did not rejoin")
    wait_until(lambda: {m["node_id"] for m in alice.network()["room_members"]} == {b_id}, 20, "room not restored")
    assert alice.node()["signalling"]["connects"] >= 2

    # disconnect: bob leaves (process stops) -> alice removes the stale peer
    bob.stop()
    wait_until(lambda: all(p["node_id"] != b_id for p in alice.network()["peers"]), 30, "stale peer not removed")
    assert alice.network()["room_members"] == []


def test_two_process_pow_block_propagation(tmp_path, procs):
    sig = SignallingProc(tmp_path)
    procs.append(sig)
    common = ["--signalling", sig.url, "--difficulty", "3", "--block-interval", "1"]
    alice = NodeProc(tmp_path, "pow", "alice", *common, "--create-room", "powroom")
    procs.append(alice)
    bob = NodeProc(tmp_path, "pow", "bob", *common, "--join-room", "powroom", "--no-miner")
    procs.append(bob)
    wait_until(lambda: alice.connected_to(bob) and bob.connected_to(alice), 30, "nodes did not connect")
    wait_until(lambda: bob.node()["height"] == 1, 30, "bob has no chain")
    status, reply = alice.post("/api/transactions", {"receiver": "bob", "amount": 7})
    assert status == 201, reply
    wait_until(lambda: bob.node()["height"] == 2, 60, "mined block not propagated")
    block = bob.get("/api/blocks/1")
    assert block["creator"] == alice.node()["public_key"] and block["hash"].startswith("000")
    # invalid (negative / overspending) submissions are rejected by the same validation path
    assert alice.post("/api/transactions", {"receiver": "bob", "amount": -5})[0] == 400
    assert bob.post("/api/transactions", {"receiver": "alice", "amount": 1000})[0] == 400


def test_two_process_poa_authority_flow(tmp_path, procs):
    sig = SignallingProc(tmp_path)
    procs.append(sig)
    common = ["--signalling", sig.url, "--round-time", "4", "--block-interval", "0.5"]
    admin = NodeProc(tmp_path, "poa", "admin", *common, "--create-room", "poaroom")
    procs.append(admin)
    bob = NodeProc(tmp_path, "poa", "bob", *common, "--join-room", "poaroom")
    procs.append(bob)
    wait_until(lambda: admin.connected_to(bob) and bob.connected_to(admin), 30, "nodes did not connect")
    wait_until(lambda: bob.node()["height"] == 1, 30, "bob has no chain")
    assert bob.node()["admin_node_id"] == admin.node()["node_id"]
    status, reply = bob.post("/api/transactions", {"receiver": "admin", "amount": 0.5})
    assert status == 400  # bob has no coins
    status, reply = admin.post("/api/transactions", {"receiver": "bob", "amount": 5})
    assert status == 201
    wait_until(lambda: bob.node()["height"] == 2, 30, "PoA block not propagated")
    assert bob.get("/api/blocks/1")["validator"] == admin.node()["node_id"]
    assert bob.node()["balance"] == 5


def test_persistence_restart_rejoins_with_saved_chain(tmp_path, procs):
    sig = SignallingProc(tmp_path)
    procs.append(sig)
    storage = tmp_path / "persist"
    common = ["--signalling", sig.url, "--epoch-time", EPOCH, "--save"]
    alice = NodeProc(tmp_path, "pos", "alice", *common, "--data-dir", "alice", "--create-room", "persist",
                     storage=storage)
    procs.append(alice)
    bob = NodeProc(tmp_path, "pos", "bob", *common, "--data-dir", "bob", "--join-room", "persist", storage=storage)
    procs.append(bob)
    wait_until(lambda: bob.node()["height"] == 1, 30, "bob has no chain")
    alice.post("/api/transactions", {"receiver": "bob", "amount": 4})
    wait_until(lambda: len(bob.transactions()) == 2, 20, "tx not propagated")
    alice.stake(10)
    wait_until(lambda: bob.node()["height"] == 2, 30, "block not propagated")
    before = bob.node()
    bob_port = bob.port
    bob.stop()
    # restart bob from disk: same identity, same chain, rejoins the room (not a new genesis)
    bob2 = NodeProc(tmp_path, "pos", "bob", "--signalling", sig.url, "--epoch-time", EPOCH, "--save", "--load",
                    "--data-dir", "bob", "--join-room", "persist", port=bob_port, storage=storage)
    procs.append(bob2)
    after = bob2.node()
    assert after["node_id"] == before["node_id"] and after["public_key"] == before["public_key"]
    assert after["height"] == 2 and after["genesis_hash"] == before["genesis_hash"]
    assert after["balance"] == 4
    wait_until(lambda: alice.connected_to(bob2) and bob2.connected_to(alice), 30, "restarted node did not reconnect")
