"""
    Phase 2: signalling server and client on real TCP sockets.
    Timeouts in these tests are only failure detectors (a hang fails the test).
"""
import asyncio, json, uuid
import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from conftest import run, wait_for, free_port
from shared_blockchain_structures import Wallet
from consensus.base_peer import sign_peer_record
from signalling.server import SignallingServer, CLOSE_REPLACED
from signalling.client import SignallingClient, SignallingError

DETECT = 10  # seconds before a stuck step is reported as a failure


def make_record(name, port=None, wallet=None, node_id=None, consensus="pos"):
    wallet = wallet or Wallet()
    record = {"host": "127.0.0.1", "port": port or free_port(), "name": name,
              "public_key": wallet.public_key_pem, "node_id": node_id or str(uuid.uuid4()),
              "consensus": consensus, "malicious": False}
    return sign_peer_record(record, wallet), wallet


class RawClient:
    """Minimal protocol client used to test the server directly."""

    def __init__(self, ws):
        self.ws = ws
        self.events = []

    @classmethod
    async def open(cls, port):
        return cls(await connect(f"ws://127.0.0.1:{port}", max_size=None))

    async def request(self, msg):
        ref = str(uuid.uuid4())
        await self.ws.send(json.dumps(dict(msg, ref=ref)))
        while True:
            reply = json.loads(await asyncio.wait_for(self.ws.recv(), DETECT))
            if reply.get("ref") == ref:
                return reply
            self.events.append(reply)

    async def next_event(self, t=None):
        for i, e in enumerate(self.events):
            if t is None or e.get("type") == t:
                return self.events.pop(i)
        while True:
            e = json.loads(await asyncio.wait_for(self.ws.recv(), DETECT))
            if t is None or e.get("type") == t:
                return e
            self.events.append(e)

    async def join(self, room, record, consensus="pos", params=None, create=False, genesis=None):
        return await self.request({"type": "create_room" if create else "join_room", "room": room, "peer": record,
                                   "consensus": consensus, "params": params or {"epoch_time": 60}, "genesis": genesis})


async def started_server():
    return await SignallingServer("127.0.0.1", 0).start()


def test_create_room_and_room_state():
    async def scenario():
        server = await started_server()
        try:
            c = await RawClient.open(server.port)
            rec, _ = make_record("alice")
            reply = await c.join("demo", rec, create=True)
            assert reply["type"] == "room_created"
            assert reply["room"] == "demo" and reply["consensus"] == "pos" and reply["members"] == []
            rooms = (await c.request({"type": "list_rooms"}))["rooms"]
            assert rooms == [{"room": "demo", "consensus": "pos", "params": {"epoch_time": 60}, "genesis": None, "members": 1}]
            again = await c.join("demo", make_record("bob")[0], create=True)
            assert again["code"] == "room_exists"
            await c.ws.close()
        finally:
            await server.stop()
    run(scenario())


def test_join_room_both_sides_get_discovery_information():
    async def scenario():
        server = await started_server()
        try:
            a, b = await RawClient.open(server.port), await RawClient.open(server.port)
            rec_a, _ = make_record("alice")
            rec_b, _ = make_record("bob")
            await a.join("demo", rec_a, create=True)
            reply = await b.join("demo", rec_b)
            assert reply["type"] == "room_joined" and reply["members"] == [rec_a]
            event = await a.next_event("peer_joined")
            assert event["peer"] == rec_b
            missing = await b.request({"type": "join_room", "room": "nope", "peer": rec_b, "consensus": "pos",
                                       "params": {"epoch_time": 60}})
            assert missing["code"] in ("no_such_room", "already_in_room")
        finally:
            await server.stop()
    run(scenario())


def test_disconnect_emits_peer_left_and_removes_state():
    async def scenario():
        server = await started_server()
        try:
            a, b = await RawClient.open(server.port), await RawClient.open(server.port)
            rec_a, _ = make_record("alice")
            rec_b, _ = make_record("bob")
            await a.join("demo", rec_a, create=True)
            await b.join("demo", rec_b)
            await b.ws.close()
            left = await a.next_event("peer_left")
            assert left["node_id"] == rec_b["node_id"]
            assert (await a.request({"type": "list_members"}))["members"] == []
            await a.request({"type": "leave_room"})
            assert (await a.request({"type": "list_rooms"}))["rooms"] == []  # empty room removed
        finally:
            await server.stop()
    run(scenario())


def test_room_isolation():
    async def scenario():
        server = await started_server()
        try:
            a, b, c = [await RawClient.open(server.port) for _ in range(3)]
            await a.join("room-x", make_record("alice")[0], create=True)
            await b.join("room-y", make_record("bob")[0], create=True)
            reply = await c.join("room-x", make_record("carol")[0])
            assert [m["name"] for m in reply["members"]] == ["alice"]
            await a.next_event("peer_joined")
            assert (await b.request({"type": "list_members"}))["members"] == []
            assert b.events == []  # bob (other room) got no events about carol
            sig = await c.request({"type": "signal", "to": "nobody", "data": {}})
            assert sig["code"] == "no_such_peer"
        finally:
            await server.stop()
    run(scenario())


def test_consensus_compatibility_checked():
    async def scenario():
        server = await started_server()
        try:
            a, b = await RawClient.open(server.port), await RawClient.open(server.port)
            await a.join("demo", make_record("alice")[0], create=True, genesis="a" * 64)
            rec, _ = make_record("bob", consensus="pow")
            assert (await b.join("demo", rec, consensus="pow"))["code"] == "consensus_mismatch"
            rec, _ = make_record("bob")
            assert (await b.join("demo", rec, params={"epoch_time": 5}))["code"] == "consensus_mismatch"
            assert (await b.join("demo", rec, genesis="b" * 64))["code"] == "genesis_mismatch"
            assert (await b.join("demo", rec, genesis="a" * 64))["type"] == "room_joined"
        finally:
            await server.stop()
    run(scenario())


def test_duplicate_registration_and_address_collision():
    async def scenario():
        server = await started_server()
        try:
            a, b1 = await RawClient.open(server.port), await RawClient.open(server.port)
            rec_a, _ = make_record("alice")
            rec_b, wallet_b = make_record("bob")
            await a.join("demo", rec_a, create=True)
            await b1.join("demo", rec_b)
            await a.next_event("peer_joined")
            # the same node re-registers from a new connection: new one wins, old is closed with 4001
            b2 = await RawClient.open(server.port)
            reply = await b2.join("demo", rec_b)
            assert reply["type"] == "room_joined"
            with pytest.raises(ConnectionClosed) as info:
                while True:
                    await asyncio.wait_for(b1.ws.recv(), DETECT)
            assert info.value.rcvd.code == CLOSE_REPLACED
            assert a.events == []  # no spurious departure for the replaced connection
            assert [m["node_id"] for m in (await a.request({"type": "list_members"}))["members"]] == [rec_b["node_id"]]
            # a different node can't claim bob's address, key, name or node id
            thief = await RawClient.open(server.port)
            steal_addr, _ = make_record("mallory", port=rec_b["port"])
            assert (await thief.join("demo", steal_addr))["code"] == "address_in_use"
            steal_name, _ = make_record("BOB")
            assert (await thief.join("demo", steal_name))["code"] == "name_taken"
            steal_id, _ = make_record("mallory", node_id=rec_b["node_id"])
            assert (await thief.join("demo", steal_id))["code"] == "node_id_conflict"
            forged = dict(rec_b, name="mallory", port=free_port())  # signature no longer valid
            assert (await thief.join("demo", forged))["code"] == "bad_request"
        finally:
            await server.stop()
    run(scenario())


def test_malformed_messages_do_not_crash_server():
    async def scenario():
        server = await started_server()
        try:
            c = await RawClient.open(server.port)
            for raw in ["{not json", "[1, 2]", "42", json.dumps({"no": "type"}), json.dumps({"type": "explode"}),
                        json.dumps({"type": "join_room", "room": "../etc", "peer": {}}),
                        json.dumps({"type": "create_room", "room": "ok", "peer": {"host": 1}}),
                        json.dumps({"type": "signal", "to": "x", "data": {}})]:
                await c.ws.send(raw)
                reply = json.loads(await asyncio.wait_for(c.ws.recv(), DETECT))
                assert reply["type"] == "error"
            await c.ws.send(b"\x00\x01binary")
            assert json.loads(await asyncio.wait_for(c.ws.recv(), DETECT))["type"] == "error"
            assert (await c.request({"type": "ping"}))["type"] == "pong"  # connection still usable
        finally:
            await server.stop()
    run(scenario())


def test_oversized_message_rejected_safely():
    async def scenario():
        server = await started_server()
        try:
            c = await RawClient.open(server.port)
            await c.ws.send(json.dumps({"type": "ping", "pad": "x" * (200 * 1024)}))
            with pytest.raises(ConnectionClosed) as info:
                await asyncio.wait_for(c.ws.recv(), DETECT)
            assert info.value.rcvd.code == 1009  # message too big
            fresh = await RawClient.open(server.port)
            assert (await fresh.request({"type": "ping"}))["type"] == "pong"
        finally:
            await server.stop()
    run(scenario())


def test_concurrent_joins_are_consistent():
    async def scenario():
        server = await started_server()
        try:
            creator = await RawClient.open(server.port)
            await creator.join("busy", make_record("creator")[0], create=True)
            clients = [await RawClient.open(server.port) for _ in range(20)]
            records = [make_record(f"n{i}")[0] for i in range(20)]
            replies = await asyncio.gather(*(c.join("busy", r) for c, r in zip(clients, records)))
            assert all(r["type"] == "room_joined" for r in replies)
            listing = await creator.request({"type": "list_members"})
            assert sorted(m["name"] for m in listing["members"]) == sorted(r["name"] for r in records)
            # every member has seen everyone (join reply + peer_joined events)
            for c, r, reply in zip(clients, records, replies):
                seen = {m["node_id"] for m in reply["members"]}
                while len(seen) < 20:
                    seen.add((await c.next_event("peer_joined"))["peer"]["node_id"])
                assert r["node_id"] not in seen
        finally:
            await server.stop()
    run(scenario())


def test_server_shutdown_with_connected_clients_does_not_hang():
    async def scenario():
        server = await started_server()
        clients = [await RawClient.open(server.port) for _ in range(5)]
        for i, c in enumerate(clients):
            await c.join("demo", make_record(f"n{i}")[0], create=(i == 0))
        await asyncio.wait_for(server.stop(), DETECT)
        for c in clients:
            with pytest.raises(ConnectionClosed):
                while True:  # drain queued room events, then the close must follow
                    await asyncio.wait_for(c.ws.recv(), DETECT)
    run(scenario())


# --------------------------------------------------------------------- client

def make_client(port, name, room_events=None, record=None, wallet=None):
    record, wallet = (record, wallet) if record else make_record(name)
    events = room_events if room_events is not None else []
    client = SignallingClient(
        f"ws://127.0.0.1:{port}", record_provider=lambda: record, consensus="pos", params={"epoch_time": 60},
        on_members=lambda members: events.append(("members", sorted(m["name"] for m in members))),
        on_peer_joined=lambda peer: events.append(("joined", peer["name"])),
        on_peer_left=lambda node_id: events.append(("left", node_id)),
        name=name, reconnect_initial=0.05, reconnect_max=0.5)
    return client, record, events


def test_client_create_join_discovery_and_leave():
    async def scenario():
        server = await started_server()
        a, rec_a, ev_a = make_client(server.port, "alice")
        b, rec_b, ev_b = make_client(server.port, "bob")
        try:
            await a.start(); await b.start()
            await a.create_room("demo")
            await b.join_room("demo")
            assert ev_b[-1] == ("members", ["alice"])
            assert await wait_for(lambda: ("joined", "bob") in ev_a, DETECT)
            assert a.status()["state"] == "joined" and a.status()["room"] == "demo"
            with pytest.raises(SignallingError) as info:
                c, _, _ = make_client(server.port, "carol")
                await c.start()
                try:
                    await c.join_room("missing")
                finally:
                    await c.close()
            assert info.value.code == "no_such_room"
            await b.leave_room()
            assert await wait_for(lambda: ("left", rec_b["node_id"]) in ev_a, DETECT)
        finally:
            await a.close(); await b.close(); await server.stop()
    run(scenario())


def test_client_reconnects_and_rejoins_after_server_restart():
    async def scenario():
        port = free_port()
        server = await SignallingServer("127.0.0.1", port).start()
        a, rec_a, ev_a = make_client(port, "alice")
        b, rec_b, ev_b = make_client(port, "bob")
        try:
            await a.start(); await b.start()
            await a.create_room("demo")
            await b.join_room("demo")
            await server.stop()  # signalling connection lost for both
            assert await wait_for(lambda: a.state in ("disconnected", "connecting") and b.state in ("disconnected", "connecting"), DETECT)
            server = await SignallingServer("127.0.0.1", port).start()
            assert await wait_for(lambda: a.state == "joined" and b.state == "joined", DETECT)
            # room recreated by whoever rejoined first; discovery state restored on both sides
            assert await wait_for(lambda: set(a.members) == {rec_b["node_id"]} and set(b.members) == {rec_a["node_id"]}, DETECT)
            assert a.connects >= 2 and b.connects >= 2
        finally:
            await a.close(); await b.close(); await server.stop()
    run(scenario())


def test_client_detects_dropped_connection_and_rejoins():
    async def scenario():
        server = await started_server()
        a, rec_a, ev_a = make_client(server.port, "alice")
        b, rec_b, ev_b = make_client(server.port, "bob")
        try:
            await a.start(); await b.start()
            await a.create_room("demo")
            await b.join_room("demo")
            # drop bob's connection from the server side (like a network failure)
            b_conn = next(c for c in server.connections.values() if c.node_id == rec_b["node_id"])
            await b_conn.ws.close(1011, "simulated failure")
            assert await wait_for(lambda: ("left", rec_b["node_id"]) in ev_a, DETECT)
            assert await wait_for(lambda: b.state == "joined" and b.connects == 2, DETECT)
            assert await wait_for(lambda: ev_a.count(("joined", "bob")) == 2, DETECT)
            assert b.members.keys() == {rec_a["node_id"]}
        finally:
            await a.close(); await b.close(); await server.stop()
    run(scenario())


def test_client_close_never_hangs_in_any_state():
    async def scenario():
        # 1) connected and joined
        server = await started_server()
        a, _, _ = make_client(server.port, "alice")
        await a.start()
        await a.create_room("demo")
        await asyncio.wait_for(a.close(), DETECT)
        assert a.state == "closed"
        # 2) in reconnect backoff (server gone)
        b, _, _ = make_client(server.port, "bob")
        b.reconnect_initial = b.reconnect_max = 30  # long backoff: close must interrupt it
        await b.start()
        await b.wait_connected()
        await server.stop()
        assert await wait_for(lambda: b.state == "disconnected", DETECT)
        await asyncio.wait_for(b.close(), DETECT)
        assert b.state == "closed"
        # 3) while connecting to an address that never answers
        c = SignallingClient("ws://10.255.255.1:9", record_provider=lambda: {}, consensus="pos", params={})
        await c.start()
        assert await wait_for(lambda: c.state == "connecting", DETECT)
        await asyncio.wait_for(c.close(), DETECT)
        assert c.state == "closed"
    run(scenario())


def test_replaced_client_stops_reconnecting():
    async def scenario():
        server = await started_server()
        record, wallet = make_record("alice")
        first, _, _ = make_client(server.port, "alice", record=record, wallet=wallet)
        second, _, _ = make_client(server.port, "alice", record=record, wallet=wallet)
        try:
            await first.start()
            await first.create_room("demo")
            await second.start()
            await second.join_room("demo")
            assert await wait_for(lambda: first.state == "replaced", DETECT)
            assert first.connects == 1 and second.state == "joined"
            listing = await second.list_rooms()
            assert listing[0]["members"] == 1
        finally:
            await first.close(); await second.close(); await server.stop()
    run(scenario())
