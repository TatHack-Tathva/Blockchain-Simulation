"""
    Room based signalling server.

    Its only job is to let nodes find each other: room membership, peer discovery
    (signed peer records with the node's P2P address), arrival / departure events and a
    small relay for connection setup messages. It never sees blocks or transactions and
    takes no part in consensus - nodes connect to each other directly and validate
    everything themselves (they also re-verify every peer record they get from here).

    Run:  python -m signalling.server --host 127.0.0.1 --port 8765
"""
import argparse, asyncio, json, logging, re, signal, uuid
from typing import Dict, Optional

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from consensus.base_peer import verify_peer_record
from shared_blockchain_structures import ValidationError

log = logging.getLogger("signalling.server")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
MAX_MESSAGE_SIZE = 64 * 1024        # frames above this close the connection (code 1009)
MAX_QUEUE = 256                     # outbound messages buffered per client before it is dropped
MAX_SIGNAL_SIZE = 16 * 1024
CLOSE_REPLACED = 4001               # a newer connection registered the same node
ROOM_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
CONSENSUS_TYPES = ("pow", "pos", "poa")


class Connection:
    """One client websocket. All outbound traffic goes through a bounded queue drained by
    a writer task, so a slow / stuck client can never block the server or the room lock."""

    def __init__(self, ws):
        self.ws = ws
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE)
        self.room: Optional[str] = None
        self.node_id: Optional[str] = None
        self.record: Optional[dict] = None
        self.writer: Optional[asyncio.Task] = None
        self.overflowed = False

    def enqueue(self, msg):
        if self.overflowed:
            return
        try:
            self.queue.put_nowait(json.dumps(msg))
        except asyncio.QueueFull:
            self.overflowed = True
            log.warning("client %s is not reading, dropping it", self.ws.remote_address)
            asyncio.ensure_future(self.ws.close(1008, "outbound queue overflow"))

    async def write_loop(self):
        try:
            while True:
                data = await self.queue.get()
                await self.ws.send(data)
        except ConnectionClosed:
            pass


class Room:
    def __init__(self, name, consensus, params, genesis):
        self.name = name
        self.consensus = consensus
        self.params = params
        self.genesis = genesis
        self.members: Dict[str, Connection] = {}

    def describe(self):
        return {"room": self.name, "consensus": self.consensus, "params": self.params,
                "genesis": self.genesis, "members": len(self.members)}


class SignallingServer:
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, max_message_size=MAX_MESSAGE_SIZE):
        self.host = host
        self.port = port
        self.max_message_size = max_message_size
        self.rooms: Dict[str, Room] = {}
        self.connections: Dict[object, Connection] = {}
        self.lock = asyncio.Lock()
        self.server = None
        self._closers = set()

    # -------------------------------------------------------------- lifecycle

    async def start(self):
        self.server = await serve(self.handler, self.host, self.port, max_size=self.max_message_size,
                                  ping_interval=10, ping_timeout=10)
        self.port = self.server.sockets[0].getsockname()[1]
        log.info("signalling server listening on ws://%s:%s", self.host, self.port)
        return self

    async def stop(self):
        if self.server is not None:
            self.server.close()          # closes every client connection (code 1001)
            await self.server.wait_closed()  # returns once every handler has finished
        if self._closers:
            await asyncio.gather(*self._closers, return_exceptions=True)

    async def serve_forever(self):
        await self.start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        try:
            await stop.wait()
        finally:
            await self.stop()

    # ----------------------------------------------------------- connections

    async def handler(self, ws):
        conn = Connection(ws)
        self.connections[ws] = conn
        conn.writer = asyncio.create_task(conn.write_loop())
        try:
            async for raw in ws:
                await self.dispatch(conn, raw)
        except ConnectionClosed:
            pass
        finally:
            async with self.lock:
                self._remove_member(conn)
                self.connections.pop(ws, None)
            conn.writer.cancel()
            await asyncio.gather(conn.writer, return_exceptions=True)

    def _remove_member(self, conn: Connection):
        """Removes conn from its room - only if it still owns the registration."""
        if conn.room is None:
            return
        room = self.rooms.get(conn.room)
        node_id = conn.node_id
        conn.room, conn.node_id = None, None
        if room is None or room.members.get(node_id) is not conn:
            return  # replaced by a newer connection of the same node: no departure event
        del room.members[node_id]
        for other in room.members.values():
            other.enqueue({"type": "peer_left", "room": room.name, "node_id": node_id,
                           "name": conn.record.get("name") if conn.record else None})
        if not room.members:
            del self.rooms[room.name]
            log.info("room %s removed (empty)", room.name)
        log.info("node %s left room %s", node_id, room.name)

    def _close_replaced(self, conn: Connection):
        conn.enqueue({"type": "error", "code": "replaced", "message": "this node registered from a new connection"})
        task = asyncio.ensure_future(conn.ws.close(CLOSE_REPLACED, "replaced by a newer connection"))
        self._closers.add(task)
        task.add_done_callback(self._closers.discard)

    # -------------------------------------------------------------- messages

    @staticmethod
    def error(conn, code, message, ref=None):
        conn.enqueue({"type": "error", "code": code, "message": message, "ref": ref})

    async def dispatch(self, conn: Connection, raw):
        if not isinstance(raw, str):
            return self.error(conn, "bad_request", "binary frames are not supported")
        try:
            msg = json.loads(raw)
        except ValueError:
            return self.error(conn, "bad_json", "message is not valid JSON")
        if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
            return self.error(conn, "bad_request", "message must be an object with a type")
        ref = msg.get("ref")
        if ref is not None and (not isinstance(ref, str) or len(ref) > 64):
            ref = None
        handler = {
            "create_room": self.on_create_room,
            "join_room": self.on_join_room,
            "leave_room": self.on_leave_room,
            "list_rooms": self.on_list_rooms,
            "list_members": self.on_list_members,
            "signal": self.on_signal,
            "ping": self.on_ping,
        }.get(msg["type"])
        if handler is None:
            return self.error(conn, "unknown_type", f"unknown message type {msg['type']!r}", ref)
        try:
            await handler(conn, msg, ref)
        except ValidationError as e:
            self.error(conn, "bad_request", str(e), ref)

    def _parse_registration(self, msg):
        room = msg.get("room")
        if not isinstance(room, str) or not ROOM_RE.match(room):
            raise ValidationError("invalid room id (1-64 chars: letters, digits, _ . -)")
        record = verify_peer_record(msg.get("peer"))
        consensus = msg.get("consensus")
        if consensus not in CONSENSUS_TYPES or record.get("consensus", consensus) != consensus:
            raise ValidationError("invalid consensus type")
        params = msg.get("params") or {}
        if not isinstance(params, dict) or len(json.dumps(params)) > 1024:
            raise ValidationError("invalid consensus params")
        genesis = msg.get("genesis")
        if genesis is not None and (not isinstance(genesis, str) or len(genesis) != 64):
            raise ValidationError("invalid genesis hash")
        return room, record, consensus, params, genesis

    async def on_create_room(self, conn, msg, ref):
        room_name, record, consensus, params, genesis = self._parse_registration(msg)
        async with self.lock:
            if room_name in self.rooms:
                return self.error(conn, "room_exists", f"room {room_name} already exists", ref)
            if conn.room is not None:
                return self.error(conn, "already_in_room", "leave the current room first", ref)
            self.rooms[room_name] = Room(room_name, consensus, params, genesis)
            self._add_member(conn, self.rooms[room_name], record, ref, "room_created")
        log.info("room %s created by %s (%s)", room_name, record["name"], consensus)

    async def on_join_room(self, conn, msg, ref):
        room_name, record, consensus, params, genesis = self._parse_registration(msg)
        async with self.lock:
            room = self.rooms.get(room_name)
            if room is None:
                if not msg.get("create_if_missing"):
                    return self.error(conn, "no_such_room", f"room {room_name} does not exist", ref)
                room = self.rooms[room_name] = Room(room_name, consensus, params, genesis)
            if conn.room is not None and (conn.room != room_name or conn.node_id != record["node_id"]):
                return self.error(conn, "already_in_room", "leave the current room first", ref)
            if room.consensus != consensus or room.params != params:
                return self.error(conn, "consensus_mismatch",
                                  f"room uses {room.consensus} {room.params}, you use {consensus} {params}", ref)
            if genesis and room.genesis and genesis != room.genesis:
                return self.error(conn, "genesis_mismatch", "your chain belongs to a different network", ref)
            if genesis and not room.genesis:
                room.genesis = genesis
            conflict = self._conflict(room, record)
            if conflict:
                return self.error(conn, conflict[0], conflict[1], ref)
            self._add_member(conn, room, record, ref, "room_joined")
        log.info("%s joined room %s", record["name"], room_name)

    def _conflict(self, room: Room, record):
        for node_id, member in room.members.items():
            other = member.record
            same_node = node_id == record["node_id"]
            if same_node and other["public_key"] != record["public_key"]:
                return "node_id_conflict", "node id is registered with another key"
            if same_node:
                continue  # same node re-registering: handled as a replacement
            if other["public_key"] == record["public_key"]:
                return "key_in_use", "public key is registered by another node"
            if (other["host"], other["port"]) == (record["host"], record["port"]):
                return "address_in_use", f"{record['host']}:{record['port']} is registered by another node"
            if other["name"].lower() == record["name"].lower():
                return "name_taken", f"name {record['name']} is already used in this room"
        return None

    def _add_member(self, conn: Connection, room: Room, record, ref, reply_type):
        previous = room.members.get(record["node_id"])
        rejoin = previous is conn
        if previous is not None and previous is not conn:
            # Duplicate registration of the same node (e.g. it reconnected before the server
            # noticed the old connection died): the newest connection wins.
            previous.room, previous.node_id = None, None
            self._close_replaced(previous)
        conn.room, conn.node_id, conn.record = room.name, record["node_id"], record
        room.members[record["node_id"]] = conn
        others = [m.record for nid, m in room.members.items() if nid != record["node_id"]]
        conn.enqueue({"type": reply_type, "ref": ref, **room.describe(), "members": others})
        if not rejoin:
            for nid, member in room.members.items():
                if nid != record["node_id"]:
                    member.enqueue({"type": "peer_joined", "room": room.name, "peer": record})

    async def on_leave_room(self, conn, msg, ref):
        async with self.lock:
            left = conn.room
            self._remove_member(conn)
        conn.enqueue({"type": "room_left", "ref": ref, "room": left})

    async def on_list_rooms(self, conn, msg, ref):
        async with self.lock:
            rooms = [room.describe() for room in self.rooms.values()]
        conn.enqueue({"type": "rooms", "ref": ref, "rooms": rooms})

    async def on_list_members(self, conn, msg, ref):
        async with self.lock:
            room = self.rooms.get(conn.room) if conn.room else None
            members = [m.record for nid, m in room.members.items() if nid != conn.node_id] if room else []
        conn.enqueue({"type": "members", "ref": ref, "room": conn.room, "members": members})

    async def on_signal(self, conn, msg, ref):
        """Relays a small connection-setup message to another member of the same room."""
        target, data = msg.get("to"), msg.get("data")
        if conn.room is None:
            return self.error(conn, "not_in_room", "join a room first", ref)
        if not isinstance(target, str) or len(json.dumps(data)) > MAX_SIGNAL_SIZE:
            return self.error(conn, "bad_request", "invalid signal", ref)
        async with self.lock:
            member = self.rooms[conn.room].members.get(target)
            if member is None:
                return self.error(conn, "no_such_peer", "peer is not in this room", ref)
            member.enqueue({"type": "signal", "from": conn.node_id, "data": data})
        conn.enqueue({"type": "signal_sent", "ref": ref})

    async def on_ping(self, conn, msg, ref):
        conn.enqueue({"type": "pong", "ref": ref})


def main():
    parser = argparse.ArgumentParser(description="Blockchain simulation signalling server")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    try:
        asyncio.run(SignallingServer(args.host, args.port).serve_forever())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
