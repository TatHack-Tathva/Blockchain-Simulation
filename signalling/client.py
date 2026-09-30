"""
    Signalling client used by nodes: connect, create / join a room, receive peer
    arrival / departure events, and automatically reconnect + rejoin the room after the
    signalling connection is lost. P2P connections between nodes are independent of it.

    Lifecycle (one background task, `_run`):
        idle -> connecting -> connected -> joined -> (connection lost) -> disconnected
             -> backoff -> connecting -> connected -> (automatic rejoin) -> joined ...
        close()  -> closed      (stop event set, socket closed, task awaited)
        server closes with 4001 -> replaced (a newer connection of this node took over;
                                   we must not reconnect or the two would evict each other)
"""
import asyncio, json, logging, uuid
from typing import Callable, Dict, Optional

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

log = logging.getLogger("signalling.client")

CLOSE_REPLACED = 4001
MAX_MESSAGE_SIZE = 64 * 1024
REQUEST_TIMEOUT = 10.0  # deadline for a server reply; turns a lost reply into an error


class SignallingError(Exception):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class SignallingClient:
    def __init__(self, url, record_provider: Callable[[], dict], consensus: str, params: dict,
                 genesis_provider: Optional[Callable[[], Optional[str]]] = None,
                 on_members: Optional[Callable] = None, on_peer_joined: Optional[Callable] = None,
                 on_peer_left: Optional[Callable] = None, name: str = "",
                 reconnect_initial: float = 0.25, reconnect_max: float = 5.0):
        self.url = url
        self.record_provider = record_provider
        self.consensus = consensus
        self.params = params
        self.genesis_provider = genesis_provider or (lambda: None)
        self.on_members = on_members
        self.on_peer_joined = on_peer_joined
        self.on_peer_left = on_peer_left
        self.name = name
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max

        self.state = "idle"
        self.room: Optional[str] = None          # room we want to be in (rejoined after reconnects)
        self.joined_room: Optional[str] = None   # room the server currently has us in
        self.members: Dict[str, dict] = {}
        self.connects = 0
        self.last_error: Optional[str] = None
        self._ws = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._connected = asyncio.Event()
        self._joined = asyncio.Event()
        self._stop = asyncio.Event()
        self._run_task: Optional[asyncio.Task] = None
        self._rejoin_task: Optional[asyncio.Task] = None

    # -------------------------------------------------------------- lifecycle

    async def start(self):
        if self._run_task is None:
            self._run_task = asyncio.create_task(self._run())

    async def close(self):
        self._stop.set()
        if self.state == "connecting" and self._run_task is not None:
            # connect() may be waiting on an unreachable host; nothing to shut down gracefully
            self._run_task.cancel()
        ws = self._ws
        if ws is not None:
            await ws.close()  # the receive loop ends with ConnectionClosedOK, _run sees _stop
        if self._run_task is not None:
            await asyncio.gather(self._run_task, return_exceptions=True)
        if self.state != "replaced":
            self.state = "closed"

    async def wait_connected(self, timeout=REQUEST_TIMEOUT):
        await asyncio.wait_for(self._connected.wait(), timeout)

    async def wait_joined(self, timeout=REQUEST_TIMEOUT):
        await asyncio.wait_for(self._joined.wait(), timeout)

    async def _run(self):
        delay = self.reconnect_initial
        while not self._stop.is_set():
            close_code = None
            self.state = "connecting"
            try:
                async with connect(self.url, max_size=MAX_MESSAGE_SIZE, open_timeout=REQUEST_TIMEOUT,
                                   ping_interval=5, ping_timeout=5) as ws:
                    self._ws = ws
                    self.state = "connected"
                    self.connects += 1
                    self._connected.set()
                    delay = self.reconnect_initial
                    if self.room is not None:
                        self._rejoin_task = asyncio.create_task(self._rejoin(self.room))
                    async for raw in ws:
                        self._dispatch(raw)
                close_code = ws.close_code
            except ConnectionClosed as e:
                close_code = e.rcvd.code if e.rcvd is not None else None
            except (OSError, asyncio.TimeoutError, InvalidHandshake, InvalidURI) as e:
                self.last_error = str(e)
                log.info("[%s] signalling connection failed: %s", self.name, e)
            finally:
                self._on_disconnected()
            if close_code == CLOSE_REPLACED:
                self.state = "replaced"
                log.warning("[%s] signalling registration replaced by a newer connection", self.name)
                return
            if self._stop.is_set():
                break
            self.state = "disconnected"
            log.info("[%s] signalling disconnected, reconnecting in %.2fs", self.name, delay)
            try:
                await asyncio.wait_for(self._stop.wait(), delay)  # reconnect backoff, ends early on close()
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, self.reconnect_max)
        self.state = "closed"

    def _on_disconnected(self):
        self._ws = None
        self._connected.clear()
        self._joined.clear()
        self.joined_room = None
        if self._rejoin_task is not None and not self._rejoin_task.done():
            self._rejoin_task.cancel()
        self._rejoin_task = None
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(SignallingError("disconnected", "signalling connection lost"))

    # -------------------------------------------------------------- messages

    def _dispatch(self, raw):
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        if not isinstance(msg, dict):
            return
        t = msg.get("type")
        if t in ("room_joined", "room_created"):
            # Membership snapshots must be applied here, in wire order. Applying them later
            # in the awaiting task would overwrite peer_joined/peer_left events that the
            # server sent after the snapshot (and that we dispatch in the meantime).
            self._apply_membership(msg)
        ref = msg.get("ref")
        if ref is not None and ref in self._pending:
            fut = self._pending.pop(ref)
            if not fut.done():
                fut.set_result(msg)
            return
        if t == "peer_joined" and isinstance(msg.get("peer"), dict):
            peer = msg["peer"]
            self.members[peer.get("node_id")] = peer
            if self.on_peer_joined:
                self.on_peer_joined(peer)
        elif t == "peer_left":
            self.members.pop(msg.get("node_id"), None)
            if self.on_peer_left:
                self.on_peer_left(msg.get("node_id"))
        elif t == "error":
            self.last_error = f"{msg.get('code')}: {msg.get('message')}"
            log.info("[%s] signalling error: %s", self.name, self.last_error)

    async def _request(self, msg) -> dict:
        await asyncio.wait_for(self._connected.wait(), REQUEST_TIMEOUT)
        ws = self._ws
        if ws is None:
            raise SignallingError("disconnected", "not connected")
        ref = str(uuid.uuid4())
        msg = dict(msg, ref=ref)
        fut = asyncio.get_running_loop().create_future()
        self._pending[ref] = fut
        try:
            await ws.send(json.dumps(msg))
            reply = await asyncio.wait_for(fut, REQUEST_TIMEOUT)
        except ConnectionClosed:
            raise SignallingError("disconnected", "signalling connection lost")
        except asyncio.TimeoutError:
            raise SignallingError("timeout", "no reply from signalling server")
        finally:
            self._pending.pop(ref, None)
        if reply.get("type") == "error":
            raise SignallingError(reply.get("code"), reply.get("message"))
        return reply

    def _registration(self, msg_type, room, **extra):
        return {"type": msg_type, "room": room, "peer": self.record_provider(), "consensus": self.consensus,
                "params": self.params, "genesis": self.genesis_provider(), **extra}

    def _apply_membership(self, reply):
        self.joined_room = reply.get("room")
        self.members = {m.get("node_id"): m for m in reply.get("members", []) if isinstance(m, dict)}
        self.state = "joined"
        self._joined.set()
        if self.on_members:
            self.on_members(list(self.members.values()))

    async def create_room(self, room):
        reply = await self._request(self._registration("create_room", room))
        self.room = room
        return reply

    async def join_room(self, room):
        reply = await self._request(self._registration("join_room", room))
        self.room = room
        return reply

    async def _rejoin(self, room):
        try:
            # create_if_missing: the server may have restarted and lost its rooms
            await self._request(self._registration("join_room", room, create_if_missing=True))
            log.info("[%s] rejoined room %s", self.name, room)
        except SignallingError as e:
            self.last_error = str(e)
            log.warning("[%s] automatic rejoin of %s failed: %s", self.name, room, e)

    async def leave_room(self):
        self.room = None
        if self._ws is not None:
            await self._request({"type": "leave_room"})
        self.joined_room = None
        self.members = {}
        self._joined.clear()
        self.state = "connected" if self._ws is not None else self.state

    async def list_rooms(self):
        return (await self._request({"type": "list_rooms"})).get("rooms", [])

    async def signal(self, node_id, data):
        return await self._request({"type": "signal", "to": node_id, "data": data})

    def status(self):
        return {
            "url": self.url,
            "state": self.state,
            "room": self.joined_room or self.room,
            "joined": self.joined_room is not None,
            "members": len(self.members),
            "connects": self.connects,
            "last_error": self.last_error,
        }
