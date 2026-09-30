"""
    Networking, persistence and validation plumbing shared by the PoW, PoS and PoA nodes.

    Consensus specific behaviour (block format, block creation, block / chain validation,
    fork choice) lives in the subclasses in consensus/<type>/p2p.py.  Everything that
    touches the chain goes through the same validation functions whether the data came
    from a peer, from the interactive menu or from the web API.
"""
import asyncio, json, uuid, base64, socket, random, time, os, shutil, subprocess, logging, traceback
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

from websockets.asyncio.server import serve
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from shared_blockchain_structures import (
    Transaction, Wallet, ValidationError, ContractContext, GAS_PRICE, BASE_DEPLOY_COST,
    parse_wire_transaction, make_wire_transaction, verify_signature, load_public_key,
    canonical_json, calculate_contract_id, deploy_fee, normalize_json_value,
    validate_contract_transaction, is_number,
)
from ipfs.ipfs import addToIpfs, download_ipfs_file_subprocess, is_valid_cid
from smart_contract.contracts_db import SmartContractDatabase
from smart_contract.secure_executor import SecureContractExecutor
from storage.storage_manager import (
    save_key, load_key, save_chain, load_chain, save_peers, load_peers, save_node_id, load_node_id,
)

log = logging.getLogger("node")

MAX_CONNECTIONS = 8
MAX_MESSAGE_SIZE = 8 * 1024 * 1024       # hard limit on any P2P websocket frame
CHAIN_REQUEST_MIN_INTERVAL = 5.0         # seconds between chain_requests served per connection
SEEN_IDS_MAX = 20000
SEEN_IDS_TTL = 600.0
PEER_RECORD_FIELDS = ("host", "port", "name", "public_key", "node_id")
MAX_NAME_LEN = 64


def get_random_element(s):
    """
        Return a random element from a set
    """
    return random.choice(list(s)) if s else None


@lru_cache(maxsize=1024)
def _resolve_host(host: str) -> str:
    return socket.gethostbyname(host)


def normalize_endpoint(ep):
    """
        Return host resolved into ipv4 address and port converted into int datatype - maintains consistency in the code
    """
    host, port = ep
    port = int(port)
    if not isinstance(host, str) or not host or not (0 < port < 65536):
        raise ValidationError("invalid endpoint")
    try:
        return (_resolve_host(host), port)
    except OSError:
        raise ValidationError(f"cannot resolve host {host}")


class SeenMessageCache:
    """Bounded, time limited set of message ids (prevents unbounded memory growth)."""

    def __init__(self, max_size=SEEN_IDS_MAX, ttl=SEEN_IDS_TTL):
        self.max_size = max_size
        self.ttl = ttl
        self._items: "OrderedDict[str, float]" = OrderedDict()

    def add(self, msg_id):
        now = time.monotonic()
        self._items[msg_id] = now
        self._items.move_to_end(msg_id)
        self._prune(now)

    def _prune(self, now):
        while self._items and (len(self._items) > self.max_size
                               or now - next(iter(self._items.values())) > self.ttl):
            self._items.popitem(last=False)

    def __contains__(self, msg_id):
        ts = self._items.get(msg_id)
        return ts is not None and time.monotonic() - ts <= self.ttl

    def __len__(self):
        return len(self._items)


def sign_peer_record(record: Dict[str, Any], wallet: Wallet) -> Dict[str, Any]:
    record = dict(record)
    record.pop("sig", None)
    record["sig"] = base64.b64encode(wallet.private_key.sign(canonical_json(record).encode())).decode()
    return record


def verify_peer_record(record) -> Dict[str, Any]:
    """
        Every peer announcement is signed by the key it announces, so a peer can't register
        itself (or relay a record) under somebody else's public key.
    """
    if not isinstance(record, dict):
        raise ValidationError("peer record must be an object")
    for key in PEER_RECORD_FIELDS:
        if key not in record:
            raise ValidationError(f"peer record missing {key}")
    if not isinstance(record["host"], str) or not record["host"] or len(record["host"]) > 255:
        raise ValidationError("invalid peer host")
    if not isinstance(record["port"], int) or isinstance(record["port"], bool) or not 0 < record["port"] < 65536:
        raise ValidationError("invalid peer port")
    if not isinstance(record["name"], str) or not record["name"].strip() or len(record["name"]) > MAX_NAME_LEN:
        raise ValidationError("invalid peer name")
    if not isinstance(record["node_id"], str) or not record["node_id"] or len(record["node_id"]) > 64:
        raise ValidationError("invalid node id")
    load_public_key(record["public_key"])
    unsigned = dict(record)
    sig = unsigned.pop("sig", None)
    try:
        sig_bytes = base64.b64decode(sig, validate=True) if isinstance(sig, str) else None
    except Exception:
        sig_bytes = None
    if not sig_bytes or not verify_signature(record["public_key"], sig_bytes, canonical_json(unsigned).encode()):
        raise ValidationError("invalid peer record signature")
    return record


class BasePeer:
    CONSENSUS = None
    MENU_EXTRA = ""

    def __init__(self, host, port, name, activate_disk_load, activate_disk_save, *,
                 data_profile=None, malicious=False):
        self.host = host
        self.port = int(port)
        self.name = name
        self.malicious = malicious
        self.data_profile = data_profile
        self.activate_disk_save = activate_disk_save
        load = activate_disk_load == "y"

        self.ipfs_port = self.port + 50  # API port
        self.gateway_port = self.port + 81  # Gateway port
        self.swarm_tcp = self.port + 2
        self.swarm_udp = self.port + 3
        self.repo_path = Path.home() / f".ipfs_{self.port}"
        self.env = os.environ.copy()
        self.env["IPFS_PATH"] = str(self.repo_path)
        self.daemon_process = None

        self.server_connections: Set = set()  # inbound websockets (we are the server)
        self.client_connections: Set = set()  # outbound websockets (we are the client)
        self.outbound_peers: Set[tuple] = set()  # endpoints we maintain an outbound connection to
        self.connection_endpoint: Dict[Any, tuple] = {}  # websocket -> peer endpoint
        self.got_pong: Dict[Any, bool] = {}
        self.have_sent_peer_info: Dict[Any, bool] = {}
        self.pending_add_peer_ws = None  # the only connection allowed to rename us (the bootstrap)
        self.last_chain_request: Dict[Any, float] = {}

        self.seen_message_ids = SeenMessageCache()

        self.name_to_public_key_dict: Dict[str, str] = {}
        self.known_peers: Dict[Tuple[str, int], Dict[str, Any]] = {}  # endpoint -> signed peer record
        self.room_members: Dict[str, Dict[str, Any]] = {}  # node_id -> record (signalling view)

        self.mem_pool: List[Transaction] = list()
        self.file_hashes: Dict[str, str] = {}
        self.contractsDB = SmartContractDatabase()
        self.recent_rejections: List[Dict[str, Any]] = []

        self.wallet = None
        if load:
            self.load_key_from_disk()
        if not self.wallet:
            self.wallet = Wallet()
            if self.activate_disk_save == "y":
                self.save_key_to_disk()

        self.node_id = load_node_id(self.CONSENSUS, self.data_profile) if load else None
        if not self.node_id:
            self.node_id = str(uuid.uuid4())
            if self.activate_disk_save == "y":
                save_node_id(self.node_id, self.CONSENSUS, self.data_profile)

        if load:
            self.load_known_peers_from_disk()

        self.chain = None
        if load:
            self.load_chain_from_disk()  # If no chain data stored, self.chain stays None

        self.chain_lock = asyncio.Lock()
        self.mem_pool_lock = asyncio.Lock()
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.server = None
        self.signalling = None
        self.web_server = None
        self._tasks: Set[asyncio.Task] = set()
        self._stopping = False
        self.started = asyncio.Event()
        self.discovery_interval = 30.0
        self.sync_interval = 60.0
        self.contract_timeout = 10.0

    # ------------------------------------------------------------------ helpers

    def spawn(self, coro, name=None):
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task):
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            exc = task.exception()
            log.error("[%s] background task failed: %s", self.name,
                      "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))

    def note_rejection(self, what, reason):
        log.info("[%s] rejected %s: %s", self.name, what, reason)
        self.recent_rejections.append({"what": what, "reason": str(reason), "ts": time.time()})
        del self.recent_rejections[:-50]

    # ------------------------------------------------------------ persistence

    def save_key_to_disk(self):
        save_key(self.wallet.private_key_pem, self.CONSENSUS, self.data_profile)

    def load_key_from_disk(self):
        key = load_key(self.CONSENSUS, self.data_profile)
        self.wallet = Wallet(key) if key else None

    def load_chain_from_disk(self):
        block_dict_list = load_chain(self.CONSENSUS, self.data_profile)
        if not block_dict_list:
            self.chain = None
            return
        try:
            block_list = [self.block_dict_to_block(block_dict) for block_dict in block_dict_list]
            # Our own saved chain was fully validated before it was saved; contracts are
            # not re-executed, everything else (signatures, balances, links) is re-checked.
            self.validate_chain(block_list, trusted_prefix=0, run_contracts=False)
        except (ValidationError, ValueError, KeyError, TypeError) as e:
            log.warning("[%s] stored chain is invalid (%s), ignoring it", self.name, e)
            self.chain = None
            return
        self.chain = self.make_chain(block_list)
        self.on_chain_replaced()

    def save_chain_to_disk(self):
        if self.chain is not None:
            save_chain(self.chain.to_block_dict_list(), self.CONSENSUS, self.data_profile)

    def save_known_peers_to_disk(self):
        content = {json.dumps(list(key)): record for key, record in self.known_peers.items()}
        save_peers(content, self.CONSENSUS, self.data_profile)

    def load_known_peers_from_disk(self):
        content = load_peers(self.CONSENSUS, self.data_profile)
        if not isinstance(content, dict):
            return
        for key, record in content.items():
            try:
                host, port = json.loads(key)
                record = verify_peer_record(record)
            except (ValueError, TypeError, ValidationError) as e:
                log.warning("[%s] skipping invalid saved peer %s: %s", self.name, key, e)
                continue
            self.known_peers[(host, int(port))] = record
            self.name_to_public_key_dict[record["name"].lower()] = record["public_key"]
            self.on_peer_registered(record)

    def persist_chain(self):
        if self.activate_disk_save == "y":
            self.save_chain_to_disk()

    # --------------------------------------------------------------- identity

    def peer_record(self) -> Dict[str, Any]:
        record = {
            "host": self.host,
            "port": self.port,
            "name": self.name,
            "public_key": self.wallet.public_key_pem,
            "node_id": self.node_id,
            "consensus": self.CONSENSUS,
            "malicious": self.malicious,
        }
        return sign_peer_record(record, self.wallet)

    def get_unique_name(self, base_name):
        existing_names = {record["name"].lower() for record in self.known_peers.values()}
        existing_names.add(self.name.lower())
        base_name = base_name.lower()
        if base_name not in existing_names:
            return base_name
        counter = 1
        while f"{base_name}{counter}" in existing_names:
            counter += 1
        return f"{base_name}{counter}"

    def is_self(self, record) -> bool:
        if record.get("public_key") == self.wallet.public_key_pem or record.get("node_id") == self.node_id:
            return True
        try:
            return normalize_endpoint((record["host"], record["port"])) == normalize_endpoint((self.host, self.port))
        except ValidationError:
            return False

    def register_peer(self, record) -> bool:
        """Registers a verified peer record. Returns True if the peer was new."""
        record = verify_peer_record(record)
        if self.is_self(record):
            return False
        endpoint = normalize_endpoint((record["host"], record["port"]))
        existing = self.known_peers.get(endpoint)
        if existing is not None:
            if existing["public_key"] != record["public_key"]:
                # First registration for an address wins; another key can't take it over.
                return False
            if existing.get("name") == record.get("name"):
                return False
        for other_endpoint, other in list(self.known_peers.items()):
            if other["public_key"] == record["public_key"] and other_endpoint != endpoint:
                self.known_peers.pop(other_endpoint)  # the same node moved address
        self.known_peers[endpoint] = record
        self.name_to_public_key_dict[record["name"].lower()] = record["public_key"]
        self.on_peer_registered(record)
        if self.activate_disk_save == "y":
            self.save_known_peers_to_disk()
        log.info("[%s] registered peer %s %s:%s", self.name, record["name"], record["host"], record["port"])
        return True

    def on_peer_registered(self, record):
        """Hook for subclasses (PoA keeps node id maps)."""

    def remove_peer(self, endpoint):
        record = self.known_peers.pop(endpoint, None)
        if record is None:
            return
        if self.name_to_public_key_dict.get(record["name"].lower()) == record["public_key"]:
            self.name_to_public_key_dict.pop(record["name"].lower(), None)
        if self.activate_disk_save == "y":
            self.save_known_peers_to_disk()
        log.info("[%s] removed stale peer %s", self.name, record["name"])

    def connected_endpoints(self) -> Set[tuple]:
        return {ep for ws, ep in self.connection_endpoint.items()
                if ws in self.server_connections or ws in self.client_connections}

    # --------------------------------------------------------------- messaging

    async def send_json(self, websocket, pkt) -> bool:
        try:
            await websocket.send(json.dumps(pkt))
            return True
        except ConnectionClosed:
            self.discard_connection(websocket)
            return False
        except Exception as e:
            log.warning("[%s] send failed: %s", self.name, e)
            self.discard_connection(websocket)
            return False

    async def broadcast_message(self, pkt, exclude=None):
        targets = [ws for ws in (self.server_connections | self.client_connections) if ws is not exclude]
        if targets:
            await asyncio.gather(*(self.send_json(ws, pkt) for ws in targets))

    def discard_connection(self, websocket):
        self.server_connections.discard(websocket)
        if websocket in self.client_connections:
            self.client_connections.discard(websocket)
        endpoint = self.connection_endpoint.pop(websocket, None)
        if endpoint is not None and endpoint not in self.connected_endpoints():
            self.outbound_peers.discard(endpoint)
            self.maybe_forget_peer(endpoint)
        self.got_pong.pop(websocket, None)
        self.have_sent_peer_info.pop(websocket, None)
        self.last_chain_request.pop(websocket, None)
        if self.pending_add_peer_ws is websocket:
            self.pending_add_peer_ws = None

    def maybe_forget_peer(self, endpoint):
        """
            With signalling, the room is the source of truth for membership: once a peer has
            left the room and its direct connection is gone, it is removed. (Without
            signalling we keep known peers so discovery can reconnect, like before.)
        """
        if self.signalling is None:
            return
        record = self.known_peers.get(endpoint)
        if record is None:
            return
        if record["node_id"] not in self.room_members and endpoint not in self.connected_endpoints():
            self.remove_peer(endpoint)

    async def process_raw(self, websocket, raw):
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            log.info("[%s] received malformed JSON, ignoring", self.name)
            return
        if not isinstance(msg, dict):
            return
        try:
            await self.handle_messages(websocket, msg)
        except ValidationError as e:
            self.note_rejection(msg.get("type"), e)
        except ConnectionClosed:
            raise
        except Exception:
            log.error("[%s] error handling %r message:\n%s", self.name, msg.get("type"), traceback.format_exc())

    def message_handlers(self):
        return {
            "ping": self.on_ping,
            "pong": self.on_pong,
            "peer_info": self.on_peer_info,
            "add_peer": self.on_add_peer,
            "new_peer": self.on_new_peer,
            "change_name": self.on_change_name,
            "known_peers": self.on_known_peers,
            "file": self.on_file,
            "new_tx": self.on_new_tx,
            "chain_request": self.on_chain_request,
            "chain": self.on_chain,
        }

    async def handle_messages(self, websocket, msg):
        """
            Dispatches a message. Every message has a type and an id; ids we have seen are
            dropped, which stops broadcasts from looping around the network.
        """
        t = msg.get("type")
        msg_id = msg.get("id")
        if not isinstance(t, str) or not isinstance(msg_id, str) or not msg_id or len(msg_id) > 128:
            return
        if msg_id in self.seen_message_ids:
            return
        self.seen_message_ids.add(msg_id)
        handler = self.message_handlers().get(t)
        if handler is not None:
            await handler(websocket, msg)

    def new_msg(self, msg_type, **fields):
        pkt = {"type": msg_type, "id": str(uuid.uuid4())}
        pkt.update(fields)
        self.seen_message_ids.add(pkt["id"])
        return pkt

    # ---------------------------------------------------------------- handshake

    async def on_ping(self, websocket, msg):
        await self.send_json(websocket, self.new_msg("pong"))

    async def on_pong(self, websocket, msg):
        self.got_pong[websocket] = True
        if not self.have_sent_peer_info.get(websocket, True):
            self.have_sent_peer_info[websocket] = True
            await self.send_json(websocket, self.new_msg("peer_info", data=self.peer_record()))

    def bind_connection(self, websocket, record):
        try:
            self.connection_endpoint[websocket] = normalize_endpoint((record["host"], record["port"]))
        except ValidationError:
            pass

    async def on_peer_info(self, websocket, msg):
        record = verify_peer_record(msg.get("data"))
        self.register_peer(record)
        if not self.is_self(record):
            self.bind_connection(websocket, record)
        await self.send_known_peers(websocket)

    async def on_add_peer(self, websocket, msg):
        record = verify_peer_record(msg.get("data"))
        if self.is_self(record):
            return
        self.bind_connection(websocket, record)
        proposed_name = self.get_unique_name(record["name"])
        known = normalize_endpoint((record["host"], record["port"])) in self.known_peers
        if proposed_name != record["name"].lower() and not known:
            # The joining node re-signs its own record with the new name and announces it.
            await self.send_json(websocket, self.new_msg("change_name", new_name=proposed_name,
                                                         new_peer_msg_id=str(uuid.uuid4())))
        elif self.register_peer(record):
            await self.broadcast_message(self.new_msg("new_peer", data=record))
        await self.send_known_peers(websocket)

    async def on_new_peer(self, websocket, msg):
        record = verify_peer_record(msg.get("data"))
        if self.register_peer(record):
            await self.broadcast_message(msg)

    async def on_change_name(self, websocket, msg):
        # Only the node we asked to join through (add_peer) may pick our name, and only once.
        if websocket is not self.pending_add_peer_ws:
            self.note_rejection("change_name", "not sent by our bootstrap connection")
            return
        new_name = msg.get("new_name")
        if not isinstance(new_name, str) or not new_name.strip() or len(new_name) > MAX_NAME_LEN:
            return
        self.pending_add_peer_ws = None
        self.name = new_name
        log.info("[%s] renamed by bootstrap to %s", self.name, new_name)
        await self.broadcast_message(self.new_msg("new_peer", data=self.peer_record()))

    async def send_known_peers(self, websocket):
        peers = list(self.known_peers.values()) + [self.peer_record()]
        await self.send_json(websocket, self.new_msg("known_peers", peers=peers))

    async def on_known_peers(self, websocket, msg):
        peers = msg.get("peers")
        if not isinstance(peers, list):
            return
        if websocket is self.pending_add_peer_ws:
            self.pending_add_peer_ws = None
        for record in peers[:1000]:
            try:
                self.register_peer(record)
            except ValidationError as e:
                log.debug("[%s] ignoring invalid peer record: %s", self.name, e)
        await self.after_known_peers(websocket)

    async def after_known_peers(self, websocket):
        await self.send_json(websocket, self.new_msg("chain_request"))

    async def on_file(self, websocket, msg):
        cid, desc = msg.get("cid"), msg.get("desc")
        if not is_valid_cid(cid) or not isinstance(desc, str) or len(desc) > 1024:
            return
        self.file_hashes[cid] = desc
        await self.broadcast_message(msg)

    # ------------------------------------------------------------ transactions

    def spendable_balance(self, public_key, pending_transactions=None):
        return self.chain.calc_balance(public_key, pending_transactions if pending_transactions is not None else self.mem_pool)

    def run_contract_sync(self, code, func_name, args, state):
        return SecureContractExecutor(code, timeout=self.contract_timeout).run(func_name, args, state)

    def contract_context(self) -> ContractContext:
        ctx = ContractContext.from_blocks(self.chain.chain)
        ctx.contracts.update(self.contractsDB.contracts)
        return ctx

    async def validate_new_transaction(self, tx: Transaction):
        """
            The single validation path for new transactions (network, menu and web API):
            structure (already checked by the parser) -> duplicates -> signature ->
            balance -> contract checks/execution (in a worker thread, off the event loop).
        """
        if self.chain is None:
            raise ValidationError("node has no chain yet")
        if self.chain.transaction_exists_in_chain(tx):
            raise ValidationError("transaction already exists in chain")
        if any(p.key == tx.key for p in self.mem_pool):
            raise ValidationError("transaction already pending")
        if not tx.is_valid_signature():
            raise ValidationError("invalid transaction signature")
        if tx.amount > self.spendable_balance(tx.sender):
            raise ValidationError("attempt to spend more than available balance")
        if tx.receiver in ("deploy", "invoke"):
            ctx = self.contract_context()
            await asyncio.to_thread(validate_contract_transaction, tx, ctx, self.run_contract_sync)

    async def process_new_tx_message(self, msg, websocket=None) -> Transaction:
        tx = parse_wire_transaction(msg)
        async with self.mem_pool_lock:
            await self.validate_new_transaction(tx)
            self.mem_pool.append(tx)
        log.info("[%s] valid transaction %s accepted into mempool", self.name, tx.id)
        await self.broadcast_message(msg)
        return tx

    async def on_new_tx(self, websocket, msg):
        await self.process_new_tx_message(msg, websocket)

    async def create_and_broadcast_tx(self, receiver_public_key, payload) -> Transaction:
        """Creates, signs and submits a transaction through the normal validation path."""
        transaction = Transaction(payload, self.wallet.public_key_pem, receiver_public_key)
        transaction.sign_with(self.wallet.private_key)
        msg = make_wire_transaction(transaction)
        self.seen_message_ids.add(msg["id"])
        return await self.process_new_tx_message(msg)

    def resolve_receiver(self, receiver: str) -> str:
        if not isinstance(receiver, str) or not receiver.strip():
            raise ValidationError("receiver is required")
        pk = self.name_to_public_key_dict.get(receiver.lower().strip())
        if pk is not None:
            return pk
        if receiver.strip().lower() == self.name.lower():
            return self.wallet.public_key_pem
        refined = receiver.replace("\\n", "\n")
        if "BEGIN PUBLIC KEY" in refined:
            load_public_key(refined)
            return refined
        raise ValidationError("no known peer with that name or public key")

    async def submit_payment(self, receiver: str, amount) -> Transaction:
        if not is_number(amount) or amount <= 0:
            raise ValidationError("amount must be a positive number")
        pk = self.resolve_receiver(receiver)
        return await self.create_and_broadcast_tx(pk, amount)

    async def submit_deploy(self, contract_code: str) -> Transaction:
        if not isinstance(contract_code, str) or not contract_code.strip():
            raise ValidationError("contract code is empty")
        return await self.create_and_broadcast_tx("deploy", [contract_code, deploy_fee(contract_code)])

    async def submit_invoke(self, contract_id, func_name, args) -> Transaction:
        ctx = self.contract_context()
        code = ctx.contracts.get(contract_id)
        if code is None:
            raise ValidationError("no such contract")
        response = await asyncio.to_thread(self.run_contract_sync, code, func_name, args,
                                           ctx.states.get(contract_id, {}))
        if response.get("error") is not None:
            raise ValidationError(f"contract error: {response['error']}")
        state = normalize_json_value(response["state"])
        amount = response["gas_used"] * GAS_PRICE
        return await self.create_and_broadcast_tx("invoke", [contract_id, func_name, list(args), state, amount])

    def select_block_transactions(self, balance_fn, max_count=100):
        """
            Picks the mempool transactions that are valid on top of the current chain
            (sequentially, so they're also valid together). Invalid ones are dropped from
            the mempool so a single bad transaction can never stall block production.
            Blocking (re-executes invoke transactions) - use gather_block_transactions().
            Returns (selected, invalid_transactions).
        """
        selected: List[Transaction] = []
        ctx = self.contract_context()
        invalid = []
        for tx in list(self.mem_pool):
            if len(selected) >= max_count:
                break
            try:
                if self.chain.transaction_exists_in_chain(tx) or any(s.key == tx.key for s in selected):
                    raise ValidationError("duplicate")
                if not tx.is_valid_signature():
                    raise ValidationError("bad signature")
                if tx.amount > balance_fn(tx.sender, selected):
                    raise ValidationError("insufficient balance")
                validate_contract_transaction(tx, ctx, self.run_contract_sync)
            except ValidationError as e:
                invalid.append(tx)
                self.note_rejection("mempool transaction", e)
                continue
            ctx.apply(tx)
            selected.append(tx)
        return selected, invalid

    async def gather_block_transactions(self, balance_fn, max_count=100) -> List[Transaction]:
        async with self.mem_pool_lock:
            selected, invalid = await asyncio.to_thread(self.select_block_transactions, balance_fn, max_count)
            if invalid:
                bad = {tx.key for tx in invalid}
                self.mem_pool = [tx for tx in self.mem_pool if tx.key not in bad]
        return selected

    def prune_after_chain_change(self):
        self.mem_pool = [tx for tx in self.mem_pool if not self.chain.transaction_exists_in_chain(tx)]
        for cid in list(self.file_hashes.keys()):
            if self.chain.cid_exists_in_chain(cid):
                self.file_hashes.pop(cid, None)

    def on_chain_replaced(self):
        """Called after the whole chain was replaced (sync / load): rebuild derived state."""
        self.contractsDB.rebuild_from_chain(self.chain.chain)
        self.prune_after_chain_change()

    def apply_block_side_effects(self, block):
        for transaction in block.transactions:
            if transaction.receiver == "deploy":
                contract_id = calculate_contract_id(transaction.sender, transaction.ts)
                self.contractsDB.store_contract(contract_id, transaction.payload[0])
                log.info("[%s] contract deployed with id %s", self.name, contract_id)
        self.prune_after_chain_change()

    # ------------------------------------------------------------ chain sync

    async def on_chain_request(self, websocket, msg):
        if self.chain is None:
            return
        now = time.monotonic()
        last = self.last_chain_request.get(websocket)
        if last is not None and now - last < CHAIN_REQUEST_MIN_INTERVAL:
            self.note_rejection("chain_request", "rate limited")
            return
        self.last_chain_request[websocket] = now
        await self.send_json(websocket, self.new_msg("chain", chain=self.chain.to_block_dict_list()))

    async def on_chain(self, websocket, msg):
        block_dict_list = msg.get("chain")
        if not isinstance(block_dict_list, list) or not block_dict_list:
            return
        block_list = [self.block_dict_to_block(block_dict) for block_dict in block_dict_list]
        async with self.chain_lock:
            if self.chain is not None:
                if block_list[0].hash != self.chain.genesis_hash:
                    raise ValidationError("received chain has a different genesis block")
                if not self.chain_is_candidate(block_list):
                    await self.inspect_fork(block_list)
                    return
                trusted = self.chain.common_prefix_length(block_list)
            else:
                trusted = 0
            await asyncio.to_thread(self.validate_chain, block_list, trusted, True)
            await self.inspect_fork(block_list)
            if self.chain is not None and not self.prefer_chain(block_list):
                return
            self.chain = self.make_chain(block_list)
            self.on_chain_replaced()
            log.info("[%s] adopted chain of length %d", self.name, len(block_list))
            self.persist_chain()
        await self.after_chain_adopted()

    def chain_is_candidate(self, block_list) -> bool:
        """Cheap pre-check before full validation (e.g. is it longer / heavier)."""
        return self.prefer_chain(block_list)

    async def inspect_fork(self, block_list):
        """Hook: PoS inspects competing blocks for double signing evidence."""

    async def after_chain_adopted(self):
        """Hook for subclasses."""

    async def find_longest_chain(self):
        """
            We routinely ask peers for their chain and replace ours if theirs wins the
            fork choice rule of the consensus type.
        """
        while True:
            await self.broadcast_message(self.new_msg("chain_request"))
            await asyncio.sleep(self.sync_interval)

    # ---------------------------------------------------------------- network

    async def handle_connections(self, websocket):
        """
            We handle our server connections from here.
            Primary job is to simply read messages and send it to handle_messages
        """
        self.server_connections.add(websocket)
        log.info("[%s] inbound connection from %s", self.name, websocket.remote_address)
        try:
            async for raw in websocket:
                await self.process_raw(websocket, raw)
        except ConnectionClosed:
            pass
        finally:
            self.discard_connection(websocket)

    async def connect_to_peer(self, host, port):
        """
            Function to form an outbound connection to the given host:port
            and handle messages that come form this connection
            Also initiates the handshake
        """
        try:
            endpoint = normalize_endpoint((host, port))
        except ValidationError as e:
            log.warning("[%s] cannot connect to %s:%s: %s", self.name, host, port, e)
            return
        if endpoint in self.outbound_peers or endpoint == normalize_endpoint((self.host, self.port)):
            return
        self.outbound_peers.add(endpoint)
        websocket = None
        try:
            websocket = await connect(f"ws://{endpoint[0]}:{endpoint[1]}", max_size=MAX_MESSAGE_SIZE,
                                      open_timeout=10)
            self.client_connections.add(websocket)
            self.connection_endpoint[websocket] = endpoint
            self.have_sent_peer_info[websocket] = False
            log.info("[%s] outbound connection formed to %s:%s", self.name, *endpoint)

            if self.chain is None and self.signalling is None:
                # First time joining through a bootstrap node: announce ourselves.
                self.pending_add_peer_ws = websocket
                self.have_sent_peer_info[websocket] = True
                pkt = self.new_msg("add_peer", data=self.peer_record())
            else:
                pkt = self.new_msg("ping")
            await self.send_json(websocket, pkt)

            async for raw in websocket:
                await self.process_raw(websocket, raw)
        except ConnectionClosed:
            pass
        except (OSError, asyncio.TimeoutError) as e:
            log.info("[%s] failed to connect to %s:%s ::: %s", self.name, host, port, e)
        finally:
            if websocket is not None:
                self.discard_connection(websocket)
                await websocket.close()
            self.outbound_peers.discard(endpoint)

    async def discover_peers(self):
        """
            Maintains up to MAX_CONNECTIONS peers.
            Connects only to fill the pool if under MAX_CONNECTIONS.
        """
        while True:
            self.connect_missing_peers()
            await asyncio.sleep(self.discovery_interval)

    def connect_missing_peers(self):
        connected = self.connected_endpoints() | self.outbound_peers
        potential_peers = [ep for ep in self.known_peers if ep not in connected]
        random.shuffle(potential_peers)
        for endpoint in potential_peers:
            if len(self.outbound_peers) >= MAX_CONNECTIONS:
                break
            self.spawn(self.connect_to_peer(*endpoint))

    async def gossip_peer_sampler(self):
        """
            Every 60s, drops one existing peer and connects to one new peer.
        """
        while True:
            await asyncio.sleep(60)
            if len(self.known_peers) <= len(self.outbound_peers) or len(self.outbound_peers) < MAX_CONNECTIONS:
                continue  # Nothing to swap
            to_drop = get_random_element(self.client_connections)
            if to_drop:
                log.info("[%s] gossip sampling: disconnecting %s", self.name, to_drop.remote_address)
                await to_drop.close()
            self.connect_missing_peers()

    # -------------------------------------------------------------- signalling

    def consensus_params(self) -> Dict[str, Any]:
        """Parameters that must match for two nodes to share a network."""
        return {}

    def signalling_genesis(self):
        return self.chain.genesis_hash if self.chain is not None else None

    async def setup_signalling(self, url):
        from signalling.client import SignallingClient
        self.signalling = SignallingClient(
            url,
            record_provider=self.peer_record,
            consensus=self.CONSENSUS,
            params=self.consensus_params(),
            genesis_provider=self.signalling_genesis,
            on_members=self.on_room_members,
            on_peer_joined=self.on_room_peer_joined,
            on_peer_left=self.on_room_peer_left,
            name=self.name,
        )
        await self.signalling.start()

    async def create_room(self, room):
        if self.signalling is None:
            raise ValidationError("signalling is not configured")
        if self.chain is None:
            self.create_genesis()
        return await self.signalling.create_room(room)

    async def join_room(self, room):
        if self.signalling is None:
            raise ValidationError("signalling is not configured")
        return await self.signalling.join_room(room)

    async def leave_room(self):
        if self.signalling is not None:
            await self.signalling.leave_room()
        self.room_members = {}

    def on_room_members(self, members):
        self.room_members = {}
        for record in members:
            self._accept_room_member(record)
        for endpoint, record in list(self.known_peers.items()):
            self.maybe_forget_peer(endpoint)

    def on_room_peer_joined(self, record):
        self._accept_room_member(record)

    def _accept_room_member(self, record):
        try:
            record = verify_peer_record(record)  # the signalling server is not trusted
        except ValidationError as e:
            self.note_rejection("room member", e)
            return
        if self.is_self(record):
            return
        if record.get("consensus") != self.CONSENSUS:
            self.note_rejection("room member", "different consensus")
            return
        self.room_members[record["node_id"]] = record
        self.register_peer(record)
        try:
            endpoint = normalize_endpoint((record["host"], record["port"]))
        except ValidationError:
            return
        if endpoint not in self.connected_endpoints() and endpoint not in self.outbound_peers:
            # Direct P2P connection; signalling is only used to find each other.
            self.spawn(self.connect_to_peer(record["host"], record["port"]))

    def on_room_peer_left(self, node_id):
        record = self.room_members.pop(node_id, None)
        if record is None:
            return
        try:
            endpoint = normalize_endpoint((record["host"], record["port"]))
        except ValidationError:
            return
        self.maybe_forget_peer(endpoint)

    # ------------------------------------------------------------------- IPFS

    def ipfs_available(self):
        return shutil.which("ipfs") is not None

    async def uploadFile(self, desc: str, path: str):
        if not self.ipfs_available():
            print("\nIPFS CLI is not installed\n")
            return None
        if not self.daemon_process:
            self.start_daemon()
        cid, name = await asyncio.to_thread(addToIpfs, path)
        if not (cid and name):
            return None
        print(f"\nNew File Created : {cid}\n")
        pkt = self.new_msg("file", desc=desc, cid=cid)
        self.file_hashes[cid] = desc
        return pkt

    def init_repo(self):
        """
            Creates a ipfs repo of name ending in ipfs_port_no eg ipfs_5000
        """
        if not self.repo_path.exists():
            subprocess.run(["ipfs", "init"], env=self.env, check=True)

    def configure_ports(self):
        subprocess.run(["ipfs", "config", "Addresses.API", f"/ip4/127.0.0.1/tcp/{self.ipfs_port}"], env=self.env, check=True)
        subprocess.run(["ipfs", "config", "Addresses.Gateway", f"/ip4/127.0.0.1/tcp/{self.gateway_port}"], env=self.env, check=True)
        subprocess.run([
            "ipfs", "config", "Addresses.Swarm", "--json",
            f'["/ip4/127.0.0.1/tcp/{self.swarm_tcp}", "/ip4/127.0.0.1/udp/{self.swarm_udp}/quic"]'
        ], env=self.env, check=True)

    def setup_ipfs(self):
        if not self.ipfs_available():
            log.info("[%s] IPFS CLI not found, file sharing disabled", self.name)
            return
        try:
            self.init_repo()
            self.configure_ports()
        except (subprocess.CalledProcessError, OSError) as e:
            log.warning("[%s] IPFS setup failed: %s", self.name, e)

    def start_daemon(self):
        self.daemon_process = subprocess.Popen(["ipfs", "daemon"], env=self.env)

    def stop_daemon(self):
        if self.daemon_process:
            self.daemon_process.terminate()
            self.daemon_process.wait()
            self.daemon_process = None

    # ------------------------------------------------------------- lifecycle

    def create_genesis(self):
        raise NotImplementedError

    def consensus_tasks(self):
        return []

    async def start_network(self, bootstrap_host=None, bootstrap_port=None, signalling_url=None,
                            create_room=None, join_room=None, create_network=None):
        """Starts the P2P server and background tasks without the interactive menu."""
        self.loop = asyncio.get_running_loop()
        self.server = await serve(self.handle_connections, self.host, self.port, max_size=MAX_MESSAGE_SIZE)

        if signalling_url:
            await self.setup_signalling(signalling_url)

        if bootstrap_host and bootstrap_port:
            self.spawn(self.connect_to_peer(bootstrap_host, bootstrap_port))
        elif create_room:
            await self.create_room(create_room)
        elif join_room:
            await self.join_room(join_room)
        elif self.chain is None and (create_network if create_network is not None else not signalling_url):
            self.create_genesis()

        for coro in [self.find_longest_chain(), self.discover_peers(), self.gossip_peer_sampler()] + self.consensus_tasks():
            self.spawn(coro)
        await asyncio.to_thread(self.setup_ipfs)
        self.started.set()

    async def stop(self):
        if self._stopping:
            return
        self._stopping = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.signalling is not None:
            await self.signalling.close()
        connections = list(self.server_connections | self.client_connections)
        await asyncio.gather(*(ws.close() for ws in connections), return_exceptions=True)
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        if self.web_server is not None:
            await asyncio.to_thread(self.web_server.shutdown)
        self.stop_daemon()

    async def start(self, bootstrap_host=None, bootstrap_port=None, interactive=True, **kwargs):
        await self.start_network(bootstrap_host, bootstrap_port, **kwargs)
        try:
            if interactive:
                await self.user_input_handler()
            else:
                await asyncio.Event().wait()
        finally:
            await self.stop()

    # ---------------------------------------------------------- thread-safe API

    def call_threadsafe(self, coro_fn, *args, timeout=30.0):
        """Runs coro_fn(*args) on the node's event loop from another thread (web layer)."""
        if self.loop is None:
            raise RuntimeError("node is not running")

        async def runner():
            return await coro_fn(*args)
        return asyncio.run_coroutine_threadsafe(runner(), self.loop).result(timeout)

    def block_summary(self, height, block) -> Dict[str, Any]:
        return {
            "height": height,
            "hash": block.hash,
            "prev_hash": block.prevHash,
            "id": block.id,
            "ts": block.ts,
            "tx_count": len(block.transactions),
            "creator": None,
            "validator": None,
        }

    def transaction_summary(self, tx, status, height=None) -> Dict[str, Any]:
        return {
            "id": tx.id,
            "sender": tx.sender,
            "sender_name": self.name_for_key(tx.sender),
            "receiver": tx.receiver,
            "receiver_name": self.name_for_key(tx.receiver),
            "amount": tx.amount,
            "type": tx.receiver if tx.receiver in ("deploy", "invoke") else ("genesis" if tx.is_genesis else "transfer"),
            "ts": tx.ts,
            "status": status,
            "block_height": height,
        }

    def name_for_key(self, public_key):
        if public_key == self.wallet.public_key_pem:
            return self.name
        for record in self.known_peers.values():
            if record["public_key"] == public_key:
                return record["name"]
        return None

    def status_extra(self) -> Dict[str, Any]:
        return {}

    def snapshot_node(self) -> Dict[str, Any]:
        signalling = self.signalling.status() if self.signalling is not None else None
        return {
            "node_id": self.node_id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "consensus": self.CONSENSUS,
            "malicious": self.malicious,
            "role": "malicious" if self.malicious else "honest",
            "public_key": self.wallet.public_key_pem,
            "height": len(self.chain.chain) if self.chain else 0,
            "has_chain": self.chain is not None,
            "genesis_hash": self.chain.genesis_hash if self.chain else None,
            "balance": self.spendable_balance(self.wallet.public_key_pem) if self.chain else 0,
            "mempool_size": len(self.mem_pool),
            "room": signalling.get("room") if signalling else None,
            "signalling": signalling,
            "consensus_params": self.consensus_params(),
            "state": "running" if self.started.is_set() and not self._stopping else ("stopping" if self._stopping else "starting"),
            "recent_rejections": list(self.recent_rejections[-10:]),
            **self.status_extra(),
        }

    def snapshot_network(self) -> Dict[str, Any]:
        connected = self.connected_endpoints()
        inbound = {self.connection_endpoint.get(ws) for ws in self.server_connections}
        outbound = {self.connection_endpoint.get(ws) for ws in self.client_connections}
        peers = []
        for endpoint, record in self.known_peers.items():
            peers.append({
                "node_id": record["node_id"],
                "name": record["name"],
                "host": record["host"],
                "port": record["port"],
                "consensus": record.get("consensus"),
                "malicious": record.get("malicious"),
                "role": "malicious" if record.get("malicious") else "honest",
                "public_key": record["public_key"],
                "connected": endpoint in connected,
                "inbound": endpoint in inbound,
                "outbound": endpoint in outbound,
                "in_room": record["node_id"] in self.room_members,
            })
        return {
            "self": {"node_id": self.node_id, "name": self.name, "host": self.host, "port": self.port,
                     "consensus": self.CONSENSUS, "role": "malicious" if self.malicious else "honest"},
            "peers": peers,
            "room_members": [
                {"node_id": r["node_id"], "name": r["name"], "host": r["host"], "port": r["port"]}
                for r in self.room_members.values()
            ],
            "connections": {"inbound": len(self.server_connections), "outbound": len(self.client_connections)},
        }

    def snapshot_blocks(self, limit=50) -> List[Dict[str, Any]]:
        if self.chain is None:
            return []
        blocks = list(enumerate(self.chain.chain))[-limit:]
        return [self.block_summary(h, b) for h, b in reversed(blocks)]

    def snapshot_block(self, height) -> Optional[Dict[str, Any]]:
        if self.chain is None or not (0 <= height < len(self.chain.chain)):
            return None
        block = self.chain.chain[height]
        summary = self.block_summary(height, block)
        summary["transactions"] = [self.transaction_summary(tx, "confirmed", height) for tx in block.transactions]
        summary["files"] = dict(block.files)
        return summary

    def snapshot_transactions(self, limit=50) -> List[Dict[str, Any]]:
        result = [self.transaction_summary(tx, "pending") for tx in reversed(self.mem_pool)]
        if self.chain is not None:
            for height in range(len(self.chain.chain) - 1, -1, -1):
                for tx in reversed(self.chain.chain[height].transactions):
                    result.append(self.transaction_summary(tx, "confirmed", height))
                if len(result) >= limit:
                    break
        return result[:limit]

    async def api_snapshot(self, what, *args):
        return getattr(self, f"snapshot_{what}")(*args)

    # ------------------------------------------------------------ interactive

    async def ainput(self, prompt):
        return await asyncio.get_running_loop().run_in_executor(None, input, prompt)

    def print_menu(self):
        print("Block Chain Menu\n***************")
        print("0) Quit\n1) Add Transaction\n2) View balance\n3) Print Chain\n4) Print Pending Transactions\n"
              "5) Send Files\n6) Download Files\n7) Network status" + self.MENU_EXTRA)

    async def handle_menu_choice(self, ch) -> bool:
        """Returns False if the choice is unknown (subclasses add their own options)."""
        return False

    async def read_contract_code(self):
        path = await self.ainput("\nEnter path of the contract .py file: ")
        try:
            return Path(path.strip()).read_text(encoding="utf-8")
        except OSError as e:
            print(f"Cannot read contract file: {e}")
            return None

    async def menu_add_transaction(self):
        rec = await self.ainput("\nEnter Receiver's Name or Public Key (or 'deploy' / 'invoke'): ")
        if rec == "deploy":
            code = await self.read_contract_code()
            if code:
                await self.submit_deploy(code)
        elif rec == "invoke":
            contract_id = (await self.ainput("\nEnter Contract Id: ")).strip()
            func_name = (await self.ainput("\nEnter Function Name: ")).strip()
            args = []
            while True:
                arg = await self.ainput(f"Enter argument {len(args) + 1} (or \\q to finish): ")
                if arg.strip() == "\\q":
                    break
                try:
                    args.append(json.loads(arg))
                except ValueError:
                    args.append(arg)
            await self.submit_invoke(contract_id, func_name, args)
        else:
            amt = await self.ainput("\nEnter Amount to send: ")
            try:
                amt = float(amt)
            except ValueError:
                print("Amount must be a number")
                return
            await self.submit_payment(rec, amt)
        print("Transaction Created")

    async def user_input_handler(self):
        """
            A function to constantly take input from the user
        """
        while True:
            self.print_menu()
            ch = await self.ainput("Enter Your Choice: ")
            try:
                ch = int(ch)
            except ValueError:
                print("\nPlease enter a valid number!!!\n")
                continue
            try:
                if ch == 0:
                    print("Quitting...")
                    break
                elif ch == 1:
                    await self.menu_add_transaction()
                elif ch == 2:
                    print("Account Balance =", self.spendable_balance(self.wallet.public_key_pem))
                elif ch == 3:
                    for i, block in enumerate(self.chain.chain if self.chain else []):
                        print(f"block{i}: {block}\n")
                elif ch == 4:
                    for i, transaction in enumerate(self.mem_pool):
                        print(f"transaction{i}: {transaction}\n")
                elif ch == 5:
                    desc = await self.ainput("\nEnter description of file: ")
                    path = await self.ainput("\nEnter path of file: ")
                    pkt = await self.uploadFile(desc, path)
                    if pkt:
                        await self.broadcast_message(pkt)
                elif ch == 6:
                    cid = await self.ainput("\nEnter cid of file: ")
                    path = await self.ainput("\nEnter path (inside ipfs/retreived) to download the file: ")
                    await asyncio.to_thread(download_ipfs_file_subprocess, cid.strip(), path.strip())
                elif ch == 7:
                    print(json.dumps(self.snapshot_network(), indent=2))
                elif not await self.handle_menu_choice(ch):
                    print("\nUnknown option\n")
            except (ValidationError, ValueError) as e:
                print(f"\nError: {e}\n")
