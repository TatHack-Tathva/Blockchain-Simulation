import asyncio, copy, logging
from typing import Dict, List, Optional

from consensus.base_peer import BasePeer
from consensus.poa.blockchain_structures import (
    Transaction, Block, Chain, validate_chain, balance_at, required_miner, last_update_seq,
    validate_admin_update, sign_admin_update, now_ms, DEFAULT_ROUND_TIME,
)
from shared_blockchain_structures import ValidationError

log = logging.getLogger("node.poa")

CONSENSUS = "poa"


class Peer(BasePeer):
    CONSENSUS = CONSENSUS
    MENU_EXTRA = "\n8) View Miners\n9) Add Miner (admin)\n10) Remove Miner (admin)"

    def __init__(self, host, port, name, activate_disk_load, activate_disk_save, *,
                 round_time=DEFAULT_ROUND_TIME, block_interval=30.0, **kwargs):
        self.round_time = float(round_time)
        self.block_interval = float(block_interval)
        self.node_id_to_name_dict: Dict[str, str] = {}
        self.name_to_node_id_dict: Dict[str, str] = {}
        self.pending_updates: List[dict] = []  # admin signed authority changes not on chain yet
        super().__init__(host, port, name, activate_disk_load, activate_disk_save, **kwargs)
        self.name_to_node_id_dict[self.name.lower()] = self.node_id
        self.node_id_to_name_dict[self.node_id] = self.name.lower()

    # ------------------------------------------------------------ consensus API

    def make_chain(self, block_list):
        return Chain(blockList=block_list, round_time=self.round_time)

    def create_genesis(self):
        self.chain = Chain(publicKey=self.wallet.public_key_pem, privatekey=self.wallet.private_key,
                           node_id=self.node_id, round_time=self.round_time)
        self.persist_chain()

    def block_dict_to_block(self, block_dict):
        return Block.from_dict(block_dict)

    def validate_chain(self, block_list, trusted_prefix=0, run_contracts=True):
        return validate_chain(block_list, self.round_time, trusted_prefix,
                              self.run_contract_sync if run_contracts else None)

    def prefer_chain(self, block_list) -> bool:
        if len(block_list) != len(self.chain.chain):
            return len(block_list) > len(self.chain.chain)
        return int(block_list[-1].hash, 16) < int(self.chain.lastBlock.hash, 16)

    def consensus_params(self):
        return {"round_time": self.round_time}

    def consensus_tasks(self):
        return [self.mine_blocks()]

    def message_handlers(self):
        handlers = super().message_handlers()
        handlers.update({
            "new_block": self.on_new_block,
            "miners_list_update": self.on_miners_list_update,
            "network_details_request": self.on_network_details_request,
            "network_details": self.on_network_details,
        })
        return handlers

    def on_peer_registered(self, record):
        self.node_id_to_name_dict[record["node_id"]] = record["name"].lower()
        self.name_to_node_id_dict[record["name"].lower()] = record["node_id"]

    def on_chain_replaced(self):
        super().on_chain_replaced()
        self.prune_pending_updates()

    @property
    def admin_id(self):
        return self.chain.admin_node_id if self.chain else None

    @property
    def is_admin(self):
        return self.chain is not None and self.chain.admin_public_key == self.wallet.public_key_pem

    @property
    def miner(self):
        return self.chain is not None and any(
            e["public_key"] == self.wallet.public_key_pem for e in self.get_current_miners_list())

    def get_current_miners_list(self):
        update = self.next_applicable_update()
        return update["miners_list"] if update else self.chain.lastBlock.miners_list

    def status_extra(self):
        miners = self.get_current_miners_list() if self.chain else []
        return {
            "miner": self.miner,
            "is_admin": self.is_admin,
            "admin_node_id": self.admin_id,
            "round_time": self.round_time,
            "authorities": [{"node_id": m["node_id"], "name": self.node_id_to_name_dict.get(m["node_id"]),
                             "public_key": m["public_key"]} for m in miners],
            "pending_authority_updates": len(self.pending_updates),
        }

    def block_summary(self, height, block):
        summary = super().block_summary(height, block)
        summary.update({
            "creator": block.miner_public_key,
            "creator_name": self.node_id_to_name_dict.get(block.miner_node_id) or self.name_for_key(block.miner_public_key),
            "validator": block.miner_node_id,
            "authorities": [m["node_id"] for m in (block.miners_list or [])],
            "admin_update_seq": block.admin_update["seq"] if block.admin_update else None,
        })
        return summary

    # ---------------------------------------------------------- authority set

    def prune_pending_updates(self):
        applied = last_update_seq(self.chain.chain)
        self.pending_updates = sorted((u for u in self.pending_updates if u["seq"] > applied), key=lambda u: u["seq"])

    def next_applicable_update(self) -> Optional[dict]:
        if self.chain is None:
            return None
        wanted = last_update_seq(self.chain.chain) + 1
        height = len(self.chain.chain)
        for update in self.pending_updates:
            if update["seq"] == wanted and update["activation_block"] <= height:
                return update
        return None

    def add_pending_update(self, update) -> bool:
        validate_admin_update(update, self.chain.admin_public_key)
        if update["seq"] <= last_update_seq(self.chain.chain):
            return False
        if any(u["seq"] == update["seq"] for u in self.pending_updates):
            return False
        self.pending_updates.append(update)
        self.prune_pending_updates()
        return True

    async def on_miners_list_update(self, websocket, msg):
        if self.chain is None:
            return
        if self.add_pending_update(msg.get("update")):
            await self.broadcast_message(msg)

    async def after_known_peers(self, websocket):
        await self.send_json(websocket, self.new_msg("network_details_request"))

    async def on_network_details_request(self, websocket, msg):
        await self.send_json(websocket, self.new_msg("network_details", pending_updates=self.pending_updates))

    async def on_network_details(self, websocket, msg):
        """
            Only carries admin-signed pending updates. The admin itself is derived from the
            genesis block, so no peer can install an administrator via this message.
        """
        updates = msg.get("pending_updates")
        self._deferred_updates = updates[:100] if isinstance(updates, list) else []
        if self.chain is not None:
            await self.after_chain_adopted()
        await self.send_json(websocket, self.new_msg("chain_request"))

    async def after_chain_adopted(self):
        # Updates are verified against the admin key of *our* chain's genesis block.
        for update in getattr(self, "_deferred_updates", []):
            try:
                self.add_pending_update(update)
            except ValidationError as e:
                self.note_rejection("pending admin update", e)
        self._deferred_updates = []

    async def change_miners(self, miner_name: str, add: bool):
        if not self.is_admin:
            raise ValidationError("only the admin can change miners")
        node_id = self.name_to_node_id_dict.get(miner_name.lower().strip())
        if node_id is None:
            raise ValidationError(f"unknown node {miner_name}")
        public_key = self.wallet.public_key_pem if node_id == self.node_id else next(
            (r["public_key"] for r in self.known_peers.values() if r["node_id"] == node_id), None)
        base = copy.deepcopy(self.pending_updates[-1]["miners_list"] if self.pending_updates else self.chain.lastBlock.miners_list)
        entry = {"node_id": node_id, "public_key": public_key}
        if add:
            if any(e["node_id"] == node_id for e in base):
                raise ValidationError(f"{miner_name} is already in miners list")
            base.append(entry)
        else:
            if not any(e["node_id"] == node_id for e in base):
                raise ValidationError(f"{miner_name} is already not in miners list")
            base = [e for e in base if e["node_id"] != node_id]
            if not base:
                raise ValidationError("the authority set cannot be empty")
        seq = last_update_seq(self.chain.chain) + len(self.pending_updates) + 1
        update = sign_admin_update(self.wallet.private_key, seq, base, len(self.chain.chain))
        self.add_pending_update(update)
        await self.broadcast_message(self.new_msg("miners_list_update", update=update))
        return update

    async def broadcast_miners_list(self, miners_list, activation_block):
        seq = last_update_seq(self.chain.chain) + len(self.pending_updates) + 1
        update = sign_admin_update(self.wallet.private_key, seq, miners_list, activation_block)
        self.add_pending_update(update)
        await self.broadcast_message(self.new_msg("miners_list_update", update=update))

    # ----------------------------------------------------------------- blocks

    async def on_new_block(self, websocket, msg):
        block = self.block_dict_to_block(msg.get("block"))
        async with self.chain_lock:
            if self.chain is None or any(b.hash == block.hash for b in self.chain.chain):
                return
            if block.prevHash != self.chain.lastBlock.hash:
                if websocket is not None:
                    await self.send_json(websocket, self.new_msg("chain_request"))
                return
            await asyncio.to_thread(self.chain.validate_block, block, self.run_contract_sync, True)
            self.chain.chain.append(block)
            self.apply_block_side_effects(block)
            self.prune_pending_updates()
            self.persist_chain()
        log.info("[%s] block %d appended (mined by %s)", self.name, len(self.chain.chain) - 1,
                 self.node_id_to_name_dict.get(block.miner_node_id))
        await self.broadcast_message(msg)

    def sign_block(self, block: Block):
        block.sign_with(self.wallet.private_key)

    def build_block(self, transactions, ts):
        update = self.next_applicable_update()
        block = Block(self.chain.lastBlock.hash, transactions, ts)
        block.miner_node_id = self.node_id
        block.miner_public_key = self.wallet.public_key_pem
        block.miners_list = copy.deepcopy(update["miners_list"] if update else self.chain.lastBlock.miners_list)
        block.admin_update = copy.deepcopy(update) if update else None
        block.files = self.file_hashes.copy()
        self.sign_block(block)
        return block

    def is_my_slot(self, ts) -> bool:
        miners_list = self.get_current_miners_list()
        expected = required_miner(miners_list, self.chain.chain, len(self.chain.chain), ts, self.round_time)
        return expected["public_key"] == self.wallet.public_key_pem and expected["node_id"] == self.node_id

    async def mine_blocks(self):
        while True:
            await asyncio.sleep(self.block_interval)
            if self.chain is None or not self.mem_pool:
                continue
            ts = now_ms()
            if not self.is_my_slot(ts):
                continue
            height = len(self.chain.chain)
            transactions = await self.gather_block_transactions(
                lambda pk, pending: balance_at(self.chain.chain, height, pk, pending))
            if not transactions:
                continue
            async with self.chain_lock:
                if len(self.chain.chain) != height:
                    continue
                block = self.build_block(transactions, ts)
                try:
                    await asyncio.to_thread(self.chain.validate_block, block, self.run_contract_sync)
                except ValidationError as e:
                    log.info("[%s] discarding own block: %s", self.name, e)
                    continue
                await self.broadcast_message(self.new_msg("new_block", block=block.to_dict()))
                self.chain.chain.append(block)
                self.apply_block_side_effects(block)
                self.prune_pending_updates()
                self.persist_chain()
            log.info("[%s] mined block %d", self.name, len(self.chain.chain) - 1)

    # ------------------------------------------------------------ interactive

    async def handle_menu_choice(self, ch):
        if ch == 8:
            print("Current miners", [self.node_id_to_name_dict.get(m["node_id"], m["node_id"])
                                     for m in self.chain.lastBlock.miners_list])
            for update in self.pending_updates:
                print(f"Miners to be activated from block {update['activation_block']}",
                      [self.node_id_to_name_dict.get(m["node_id"], m["node_id"]) for m in update["miners_list"]])
        elif ch in (9, 10):
            name = await self.ainput("\nEnter Miner's Name: ")
            await self.change_miners(name, add=(ch == 9))
        else:
            return False
        return True
