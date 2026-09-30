import asyncio, threading, logging
from typing import List

from consensus.base_peer import BasePeer
from consensus.pow.blockchain_structures import (
    Transaction, Block, Chain, validate_chain, balance_at, meets_difficulty, DEFAULT_DIFFICULTY, MINER_REWARD,
)
from shared_blockchain_structures import ValidationError

log = logging.getLogger("node.pow")

CONSENSUS = "pow"


class Peer(BasePeer):
    CONSENSUS = CONSENSUS

    def __init__(self, host, port, name, miner: bool, activate_disk_load, activate_disk_save, *,
                 difficulty=DEFAULT_DIFFICULTY, block_interval=30.0, **kwargs):
        self.miner = miner
        self.difficulty = int(difficulty)
        self.block_interval = float(block_interval)
        self.mining_cancel = threading.Event()
        super().__init__(host, port, name, activate_disk_load, activate_disk_save, **kwargs)

    # ------------------------------------------------------------ consensus API

    def make_chain(self, block_list):
        return Chain(blockList=block_list, difficulty=self.difficulty)

    def create_genesis(self):
        self.chain = Chain(publicKey=self.wallet.public_key_pem, difficulty=self.difficulty)
        self.persist_chain()

    def block_dict_to_block(self, block_dict):
        return Block.from_dict(block_dict)

    def validate_chain(self, block_list, trusted_prefix=0, run_contracts=True):
        return validate_chain(block_list, self.difficulty, trusted_prefix,
                              self.run_contract_sync if run_contracts else None)

    def prefer_chain(self, block_list) -> bool:
        # Longest chain rule (every block carries the same difficulty)
        return len(block_list) > len(self.chain.chain)

    def consensus_params(self):
        return {"difficulty": self.difficulty}

    def consensus_tasks(self):
        return [self.mine_blocks()] if self.miner else []

    def message_handlers(self):
        handlers = super().message_handlers()
        handlers["new_block"] = self.on_new_block
        return handlers

    def status_extra(self):
        return {"miner": self.miner, "difficulty": self.difficulty}

    def block_summary(self, height, block):
        summary = super().block_summary(height, block)
        summary["creator"] = block.miner
        summary["creator_name"] = self.name_for_key(block.miner) if block.miner else None
        summary["nonce"] = block.nonce
        return summary

    # ---------------------------------------------------------------- blocks

    async def on_new_block(self, websocket, msg):
        block = self.block_dict_to_block(msg.get("block"))
        async with self.chain_lock:
            if self.chain is None:
                return
            if any(b.hash == block.hash for b in self.chain.chain):
                return
            if block.prevHash != self.chain.lastBlock.hash:
                # We may be behind or on a fork: ask the sender for its chain.
                if websocket is not None:
                    await self.send_json(websocket, self.new_msg("chain_request"))
                return
            await asyncio.to_thread(self.chain.validate_block, block, self.run_contract_sync)
            self.chain.chain.append(block)
            self.mining_cancel.set()
            self.apply_block_side_effects(block)
            self.persist_chain()
        log.info("[%s] block %d appended (from network)", self.name, len(self.chain.chain) - 1)
        await self.broadcast_message(msg)

    async def after_chain_adopted(self):
        self.mining_cancel.set()

    async def build_block(self):
        """Selects valid transactions and mines a block on top of the current tip (or None)."""
        height = len(self.chain.chain)
        transactions = await self.gather_block_transactions(
            lambda pk, pending: balance_at(self.chain.chain, height, pk, pending))
        if not transactions:
            return None
        block = Block(self.chain.lastBlock.hash, transactions, miner=self.wallet.public_key_pem)
        block.files = self.file_hashes.copy()
        self.mining_cancel = cancel = threading.Event()
        found = await asyncio.to_thread(self.chain.mine, block, cancel)
        return block if found else None

    async def mine_blocks(self):
        """
            Every block_interval seconds we mine a block if there are pending transactions.
        """
        while True:
            await asyncio.sleep(self.block_interval)
            if self.chain is None or not self.mem_pool:
                continue
            block = await self.build_block()
            if block is None:
                continue
            async with self.chain_lock:
                try:
                    await asyncio.to_thread(self.chain.validate_block, block, self.run_contract_sync)
                except ValidationError as e:
                    # A competing block arrived while we were mining (stale prevHash) etc.
                    log.info("[%s] discarding mined block: %s", self.name, e)
                    continue
                self.chain.chain.append(block)
                self.apply_block_side_effects(block)
                self.persist_chain()
            log.info("[%s] mined block %d", self.name, len(self.chain.chain) - 1)
            await self.broadcast_message(self.new_msg("new_block", block=block.to_dict(), miner=block.miner))
