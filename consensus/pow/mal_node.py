"""
    Malicious PoW node used to test that honest nodes enforce the validation rules.

    It behaves like an honest node for everything it receives, but it:
      1. creates transactions without checking its balance / amount (invalid transactions)
      2. mines two conflicting blocks (double spend) and sends one to each half of its peers
"""
import asyncio, json, logging, threading

from consensus.pow.p2p import Peer as HonestPeer
from consensus.pow.blockchain_structures import Block, Transaction, balance_at
from shared_blockchain_structures import make_wire_transaction

log = logging.getLogger("node.pow.mal")


class Peer(HonestPeer):
    def __init__(self, *args, **kwargs):
        kwargs["malicious"] = True
        super().__init__(*args, **kwargs)

    async def create_and_broadcast_tx(self, receiver_public_key, payload):
        # Change NO 1 for malicious node: no balance / amount check before broadcasting.
        transaction = Transaction(payload, self.wallet.public_key_pem, receiver_public_key)
        transaction.sign_with(self.wallet.private_key)
        msg = make_wire_transaction(transaction)
        self.seen_message_ids.add(msg["id"])
        await self.broadcast_message(msg)
        return transaction

    def make_double_spend_blocks(self):
        """Two blocks on the same parent, each spending 75% of our balance to someone else."""
        others = [pk for pk in self.name_to_public_key_dict.values() if pk != self.wallet.public_key_pem]
        if len(others) < 2:
            others = (others * 2)[:2]
        if not others:
            return None
        payload = round(self.spendable_balance(self.wallet.public_key_pem) * 0.75, 6)
        if payload <= 0:
            return None
        blocks = []
        for receiver in others[:2]:
            tx = Transaction(payload, self.wallet.public_key_pem, receiver)
            tx.sign_with(self.wallet.private_key)
            block = Block(self.chain.lastBlock.hash, [tx], miner=self.wallet.public_key_pem)
            block.files = self.file_hashes.copy()
            blocks.append(block)
        return blocks

    async def send_split(self, pkt1, pkt2):
        targets = list(self.server_connections | self.client_connections)
        half = len(targets) // 2
        await asyncio.gather(*(self.send_json(ws, pkt1) for ws in targets[:half]),
                             *(self.send_json(ws, pkt2) for ws in targets[half:]))

    async def mine_blocks(self):
        while True:
            await asyncio.sleep(self.block_interval)
            if self.chain is None:
                continue
            blocks = self.make_double_spend_blocks()
            if not blocks:
                continue
            for block in blocks:
                await asyncio.to_thread(self.chain.mine, block, threading.Event())
            newBlock1, newBlock2 = blocks
            async with self.chain_lock:
                if newBlock1.prevHash != self.chain.lastBlock.hash:
                    continue
                self.chain.chain.append(newBlock1)
                self.persist_chain()
            pkt1 = self.new_msg("new_block", block=newBlock1.to_dict(), miner=newBlock1.miner)
            pkt2 = self.new_msg("new_block", block=newBlock2.to_dict(), miner=newBlock2.miner)
            log.info("[%s] sending conflicting blocks to two halves of the network", self.name)
            await self.send_split(pkt1, pkt2)
