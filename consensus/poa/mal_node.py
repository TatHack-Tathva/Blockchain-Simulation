"""
    Malicious PoA node used to test that honest nodes enforce the validation rules.

    It validates what it receives like an honest node, but it:
      1. creates transactions without checking balance / amount (invalid transactions)
      2. mines blocks even when it is not the scheduled authority, and creates two
         conflicting blocks for the same height sent to different halves of its peers
      3. tries to change the authority set without being the admin
"""
import asyncio, logging

from consensus.poa.p2p import Peer as HonestPeer
from consensus.poa.blockchain_structures import Transaction, Block, sign_admin_update, last_update_seq, now_ms
from shared_blockchain_structures import make_wire_transaction

log = logging.getLogger("node.poa.mal")


class Peer(HonestPeer):
    def __init__(self, *args, **kwargs):
        kwargs["malicious"] = True
        super().__init__(*args, **kwargs)

    async def create_and_broadcast_tx(self, receiver_public_key, payload):
        transaction = Transaction(payload, self.wallet.public_key_pem, receiver_public_key)
        transaction.sign_with(self.wallet.private_key)
        msg = make_wire_transaction(transaction)
        self.seen_message_ids.add(msg["id"])
        await self.broadcast_message(msg)
        return transaction

    async def change_miners(self, miner_name, add):
        # Signs an authority update with its own (non admin) key.
        miners = list(self.chain.lastBlock.miners_list) + [{"node_id": self.node_id, "public_key": self.wallet.public_key_pem}]
        update = sign_admin_update(self.wallet.private_key, last_update_seq(self.chain.chain) + 1, miners, len(self.chain.chain))
        await self.broadcast_message(self.new_msg("miners_list_update", update=update))
        return update

    def forged_blocks(self):
        others = [pk for pk in self.name_to_public_key_dict.values() if pk != self.wallet.public_key_pem]
        if not others:
            return []
        payload = round(self.spendable_balance(self.wallet.public_key_pem) * 0.75, 6) or 1
        blocks = []
        for receiver in (others * 2)[:2]:
            tx = Transaction(payload, self.wallet.public_key_pem, receiver)
            tx.sign_with(self.wallet.private_key)
            block = Block(self.chain.lastBlock.hash, [tx], now_ms())
            block.miner_node_id = self.node_id
            block.miner_public_key = self.wallet.public_key_pem
            block.miners_list = list(self.chain.lastBlock.miners_list)
            block.files = {}
            self.sign_block(block)
            blocks.append(block)
        return blocks

    async def mine_blocks(self):
        while True:
            await asyncio.sleep(self.block_interval)
            if self.chain is None:
                continue
            blocks = self.forged_blocks()
            if len(blocks) < 2:
                continue
            targets = list(self.server_connections | self.client_connections)
            half = len(targets) // 2
            pkt1 = self.new_msg("new_block", block=blocks[0].to_dict())
            pkt2 = self.new_msg("new_block", block=blocks[1].to_dict())
            log.info("[%s] sending unscheduled conflicting blocks", self.name)
            await asyncio.gather(*(self.send_json(ws, pkt1) for ws in targets[:half]),
                                 *(self.send_json(ws, pkt2) for ws in targets[half:]))
