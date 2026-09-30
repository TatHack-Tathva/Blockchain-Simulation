"""
    Malicious PoS node used to test that honest nodes enforce the validation rules.

    It validates what it receives like an honest node, but it:
      1. creates transactions without checking balance / amount (invalid transactions)
      2. double signs: when it wins the lottery it creates two different blocks for the
         same parent and sends one to each half of its peers (honest nodes must slash it)
      3. optionally ("omit_stakes") builds blocks that only contain its own stake, which
         lowers the total stake and raises its chance of winning (honest nodes must reject)
"""
import asyncio, logging, time

from consensus.pos.p2p import Peer as HonestPeer
from consensus.pos.blockchain_structures import Transaction, Block, compute_seed, wins_lottery, now_ms
from shared_blockchain_structures import make_wire_transaction

log = logging.getLogger("node.pos.mal")


class Peer(HonestPeer):
    def __init__(self, host, port, name, staker, activate_disk_load, activate_disk_save, *, attack="double_sign", **kwargs):
        kwargs["malicious"] = True
        self.attack = attack
        super().__init__(host, port, name, True, activate_disk_load, activate_disk_save, **kwargs)

    async def create_and_broadcast_tx(self, receiver_public_key, payload):
        # No balance / amount check before broadcasting.
        transaction = Transaction(payload, self.wallet.public_key_pem, receiver_public_key)
        transaction.sign_with(self.wallet.private_key)
        msg = make_wire_transaction(transaction)
        self.seen_message_ids.add(msg["id"])
        await self.broadcast_message(msg)
        return transaction

    def conflicting_transactions(self):
        others = [pk for pk in self.name_to_public_key_dict.values() if pk != self.wallet.public_key_pem]
        if not others:
            return None
        receivers = (others * 2)[:2]
        payload = round(self.spendable_balance(self.wallet.public_key_pem) * 0.75, 6)
        if payload <= 0:
            return None
        txs = []
        for receiver in receivers:
            tx = Transaction(payload, self.wallet.public_key_pem, receiver)
            tx.sign_with(self.wallet.private_key)
            txs.append(tx)
        return txs

    async def send_split(self, pkt1, pkt2):
        targets = list(self.server_connections | self.client_connections)
        half = max(1, len(targets) // 2) if len(targets) > 1 else len(targets)
        await asyncio.gather(*(self.send_json(ws, pkt1) for ws in targets[:half]),
                             *(self.send_json(ws, pkt2) for ws in targets[half:]))
        if len(targets) == 1:
            # A single peer still receives both versions (the double signing evidence).
            await self.send_json(targets[0], pkt2)

    async def create_blocks(self, delay):
        await asyncio.sleep(delay)
        async with self.chain_lock:
            async with self.curr_stakers_condition:
                tip = self.chain.lastBlock
                own = self.current_stakes.get(self.wallet.public_key_pem)
                if own is None or own.prev_hash != tip.hash:
                    return
                stakes = [s for s in self.current_stakes.values() if s.prev_hash == tip.hash]
                if self.attack == "omit_stakes":
                    stakes = [own]
                ts = max(now_ms(), tip.ts + int(self.epoch_time * 1000 * 5 / 6) + 1)
                if ts > now_ms():
                    await asyncio.sleep((ts - now_ms()) / 1000)
                seed = compute_seed(self.chain.chain, len(self.chain.chain), ts, self.epoch_time)
                if not wins_lottery(seed, self.wallet.public_key_pem, self.staked_amt, sum(s.amt for s in stakes)):
                    self.staked_amt = 0
                    return
                if self.attack == "omit_stakes":
                    txs = list(self.mem_pool)[:10] or self.conflicting_transactions()[:1]
                    block = self.build_candidate_block(txs, stakes, ts, seed)
                    log.info("[%s] broadcasting block that omits other stakes", self.name)
                    await self.broadcast_message(self.new_msg("new_block", block=block.to_dict_with_stakers()))
                    self.reset_epoch()
                    return
                txs = self.conflicting_transactions()
                if not txs:
                    return
                newBlock1 = self.build_candidate_block([txs[0]], stakes, ts, seed)
                newBlock2 = self.build_candidate_block([txs[1]], stakes, ts, seed)
                self.chain.chain.append(newBlock1)
                self.reset_epoch()
                self.persist_chain()
        pkt1 = self.new_msg("new_block", block=newBlock1.to_dict_with_stakers())
        pkt2 = self.new_msg("new_block", block=newBlock2.to_dict_with_stakers())
        log.info("[%s] double signing: sending two different blocks for the same height", self.name)
        await self.send_split(pkt1, pkt2)
