import asyncio, time, logging
from typing import Dict, List, Optional

from consensus.base_peer import BasePeer
from consensus.pos.blockchain_structures import (
    Transaction, Stake, Block, Chain, validate_chain, validate_next_block, balance_at, weight_of_chain,
    chain_rank, compute_seed, wins_lottery, is_double_sign_evidence, now_ms, DEFAULT_EPOCH_TIME,
)
from shared_blockchain_structures import ValidationError, is_number

log = logging.getLogger("node.pos")

CONSENSUS = "pos"


class StakeRegistrationClosed(ValidationError):
    def __init__(self, retry_after):
        super().__init__(f"stake registration period closed, try again in {retry_after:.1f}s")
        self.retry_after = retry_after


class Peer(BasePeer):
    CONSENSUS = CONSENSUS
    MENU_EXTRA = "\n8) Print Current Stakers\n9) Time since last epoch\n10) Stake"

    def __init__(self, host, port, name, staker: bool, activate_disk_load, activate_disk_save, *,
                 epoch_time=DEFAULT_EPOCH_TIME, **kwargs):
        self.staker = staker
        self.epoch_time = float(epoch_time)
        self.slashed_block_hashes = set()
        self.processed_evidence = set()
        super().__init__(host, port, name, activate_disk_load, activate_disk_save, **kwargs)
        self.staked_amt: int = 0
        self.current_stakes: Dict[str, Stake] = {}  # staker public key -> stake (for the current tip)
        self.curr_stakers_condition = asyncio.Condition()
        self.last_epoch_end_ts = time.time()
        self.create_block_task: Optional[asyncio.Task] = None

    @property
    def current_stakers(self) -> Dict[str, int]:
        return {pk: stake.amt for pk, stake in self.current_stakes.items()}

    # ------------------------------------------------------------ consensus API

    def make_chain(self, block_list):
        return Chain(blockList=block_list, epoch_time=self.epoch_time)

    def create_genesis(self):
        self.chain = Chain(publicKey=self.wallet.public_key_pem, privatekey=self.wallet.private_key,
                           epoch_time=self.epoch_time)
        self.last_epoch_end_ts = time.time()
        self.persist_chain()

    def block_dict_to_block(self, block_dict):
        return Block.from_dict(block_dict)

    def validate_chain(self, block_list, trusted_prefix=0, run_contracts=True):
        return validate_chain(block_list, self.epoch_time, trusted_prefix,
                              self.run_contract_sync if run_contracts else None)

    def prefer_chain(self, block_list) -> bool:
        return chain_rank(block_list) > chain_rank(self.chain.chain)

    def consensus_params(self):
        return {"epoch_time": self.epoch_time}

    def consensus_tasks(self):
        return [self.restart_epoch()]

    def message_handlers(self):
        handlers = super().message_handlers()
        handlers.update({
            "new_block": self.on_new_block,
            "stake_announcement": self.on_stake_announcement,
            "slash_announcement": self.on_slash_announcement,
        })
        return handlers

    def spendable_balance(self, public_key, pending_transactions=None):
        pending = pending_transactions if pending_transactions is not None else self.mem_pool
        return self.chain.calc_balance(public_key, pending, list(self.current_stakes.values()))

    def on_chain_replaced(self):
        for block in self.chain.chain:
            if block.hash in self.slashed_block_hashes:
                block.is_valid = False
                block.slash_creator = True
        super().on_chain_replaced()

    def status_extra(self):
        return {
            "staker": self.staker,
            "staked_amt": self.staked_amt,
            "epoch_time": self.epoch_time,
            "seconds_since_epoch_start": round(time.time() - self.last_epoch_end_ts, 2),
            "current_stakers": [
                {"staker": pk, "name": self.name_for_key(pk), "amount": s.amt} for pk, s in self.current_stakes.items()
            ],
            "slashed_blocks": sorted(self.slashed_block_hashes),
        }

    def block_summary(self, height, block):
        summary = super().block_summary(height, block)
        summary.update({
            "creator": block.creator,
            "creator_name": self.name_for_key(block.creator),
            "validator": block.creator,
            "staked_amt": block.staked_amt,
            "total_stake": sum(s.amt for s in block.stakers),
            "stakers": [{"staker": s.staker, "name": self.name_for_key(s.staker), "amount": s.amt} for s in block.stakers],
            "seed": block.seed,
            "slashed": not block.is_valid,
        })
        return summary

    # ----------------------------------------------------------------- epochs

    def reset_epoch(self):
        self.last_epoch_end_ts = time.time()
        self.staked_amt = 0
        self.current_stakes.clear()
        if self.create_block_task is not None and not self.create_block_task.done():
            if self.create_block_task is not asyncio.current_task():
                self.create_block_task.cancel()
        self.create_block_task = None

    async def restart_epoch(self):
        while True:
            await asyncio.sleep(self.epoch_time / 2)
            async with self.curr_stakers_condition:
                if time.time() - self.last_epoch_end_ts > self.epoch_time * 7 / 6:
                    self.reset_epoch()

    # ----------------------------------------------------------------- stakes

    async def on_stake_announcement(self, websocket, msg):
        stake = Stake.from_dict(msg.get("stake"))
        if self.chain is None:
            return
        async with self.curr_stakers_condition:
            await self.accept_stake(stake)
        await self.broadcast_message(msg)

    async def accept_stake(self, stake: Stake):
        """Validates a stake for the current epoch (caller holds curr_stakers_condition)."""
        if stake.prev_hash != self.chain.lastBlock.hash:
            raise ValidationError("stake is not for the current chain tip")
        if stake.staker in self.current_stakes:
            raise ValidationError("duplicate stake announcement for this epoch")
        if not stake.is_valid_signature():
            raise ValidationError("wrong stake signature")
        if stake.amt > self.chain.calc_balance(stake.staker, self.mem_pool, list(self.current_stakes.values())):
            raise ValidationError("staked more than available")
        self.current_stakes[stake.staker] = stake
        log.info("[%s] new stake %s:%s", self.name, self.name_for_key(stake.staker) or stake.staker[27:40], stake.amt)

    async def stake(self, amt) -> Stake:
        """Node side staking (menu option / web API): announce a stake and schedule block creation."""
        if not self.staker:
            raise ValidationError("this node is not a staker")
        if self.chain is None:
            raise ValidationError("node has no chain yet")
        if not isinstance(amt, int) or isinstance(amt, bool) or amt <= 0:
            raise ValidationError("stake amount must be a positive integer")
        async with self.curr_stakers_condition:
            time_since = time.time() - self.last_epoch_end_ts
            if time_since > self.epoch_time * 5 / 6:
                if time_since > self.epoch_time * 7 / 6:
                    self.reset_epoch()
                    time_since = 0
                else:
                    raise StakeRegistrationClosed(self.epoch_time * 7 / 6 - time_since)
            if self.staked_amt > 0 or self.wallet.public_key_pem in self.current_stakes:
                raise ValidationError("can't send multiple stakes in one epoch")
            new_stake = Stake(self.wallet.public_key_pem, amt, self.chain.lastBlock.hash)
            new_stake.sign_with(self.wallet.private_key)
            await self.accept_stake(new_stake)
            self.staked_amt = amt
            time_left = max(0.0, self.epoch_time - time_since)
            self.create_block_task = self.spawn(self.create_blocks(time_left))
        pkt = self.new_msg("stake_announcement", public_key=self.wallet.public_key_pem, stake=new_stake.to_wire_dict())
        await self.broadcast_message(pkt)
        log.info("[%s] staked %s, creating block in %.1fs", self.name, amt, time_left)
        return new_stake

    async def send_stake_announcements(self, amt: int):
        return await self.stake(amt)

    # ----------------------------------------------------------------- blocks

    def build_candidate_block(self, transactions, stakes, ts, seed):
        block = Block(self.chain.lastBlock.hash, transactions, ts)
        block.files = self.file_hashes.copy()
        block.creator = self.wallet.public_key_pem
        block.staked_amt = self.staked_amt
        block.stakers = list(stakes)
        block.seed = seed
        block.vrf_proof = self.wallet.private_key.sign(seed.encode())
        block.sign_with(self.wallet.private_key)
        return block

    async def create_blocks(self, delay):
        await asyncio.sleep(delay)
        async with self.chain_lock:
            async with self.curr_stakers_condition:
                tip = self.chain.lastBlock
                own = self.current_stakes.get(self.wallet.public_key_pem)
                if own is None or own.prev_hash != tip.hash or self.staked_amt <= 0:
                    return
                stakes = [s for s in self.current_stakes.values() if s.prev_hash == tip.hash]
                ts = max(now_ms(), tip.ts + int(self.epoch_time * 1000 * 5 / 6) + 1)
                if ts > now_ms():
                    await asyncio.sleep((ts - now_ms()) / 1000)
                seed = compute_seed(self.chain.chain, len(self.chain.chain), ts, self.epoch_time)
                total_stake = sum(s.amt for s in stakes)
                if not wins_lottery(seed, self.wallet.public_key_pem, self.staked_amt, total_stake):
                    log.info("[%s] You've lost the lottery this epoch", self.name)
                    self.staked_amt = 0
                    return
                log.info("[%s] You won the lottery", self.name)
                height = len(self.chain.chain)
                transactions = await self.gather_block_transactions(
                    lambda pk, pending: balance_at(self.chain.chain, height, pk, pending, stakes))
                if not transactions:
                    log.info("[%s] no pending transactions", self.name)
                    self.reset_epoch()
                    return
                block = self.build_candidate_block(transactions, stakes, ts, seed)
                await self.finish_own_block(block)

    async def finish_own_block(self, block):
        """Validate our own block like anyone else would, broadcast it, then append it."""
        await asyncio.to_thread(self.chain.validate_block, block, self.run_contract_sync)
        await self.broadcast_message(self.new_msg("new_block", block=block.to_dict_with_stakers()))
        self.chain.chain.append(block)
        self.apply_block_side_effects(block)
        self.reset_epoch()
        self.persist_chain()
        log.info("[%s] created block %d", self.name, len(self.chain.chain) - 1)

    async def on_new_block(self, websocket, msg):
        block = self.block_dict_to_block(msg.get("block"))
        async with self.chain_lock:
            if self.chain is None or any(b.hash == block.hash for b in self.chain.chain):
                return
            if block.prevHash != self.chain.lastBlock.hash:
                await self.check_equivocation(block)
                if websocket is not None:
                    await self.send_json(websocket, self.new_msg("chain_request"))
                return
            async with self.curr_stakers_condition:
                local_stakes = dict(self.current_stakes)
            await asyncio.to_thread(self.chain.validate_block, block, self.run_contract_sync, True, local_stakes)
            async with self.curr_stakers_condition:
                self.chain.chain.append(block)
                self.apply_block_side_effects(block)
                self.reset_epoch()
            self.persist_chain()
        log.info("[%s] block %d appended (created by %s)", self.name, len(self.chain.chain) - 1,
                 self.name_for_key(block.creator))
        await self.broadcast_message(msg)

    # --------------------------------------------------------------- slashing

    async def check_equivocation(self, block: Block):
        """A block competing with one we have: same creator + same parent = double signing."""
        for existing in self.chain.chain[1:]:
            if existing.prevHash == block.prevHash and existing.creator == block.creator:
                if is_double_sign_evidence(existing, block):
                    await self.slash(existing, block)
                return

    async def inspect_fork(self, block_list):
        """
            Compares a received chain with ours at the divergence point. Different creators
            is an ordinary fork (resolved by chain_rank); the same creator signing two
            different blocks at the same height is double signing and gets slashed.
        """
        if self.chain is None:
            return
        pos = self.chain.checkEquivalence(block_list)
        if pos <= 0:
            return
        ours, theirs = self.chain.chain[pos], block_list[pos]
        if is_double_sign_evidence(ours, theirs):
            await self.slash(ours, theirs)

    def mark_slashed(self, block1: Block, block2: Block):
        self.slashed_block_hashes.update({block1.hash, block2.hash})
        for block in self.chain.chain:
            if block.hash in self.slashed_block_hashes:
                block.is_valid = False
                block.slash_creator = True

    async def slash(self, block1: Block, block2: Block):
        evidence_key = frozenset((block1.hash, block2.hash))
        if evidence_key in self.processed_evidence:
            return
        if not is_double_sign_evidence(block1, block2):
            raise ValidationError("invalid slashing evidence")
        self.processed_evidence.add(evidence_key)
        self.mark_slashed(block1, block2)
        self.contractsDB.rebuild_from_chain(self.chain.chain)
        self.persist_chain()
        log.warning("[%s] double signing detected, slashed %s", self.name, self.name_for_key(block1.creator))
        await self.broadcast_message(self.new_msg(
            "slash_announcement",
            evidence1=block1.to_dict_with_stakers(),
            evidence2=block2.to_dict_with_stakers(),
        ))

    async def on_slash_announcement(self, websocket, msg):
        """
            Evidence must prove itself (both blocks signed by the same creator for the same
            parent). A slash message never rewrites or trims our chain.
        """
        block1 = self.block_dict_to_block(msg.get("evidence1"))
        block2 = self.block_dict_to_block(msg.get("evidence2"))
        if not is_double_sign_evidence(block1, block2):
            raise ValidationError("invalid slashing evidence")
        if self.chain is None:
            return
        evidence_key = frozenset((block1.hash, block2.hash))
        if evidence_key in self.processed_evidence:
            return
        self.processed_evidence.add(evidence_key)
        async with self.chain_lock:
            self.mark_slashed(block1, block2)
            self.contractsDB.rebuild_from_chain(self.chain.chain)
            self.persist_chain()
        log.warning("[%s] slash announcement accepted for %s", self.name, self.name_for_key(block1.creator))
        await self.broadcast_message(msg)

    async def verify_and_slash(self, block1: Block, block2: Block, pos: int = None, block_list=None):
        if is_double_sign_evidence(block1, block2):
            await self.slash(block1, block2)
            return True
        return False

    # ------------------------------------------------------------ interactive

    async def handle_menu_choice(self, ch):
        if ch == 8:
            for pk, amt in self.current_stakers.items():
                print(f"{self.name_for_key(pk) or pk}:{amt}\n")
        elif ch == 9:
            print(f"\n{time.time() - self.last_epoch_end_ts:.1f}\n")
        elif ch == 10:
            amt = await self.ainput("\nEnter Amount to stake: ")
            try:
                await self.stake(int(amt))
            except ValueError:
                print("\nPlease enter a valid number!!!\n")
        else:
            return False
        return True
