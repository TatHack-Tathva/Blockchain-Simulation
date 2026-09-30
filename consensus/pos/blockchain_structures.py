import json, hashlib, uuid, base64
from typing import List, Dict, Optional, Callable, Iterable
from datetime import datetime
from ecdsa import SigningKey
from shared_blockchain_structures import (
    Transaction,
    BaseBlock,
    CommonChain,
    Wallet,
    ValidationError,
    ContractContext,
    txs_to_json_digestable_form,
    valid_chain_length,
    transaction_exists_in_block_list,
    parse_block_common,
    validate_genesis_transactions,
    validate_transaction_list,
    load_public_key,
    verify_signature,
    b64decode_strict,
    is_number,
    MAX_ID_LEN,
)

GAS_PRICE = 0.001  # coin per gas unit
MAX_OUTPUT = 2**256
DEFAULT_EPOCH_TIME = 60
BLOCK_REWARD = 6
MAX_CLOCK_SKEW_MS = 10 * 1000


def now_ms():
    return int(datetime.now().timestamp() * 1000)


class Stake:
    """
        A signed stake for the block that will follow `prev_hash`. Binding the stake to
        the parent block means it can't be replayed into a later epoch.
    """
    def __init__(self, staker: str, amt: int, prev_hash: str = None, ts=None, id=None):
        self.id = id or str(uuid.uuid4())
        self.staker = staker
        self.amt = amt
        self.prev_hash = prev_hash
        self.sign: bytes = None
        self.ts = ts if ts is not None else datetime.now().timestamp()

    def to_dict(self):
        return {
            "id": self.id,
            "staker": self.staker,
            "amt": self.amt,
            "ts": self.ts,
            "prev_hash": self.prev_hash,
        }

    def to_wire_dict(self):
        stake_dict = self.to_dict()
        stake_dict["sign"] = base64.b64encode(self.sign).decode() if self.sign else None
        return stake_dict

    def __str__(self):
        return json.dumps(self.to_dict())

    def sign_with(self, private_key: SigningKey):
        self.sign = private_key.sign(str(self).encode())
        return self.sign

    def is_valid_signature(self):
        return verify_signature(self.staker, self.sign, str(self).encode())

    @classmethod
    def from_dict(cls, stake_dict):
        if not isinstance(stake_dict, dict):
            raise ValidationError("stake must be an object")
        required = ("id", "staker", "amt", "ts", "prev_hash", "sign")
        if set(stake_dict) != set(required):
            raise ValidationError("stake fields invalid")
        amt = stake_dict["amt"]
        if not isinstance(amt, int) or isinstance(amt, bool) or amt <= 0:
            raise ValidationError("stake amount must be a positive integer")
        if not isinstance(stake_dict["id"], str) or not stake_dict["id"] or len(stake_dict["id"]) > MAX_ID_LEN:
            raise ValidationError("invalid stake id")
        if not isinstance(stake_dict["prev_hash"], str) or len(stake_dict["prev_hash"]) != 64:
            raise ValidationError("invalid stake prev_hash")
        if not is_number(stake_dict["ts"]):
            raise ValidationError("invalid stake timestamp")
        load_public_key(stake_dict["staker"])
        stake = cls(stake_dict["staker"], amt, stake_dict["prev_hash"], stake_dict["ts"], stake_dict["id"])
        stake.sign = b64decode_strict(stake_dict["sign"])
        return stake


class Block(BaseBlock):
    def __init__(self, prevHash: str, transactions: List[Transaction], ts=None, id=None):
        super().__init__(prevHash, transactions, ts, id)

        self.creator: str = ""
        self.staked_amt = 0
        self.stakers: List[Stake] = []
        self.seed: str = ""
        self.vrf_proof: bytes = None
        self.sign: bytes = None
        # Local slashing state (set when double signing evidence is seen), not hashed
        self.is_valid: bool = True
        self.slash_creator = False

    def to_dict(self):
        """Everything the creator signs (and the block hash covers), including the stakers and VRF proof."""
        return {
            "id": self.id,
            "prevHash": self.prevHash,
            "transactions": txs_to_json_digestable_form(self.transactions),
            "ts": self.ts,
            "creator": self.creator,
            "staked_amt": self.staked_amt,
            "stakers": [stake.to_wire_dict() for stake in self.stakers],
            "seed": self.seed,
            "vrf_proof_b64": base64.b64encode(self.vrf_proof).decode() if self.vrf_proof else None,
            "files": self.files
        }

    def to_dict_with_stakers(self):
        """Wire form: the signed content plus the creator's signature."""
        block_dict = self.to_dict()
        block_dict["sign"] = base64.b64encode(self.sign).decode() if self.sign else None
        return block_dict

    def __str__(self):
        return json.dumps(self.to_dict())

    @property  ## Now you can access hash like this myblock.hash
    def hash(self):
        return hashlib.sha256(json.dumps(self.to_dict()).encode()).hexdigest()

    def sign_with(self, private_key: SigningKey):
        self.sign = private_key.sign(str(self).encode())
        return self.sign

    def is_valid_signature(self):
        return verify_signature(self.creator, self.sign, str(self).encode())

    def is_equal(self, other):
        return self.hash == other.hash and self.sign == other.sign

    @classmethod
    def from_dict(cls, block_dict):
        required = ("id", "prevHash", "transactions", "ts", "creator", "staked_amt", "stakers", "seed",
                    "vrf_proof_b64", "sign")
        transactions, files = parse_block_common(block_dict, required)
        block = cls(block_dict["prevHash"], transactions, block_dict["ts"], block_dict["id"])
        block.files = dict(files)
        block.creator = block_dict["creator"]
        load_public_key(block.creator)
        staked_amt = block_dict["staked_amt"]
        if not isinstance(staked_amt, int) or isinstance(staked_amt, bool) or staked_amt < 0:
            raise ValidationError("invalid staked_amt")
        block.staked_amt = staked_amt
        if not isinstance(block_dict["stakers"], list) or len(block_dict["stakers"]) > 1000:
            raise ValidationError("invalid stakers list")
        block.stakers = [Stake.from_dict(s) for s in block_dict["stakers"]]
        if not isinstance(block_dict["seed"], str):
            raise ValidationError("invalid seed")
        block.seed = block_dict["seed"]
        if block_dict["vrf_proof_b64"] is not None:
            block.vrf_proof = b64decode_strict(block_dict["vrf_proof_b64"])
        block.sign = b64decode_strict(block_dict["sign"])
        return block


def compute_seed(blocks: List[Block], height: int, ts: int, epoch_time) -> str:
    """
        Lottery seed for the block at `height` created at `ts`. It combines the last
        finalised block hash with the number of epochs elapsed since the parent block,
        so an epoch in which no staker wins is followed by a fresh lottery instead of the
        same (losing) draw forever.
    """
    base = blocks[valid_chain_length(height) - 1].hash
    elapsed = max(0, ts - blocks[height - 1].ts)
    lottery_round = int(elapsed // int(epoch_time * 1000))
    return hashlib.sha256(f"{base}:{lottery_round}".encode()).hexdigest()


def lottery_value(seed: str, staker_public_key: str) -> int:
    """
        Deterministic per staker draw. It depends only on the seed and the staker's
        identity, so it can't be re-rolled (ECDSA signatures are randomised, hashing a
        fresh signature would let a staker sign repeatedly until it wins).
    """
    return int(hashlib.sha256(f"{seed}:{staker_public_key}".encode()).hexdigest(), 16)


def lottery_threshold(staked_amt: int, total_stake: int) -> int:
    return (staked_amt * MAX_OUTPUT) // total_stake


def wins_lottery(seed: str, staker_public_key: str, staked_amt: int, total_stake: int) -> bool:
    if staked_amt <= 0 or total_stake <= 0 or staked_amt > total_stake:
        return False
    return lottery_value(seed, staker_public_key) < lottery_threshold(staked_amt, total_stake)


def balance_at(block_list: List[Block], height: int, publicKey, mem_pool: List[Transaction] = None,
               currStakes: Iterable[Stake] = None):
    """
        Balance of publicKey on top of block_list[:height]. Received coins / rewards count
        only once final; spent coins, pending transactions and current stakes count at once.
    """
    bal = 0
    final_len = valid_chain_length(height)
    for i in range(height):
        block = block_list[i]
        if block.slash_creator and block.creator == publicKey:
            bal -= block.staked_amt
        if not block.is_valid:
            continue
        for transaction in block.transactions:
            if transaction.sender == publicKey:
                bal -= transaction.amount
            elif transaction.receiver == publicKey and i < final_len:
                bal += transaction.payload
        if i < final_len and block.creator == publicKey:
            bal += BLOCK_REWARD
    for transaction in mem_pool or []:
        if transaction.sender == publicKey:
            bal -= transaction.amount
    for stake in currStakes or []:
        if stake.staker == publicKey:
            bal -= stake.amt
    return bal


def calc_balance_block_list(block_list: List[Block], publicKey, i, mem_pool: List[Transaction] = None,
                            currStakes: List[Stake] = None):
    return balance_at(block_list, i, publicKey, mem_pool, currStakes)


def weight_of_chain(block_list: List[Block]):
    total_weight = 0
    for block in block_list:
        if not block.is_valid:
            continue
        for stake in block.stakers:
            total_weight += stake.amt
    return total_weight


def chain_rank(block_list: List[Block]):
    """Fork choice: heaviest chain (total stake), then longest, then lowest tip hash."""
    return (weight_of_chain(block_list), len(block_list), -int(block_list[-1].hash, 16))


def validate_genesis(block: Block):
    if block.prevHash is not None:
        raise ValidationError("first block is not a genesis block")
    validate_genesis_transactions(block)
    if block.transactions[0].receiver != block.creator:
        raise ValidationError("genesis coins must go to the genesis creator")
    if block.stakers or block.staked_amt:
        raise ValidationError("genesis block cannot contain stakes")
    if not block.is_valid_signature():
        raise ValidationError("invalid genesis signature")


def validate_next_block(blocks: List[Block], block: Block, epoch_time, run_contract: Optional[Callable] = None,
                        live: bool = False, local_stakes: Optional[Dict[str, Stake]] = None, now=None):
    """
        Validates `block` as the successor of blocks[-1]. Raises ValidationError.
        live=True adds the checks that only make sense for a freshly broadcast block
        (not too old, contains every stake this node saw for the epoch).
    """
    height = len(blocks)
    prev = blocks[-1]
    if block.prevHash != prev.hash:
        raise ValidationError("block does not extend the chain")
    if not block.is_valid_signature():
        raise ValidationError("invalid block signature")

    epoch_ms = int(epoch_time * 1000)
    now = now if now is not None else now_ms()
    if block.ts > now + MAX_CLOCK_SKEW_MS:
        raise ValidationError("block timestamp in future")
    if block.ts - prev.ts < epoch_ms * 5 // 6:
        raise ValidationError(f"block created too quickly after its parent ({block.ts - prev.ts} ms)")
    if live and block.ts < now - 2 * epoch_ms:
        raise ValidationError("block timestamp too old")

    seed = compute_seed(blocks, height, block.ts, epoch_time)
    if block.seed != seed:
        raise ValidationError("invalid seed")
    if not verify_signature(block.creator, block.vrf_proof, seed.encode()):
        raise ValidationError("invalid vrf proof")

    if not block.stakers:
        raise ValidationError("block has no stakes")
    stakers_seen = set()
    total_stake = 0
    creator_stake = None
    for stake in block.stakers:
        if stake.staker in stakers_seen:
            raise ValidationError("duplicate staker in block")
        stakers_seen.add(stake.staker)
        if stake.prev_hash != block.prevHash:
            raise ValidationError("stake belongs to another epoch")
        if not stake.is_valid_signature():
            raise ValidationError("invalid signature on stake")
        total_stake += stake.amt
        if stake.staker == block.creator:
            creator_stake = stake
    if creator_stake is None or creator_stake.amt != block.staked_amt:
        raise ValidationError("creator stake missing or does not match staked_amt")
    if local_stakes:
        missing = [s for s in local_stakes.values() if s.prev_hash == block.prevHash and s.staker not in stakers_seen]
        if missing:
            raise ValidationError("block omits stakes announced in this epoch")
    if not wins_lottery(seed, block.creator, block.staked_amt, total_stake):
        raise ValidationError("creator did not win the lottery (VRF output >= threshold)")

    validate_transaction_list(
        block.transactions, blocks,
        lambda pk, pending: balance_at(blocks, height, pk, pending, block.stakers),
        ContractContext.from_blocks(blocks), run_contract)

    for stake in block.stakers:
        if stake.amt > balance_at(blocks, height, stake.staker, block.transactions):
            raise ValidationError("stake exceeds balance")
    return True


class Chain(CommonChain):

    def __init__(self, publicKey: str = None, privatekey=None, blockList: List[Block] = None,
                 epoch_time=DEFAULT_EPOCH_TIME):
        """
            If we are the first node, we create the genesis block for ourself
            otherwise we receive blockList from the network and
            we assign that to be the chain
        """
        self.epoch_time = epoch_time
        if publicKey and not blockList:
            genesis_block = Block(None, [Transaction(50, "Genesis", publicKey)])
            genesis_block.creator = publicKey
            genesis_block.sign_with(privatekey)
            super().__init__(genesis_block=genesis_block)
        elif blockList and not publicKey:
            super().__init__(block_list=blockList)
        else:
            raise ValueError("Invalid arguments")

    def to_block_dict_list(self):
        return [block.to_dict_with_stakers() for block in self.chain]

    def rewrite(self, blockList: List[Block]):
        if chain_rank(self.chain) >= chain_rank(blockList):
            return
        self.chain = list(blockList)

    def validate_block(self, block: Block, run_contract=None, live=False, local_stakes=None):
        return validate_next_block(self.chain, block, self.epoch_time, run_contract, live, local_stakes)

    def isValidBlock(self, block: Block, run_contract=None) -> bool:
        try:
            self.validate_block(block, run_contract)
            return True
        except ValidationError as e:
            print(f"\nInvalid Block: {e}\n")
            return False

    def calc_balance(self, publicKey, pending_transactions: List[Transaction] = None,
                     current_stakes: List[Stake] = None):
        return balance_at(self.chain, len(self.chain), publicKey, pending_transactions, current_stakes)

    def epoch_seed(self, ts=None):
        return compute_seed(self.chain, len(self.chain), ts if ts is not None else now_ms(), self.epoch_time)

    def checkEquivalence(self, block_list: List[Block]):
        """
            Returns -1 if there is no divergence, returns index of divergence if there is any
        """
        for i in range(min(len(self.chain), len(block_list))):
            if not self.chain[i].is_equal(block_list[i]):
                return i
        return -1


def validate_chain(blockList: List[Block], epoch_time=DEFAULT_EPOCH_TIME, trusted_prefix: int = 0,
                   run_contract: Optional[Callable] = None):
    if not blockList:
        raise ValidationError("empty chain")
    validate_genesis(blockList[0])
    for i in range(1, len(blockList)):
        try:
            validate_next_block(blockList[:i], blockList[i], epoch_time,
                                run_contract if i >= trusted_prefix else None)
        except ValidationError as e:
            raise ValidationError(f"block {i}: {e}")
    return True


def isvalidChain(blockList: List[Block], epoch_time=DEFAULT_EPOCH_TIME, trusted_prefix=0, run_contract=None):
    try:
        return validate_chain(blockList, epoch_time, trusted_prefix, run_contract)
    except ValidationError as e:
        print(f"\nInvalid Chain: {e}\n")
        return False


def is_double_sign_evidence(block1: Block, block2: Block) -> bool:
    """Two different blocks for the same parent, both validly signed by the same creator."""
    return (
        block1.creator == block2.creator
        and block1.prevHash == block2.prevHash
        and block1.prevHash is not None
        and block1.hash != block2.hash
        and block1.is_valid_signature()
        and block2.is_valid_signature()
    )
