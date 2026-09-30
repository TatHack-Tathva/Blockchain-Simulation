import json, hashlib, binascii
from typing import List, Dict, Optional, Callable
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
    transaction_exists_in_block_list,
    parse_block_common,
    validate_genesis_transactions,
    validate_transaction_list,
    load_public_key,
    verify_signature,
    canonical_json,
)

GAS_PRICE = 0.001  # coin per gas unit
MINER_REWARD = 6
DEFAULT_ROUND_TIME = 90
MAX_CLOCK_SKEW_MS = 10 * 1000


def now_ms():
    return int(datetime.now().timestamp() * 1000)


def validate_miners_list(miners_list):
    """Authority entries bind a node id to the public key that must sign its blocks."""
    if not isinstance(miners_list, list) or not miners_list or len(miners_list) > 100:
        raise ValidationError("miners list must be a non-empty list")
    keys, ids = set(), set()
    for entry in miners_list:
        if not isinstance(entry, dict) or set(entry) != {"node_id", "public_key"}:
            raise ValidationError("invalid miners list entry")
        if not isinstance(entry["node_id"], str) or not entry["node_id"] or len(entry["node_id"]) > 64:
            raise ValidationError("invalid miner node id")
        load_public_key(entry["public_key"])
        if entry["public_key"] in keys or entry["node_id"] in ids:
            raise ValidationError("duplicate miner")
        keys.add(entry["public_key"])
        ids.add(entry["node_id"])
    return miners_list


def admin_update_message(update) -> bytes:
    return canonical_json({
        "seq": update["seq"],
        "miners_list": update["miners_list"],
        "activation_block": update["activation_block"],
    }).encode()


def sign_admin_update(private_key: SigningKey, seq: int, miners_list, activation_block: int):
    update = {"seq": seq, "miners_list": miners_list, "activation_block": activation_block}
    update["signature"] = private_key.sign(admin_update_message(update)).hex()
    return update


def validate_admin_update(update, admin_public_key: str):
    if not isinstance(update, dict) or set(update) != {"seq", "miners_list", "activation_block", "signature"}:
        raise ValidationError("invalid admin update")
    for key in ("seq", "activation_block"):
        if not isinstance(update[key], int) or isinstance(update[key], bool) or update[key] < 1:
            raise ValidationError(f"invalid admin update {key}")
    validate_miners_list(update["miners_list"])
    try:
        signature = binascii.unhexlify(update["signature"])
    except (binascii.Error, TypeError, ValueError):
        raise ValidationError("invalid admin update signature encoding")
    if not verify_signature(admin_public_key, signature, admin_update_message(update)):
        raise ValidationError("admin update not signed by the network admin")
    return update


class Block(BaseBlock):
    def __init__(self, prevHash: str, transactions: List[Transaction], ts=None, id=None):
        super().__init__(prevHash, transactions, ts, id)
        self.miner_node_id = None
        self.miner_public_key = None
        self.signature = None  # This will hold the digital signature from the miner
        self.miners_list = None  # Authority set active for this block
        self.admin_update = None  # Admin signed authority change applied by this block

    def to_dict(self):
        return {
            "id": self.id,
            "prevHash": self.prevHash,
            "transactions": txs_to_json_digestable_form(self.transactions),
            "ts": self.ts,
            "miner_node_id": self.miner_node_id,
            "miner_public_key": self.miner_public_key,
            "miners_list": self.miners_list,
            "admin_update": self.admin_update,
            "signature": self.signature,
            "files": self.files
        }

    def __str__(self):
        return json.dumps(self.to_dict())

    @property  ## Now you can access hash like this myblock.hash
    def hash(self):
        return hashlib.sha256(json.dumps(self.to_dict()).encode()).hexdigest()

    def get_message_to_sign(self):
        return json.dumps({
            "id": self.id,
            "ts": self.ts,
            "prevHash": self.prevHash,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "miner_node_id": self.miner_node_id,
            "miner_public_key": self.miner_public_key,
            "miners_list": self.miners_list,
            "admin_update": self.admin_update,
            "files": self.files
        }, sort_keys=True).encode()

    def sign_with(self, private_key: SigningKey):
        self.signature = private_key.sign(self.get_message_to_sign()).hex()

    def is_valid_signature(self):
        try:
            signature = binascii.unhexlify(self.signature)
        except (binascii.Error, TypeError, ValueError):
            return False
        return verify_signature(self.miner_public_key, signature, self.get_message_to_sign())

    @classmethod
    def from_dict(cls, block_dict):
        required = ("id", "prevHash", "transactions", "ts", "miner_node_id", "miner_public_key",
                    "miners_list", "admin_update", "signature")
        transactions, files = parse_block_common(block_dict, required)
        block = cls(block_dict["prevHash"], transactions, block_dict["ts"], block_dict["id"])
        block.files = dict(files)
        if not isinstance(block_dict["miner_node_id"], str):
            raise ValidationError("invalid miner node id")
        block.miner_node_id = block_dict["miner_node_id"]
        load_public_key(block_dict["miner_public_key"])
        block.miner_public_key = block_dict["miner_public_key"]
        block.miners_list = validate_miners_list(block_dict["miners_list"])
        block.admin_update = block_dict["admin_update"]
        if not isinstance(block_dict["signature"], str):
            raise ValidationError("missing block signature")
        block.signature = block_dict["signature"]
        return block


def valid_chain_length(i):
    valid_chain_len = i  # because we use zero indexing
    return valid_chain_len


def balance_at(block_list: List[Block], height: int, publicKey, pending_transactions: List[Transaction] = None):
    bal = 0
    for i in range(valid_chain_length(height)):
        for transaction in block_list[i].transactions:
            if transaction.sender == publicKey:
                bal -= transaction.amount
            elif transaction.receiver == publicKey:
                bal += transaction.payload
        if block_list[i].miner_public_key == publicKey:
            bal += MINER_REWARD
    for transaction in pending_transactions or []:
        if transaction.sender == publicKey:
            bal -= transaction.amount
    return bal


def calc_balance_block_list(block_list: List[Block], publicKey, i, pending_transactions: List[Transaction] = None):
    return balance_at(block_list, i, publicKey, pending_transactions)


def admin_public_key(blocks: List[Block]) -> str:
    """The admin is whoever created (and signed) the genesis block - derived from the chain only."""
    return blocks[0].miner_public_key


def last_update_seq(blocks: List[Block]) -> int:
    seq = 0
    for block in blocks:
        if block.admin_update:
            seq = max(seq, block.admin_update["seq"])
    return seq


def slot_index(blocks: List[Block], height: int, ts: int, round_time) -> int:
    """Deterministic miner slot: rotates to the next authority every round_time seconds."""
    elapsed = max(0, ts - blocks[height - 1].ts)
    return height + int(elapsed // int(round_time * 1000))


def required_miner(miners_list, blocks: List[Block], height: int, ts: int, round_time):
    return miners_list[slot_index(blocks, height, ts, round_time) % len(miners_list)]


def validate_genesis(block: Block):
    if block.prevHash is not None:
        raise ValidationError("first block is not a genesis block")
    validate_genesis_transactions(block)
    if block.transactions[0].receiver != block.miner_public_key:
        raise ValidationError("genesis coins must go to the admin")
    if block.miners_list != [{"node_id": block.miner_node_id, "public_key": block.miner_public_key}]:
        raise ValidationError("genesis authority set must be the admin")
    if block.admin_update is not None:
        raise ValidationError("genesis cannot carry an admin update")
    if not block.is_valid_signature():
        raise ValidationError("invalid genesis signature")


def validate_next_block(blocks: List[Block], block: Block, round_time, run_contract: Optional[Callable] = None,
                        live: bool = False, now=None):
    height = len(blocks)
    prev = blocks[-1]
    if block.prevHash != prev.hash:
        raise ValidationError("block does not extend the chain")

    if block.admin_update is None:
        if block.miners_list != prev.miners_list:
            raise ValidationError("authority set changed without an admin update")
    else:
        update = validate_admin_update(block.admin_update, admin_public_key(blocks))
        if update["seq"] != last_update_seq(blocks) + 1:
            raise ValidationError("admin update out of sequence (replay?)")
        if update["activation_block"] > height:
            raise ValidationError("admin update not active yet")
        if update["miners_list"] != block.miners_list:
            raise ValidationError("block authority set does not match admin update")

    now = now if now is not None else now_ms()
    if block.ts < prev.ts:
        raise ValidationError("block timestamp before previous block")
    if block.ts > now + MAX_CLOCK_SKEW_MS:
        raise ValidationError("block timestamp in future")
    if live and block.ts < now - int(round_time * 1000):
        raise ValidationError("block timestamp too old")

    expected = required_miner(block.miners_list, blocks, height, block.ts, round_time)
    if block.miner_node_id != expected["node_id"] or block.miner_public_key != expected["public_key"]:
        raise ValidationError("mined by a node that is not the scheduled authority")
    if not block.is_valid_signature():
        raise ValidationError("invalid signature on block")
    if not block.transactions:
        raise ValidationError("block has no transactions")

    validate_transaction_list(
        block.transactions, blocks,
        lambda pk, pending: balance_at(blocks, height, pk, pending),
        ContractContext.from_blocks(blocks), run_contract)
    return True


class Chain(CommonChain):

    def __init__(self, publicKey=None, blockList=None, privatekey=None, node_id=None, round_time=DEFAULT_ROUND_TIME):
        """
            If we are the first node (the admin), we create the genesis block for ourself
            otherwise we receive blockList from the network and
            we assign that to be the chain
        """
        self.round_time = round_time
        if publicKey and not blockList:
            genesis_block = Block(None, [Transaction(50, "Genesis", publicKey)])
            genesis_block.miner_node_id = node_id
            genesis_block.miner_public_key = publicKey
            genesis_block.miners_list = [{"node_id": node_id, "public_key": publicKey}]
            genesis_block.sign_with(privatekey)
            super().__init__(genesis_block=genesis_block)
        elif blockList and not publicKey:
            super().__init__(block_list=blockList)
        else:
            raise ValueError("Invalid arguments")

    @property
    def admin_public_key(self):
        return admin_public_key(self.chain)

    @property
    def admin_node_id(self):
        return self.chain[0].miner_node_id

    def rewrite(self, blockList: List[Block]):
        if len(self.chain) >= len(blockList):
            return
        self.chain = list(blockList)

    def validate_block(self, block: Block, run_contract=None, live=False):
        return validate_next_block(self.chain, block, self.round_time, run_contract, live)

    def isValidBlock(self, block: Block, run_contract=None) -> bool:
        try:
            self.validate_block(block, run_contract)
            return True
        except ValidationError as e:
            print(f"\nInvalid Block: {e}\n")
            return False

    def calc_balance(self, publicKey, pending_transactions: List[Transaction] = None):
        return balance_at(self.chain, len(self.chain), publicKey, pending_transactions)


def validate_chain(blockList: List[Block], round_time=DEFAULT_ROUND_TIME, trusted_prefix: int = 0,
                   run_contract: Optional[Callable] = None):
    if not blockList:
        raise ValidationError("empty chain")
    validate_genesis(blockList[0])
    for i in range(1, len(blockList)):
        try:
            validate_next_block(blockList[:i], blockList[i], round_time, run_contract if i >= trusted_prefix else None)
        except ValidationError as e:
            raise ValidationError(f"block {i}: {e}")
    return True


# Is valid chain function
def isvalidChain(blockList: List[Block], round_time=DEFAULT_ROUND_TIME, trusted_prefix=0, run_contract=None):
    try:
        return validate_chain(blockList, round_time, trusted_prefix, run_contract)
    except ValidationError as e:
        print(f"\nInvalid Chain: {e}\n")
        return False
