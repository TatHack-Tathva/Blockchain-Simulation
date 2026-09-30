import json, hashlib, threading
from typing import List, Optional, Callable
from datetime import datetime
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
)

DEFAULT_DIFFICULTY = 5
MINER_REWARD = 6
MAX_CLOCK_SKEW_MS = 60 * 1000


class Block(BaseBlock):
    # pow block doesn't require sign for checking whether a block is valid
    def __init__(self, prevHash: str, transactions: List[Transaction], ts=None, nonce=None, id=None, miner=None):
        super().__init__(prevHash, transactions, ts, id)
        self.nonce = nonce or 0
        self.miner: Optional[str] = miner  # part of the hashed content so the reward can't be redirected

    def to_dict(self):
        return {
            "id": self.id,
            "prevHash": self.prevHash,
            "transactions": txs_to_json_digestable_form(self.transactions),
            "ts": self.ts,
            "nonce": self.nonce,
            "miner": self.miner,
            "files": self.files
        }

    def __str__(self):
        return json.dumps(self.to_dict())

    @property  ## Now you can access hash like this myblock.hash
    def hash(self):
        block_str = json.dumps(self.to_dict())
        return hashlib.sha256(block_str.encode()).hexdigest()

    @classmethod
    def from_dict(cls, block_dict):
        transactions, files = parse_block_common(block_dict, ("id", "prevHash", "transactions", "ts", "nonce", "miner"))
        nonce = block_dict["nonce"]
        if not isinstance(nonce, int) or isinstance(nonce, bool) or nonce < 0:
            raise ValidationError("invalid nonce")
        miner = block_dict["miner"]
        if miner is not None:
            load_public_key(miner)
        block = cls(block_dict["prevHash"], transactions, block_dict["ts"], nonce, block_dict["id"], miner)
        block.files = dict(files)
        return block


def meets_difficulty(block: Block, difficulty: int) -> bool:
    return block.hash.startswith("0" * difficulty)


def balance_at(blocks: List[Block], height: int, publicKey, pending_transactions: List[Transaction] = None):
    """
        Balance of publicKey on top of blocks[:height]. Money received (and miner rewards)
        only counts once the block is final (valid_chain_length), money spent counts
        immediately - otherwise coins could be spent twice inside the unfinalised window.
    """
    bal = 0
    final_len = valid_chain_length(height)
    for i in range(height):
        block = blocks[i]
        for transaction in block.transactions:
            if transaction.sender == publicKey:
                bal -= transaction.amount
            elif transaction.receiver == publicKey and i < final_len:
                bal += transaction.payload
        if i < final_len and block.miner == publicKey:
            bal += MINER_REWARD
    for transaction in pending_transactions or []:
        if transaction.sender == publicKey:
            bal -= transaction.amount
    return bal


class Chain(CommonChain):

    def __init__(self, publicKey: str = None, blockList: List[Block] = None, difficulty: int = DEFAULT_DIFFICULTY):
        """
            If we are the first node, we mine the genesis block for ouself
            otherwise we receive blockList from the network and
            we assign that to be the chain
        """
        self.difficulty = difficulty
        if publicKey and not blockList:
            genesis_block = Block(None, [Transaction(50, "Genesis", publicKey)])
            self.mine(genesis_block)
            super().__init__(genesis_block=genesis_block)
        elif blockList and not publicKey:
            super().__init__(block_list=blockList)
        else:
            raise ValueError("Invalid arguments")

    def mine(self, block: Block, stop_event: threading.Event = None) -> bool:
        """Finds a nonce. Returns False if stop_event was set (a competing block arrived)."""
        block.nonce = 0
        while not meets_difficulty(block, self.difficulty):
            block.nonce += 1
            if stop_event is not None and block.nonce % 1000 == 0 and stop_event.is_set():
                return False
        return True

    def rewrite(self, blockList: List[Block]):
        if len(self.chain) >= len(blockList):
            return
        self.chain = list(blockList)

    def calc_balance(self, publicKey, pending_transactions: List[Transaction] = None):
        return balance_at(self.chain, len(self.chain), publicKey, pending_transactions)

    def validate_block(self, block: Block, run_contract: Optional[Callable] = None):
        """Raises ValidationError unless block is a valid next block for this chain."""
        if not meets_difficulty(block, self.difficulty):
            raise ValidationError(f"insufficient proof of work (hash={block.hash})")
        if self.lastBlock.hash != block.prevHash:
            raise ValidationError("block does not extend the current chain")
        if block.miner is None:
            raise ValidationError("block has no miner")
        if block.ts > int(datetime.now().timestamp() * 1000) + MAX_CLOCK_SKEW_MS:
            raise ValidationError("block timestamp is in the future")
        if not block.transactions:
            raise ValidationError("block has no transactions")
        height = len(self.chain)
        validate_transaction_list(
            block.transactions, self.chain,
            lambda pk, pending: balance_at(self.chain, height, pk, pending),
            ContractContext.from_blocks(self.chain), run_contract)

    def isValidBlock(self, block: Block, run_contract: Optional[Callable] = None) -> bool:
        try:
            self.validate_block(block, run_contract)
            return True
        except ValidationError as e:
            print(f"\nInvalid Block: {e}\n")
            return False


def calc_balance_block_list(block_list: List[Block], publicKey, i, pending_transactions: List[Transaction] = None):
    return balance_at(block_list, i, publicKey, pending_transactions)


def validate_chain(blockList: List[Block], difficulty: int = DEFAULT_DIFFICULTY, trusted_prefix: int = 0,
                   run_contract: Optional[Callable] = None):
    """
        Raises ValidationError if blockList isn't a valid chain. Blocks in the trusted prefix
        (identical to blocks we validated before) skip contract re-execution only.
    """
    if not blockList:
        raise ValidationError("empty chain")
    genesis = blockList[0]
    if genesis.prevHash is not None:
        raise ValidationError("first block is not a genesis block")
    validate_genesis_transactions(genesis)
    if not meets_difficulty(genesis, difficulty):
        raise ValidationError("genesis block has no proof of work")
    ctx = ContractContext.from_blocks([])
    for i in range(1, len(blockList)):
        currBlock = blockList[i]
        if currBlock.prevHash != blockList[i - 1].hash:
            raise ValidationError(f"block {i} prev hash is incorrect")
        if not meets_difficulty(currBlock, difficulty):
            raise ValidationError(f"block {i} has no proof of work")
        if currBlock.miner is None:
            raise ValidationError(f"block {i} has no miner")
        if not currBlock.transactions:
            raise ValidationError(f"block {i} has no transactions")
        validate_transaction_list(
            currBlock.transactions, blockList[:i],
            lambda pk, pending, i=i: balance_at(blockList, i, pk, pending),
            ctx, run_contract if i >= trusted_prefix else None)
    return True


def isvalidChain(blockList: List[Block], difficulty: int = DEFAULT_DIFFICULTY, trusted_prefix: int = 0,
                 run_contract: Optional[Callable] = None) -> bool:
    try:
        return validate_chain(blockList, difficulty, trusted_prefix, run_contract)
    except ValidationError as e:
        print(f"\nInvalid Chain: {e}\n")
        return False
