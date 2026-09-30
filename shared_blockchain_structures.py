import json, uuid, base64, math, hashlib, keyword
from functools import lru_cache
from typing import List, Dict, Optional, Callable, Iterable
from datetime import datetime
from ecdsa import VerifyingKey, SigningKey, SECP256k1

GAS_PRICE = 0.001  # coin per gas unit
BASE_DEPLOY_COST = 5
MAX_CONTRACT_CODE_LEN = 64 * 1024
MAX_ID_LEN = 128
MAX_PEM_LEN = 1024
GENESIS_SENDER = "Genesis"


class ValidationError(Exception):
    """Raised when received data (transaction, block, stake, chain) is invalid."""


def is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


@lru_cache(maxsize=4096)
def _parse_public_key(pem: str) -> VerifyingKey:
    return VerifyingKey.from_pem(pem.encode())


def load_public_key(pem) -> VerifyingKey:
    """Parses a PEM public key or raises ValidationError."""
    if not isinstance(pem, str) or not pem or len(pem) > MAX_PEM_LEN:
        raise ValidationError("public key must be a PEM string")
    try:
        return _parse_public_key(pem)
    except Exception as e:
        raise ValidationError(f"invalid public key: {e}")


def verify_signature(public_key_pem: str, signature, message: bytes) -> bool:
    """Returns True only if `signature` is a valid signature of `message` by `public_key_pem`."""
    if not isinstance(signature, (bytes, bytearray)) or not signature:
        return False
    try:
        return load_public_key(public_key_pem).verify(bytes(signature), message)
    except Exception:
        return False


def b64decode_strict(value) -> bytes:
    if not isinstance(value, str) or not value:
        raise ValidationError("expected a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except Exception:
        raise ValidationError("invalid base64 encoding")


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def calculate_contract_id(sender: str, timestamp) -> str:
    return hashlib.sha256(f"{sender}:{timestamp}".encode("utf-8")).hexdigest()


def deploy_fee(contract_code: str) -> float:
    gas_used = len(contract_code) // 10 + BASE_DEPLOY_COST
    return gas_used * GAS_PRICE


def normalize_json_value(value):
    """Round-trips a value through JSON so tuples/lists etc. compare identically on every node."""
    return json.loads(json.dumps(value))


class Transaction:
    FIELDS = ("id", "payload", "sender", "receiver", "ts")

    def __init__(self, payload, sender: str, receiver: str, id=None, ts=None):
        self.id = id or str(uuid.uuid4())
        self.payload = payload   # amount or [code, amount] or [contract id, function_name, arguments, state, amount]
        self.sender: str = sender  # Public Key
        self.receiver: str = receiver   # Public Key or "deploy" or "invoke"
        self.sign: bytes = None
        self.ts = ts if ts is not None else datetime.now().timestamp()

    def to_dict(self):
        return {
            "id": self.id,
            "payload": self.payload,
            "sender": self.sender,
            "receiver": self.receiver,
            "ts": self.ts
        }

    def to_wire_dict(self):
        tx_dict = self.to_dict()
        if self.sender != GENESIS_SENDER:
            tx_dict["sign"] = base64.b64encode(self.sign).decode()
        return tx_dict

    def __eq__(self, other):
        if not isinstance(other, Transaction):
            return NotImplemented
        return (
            self.id == other.id and
            self.sender == other.sender and
            self.receiver == other.receiver and
            self.ts == other.ts
        )

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return json.dumps(self.to_dict())

    @property
    def key(self):
        """Identity used for duplicate detection: a signed transaction can't be replayed with the same id."""
        return (self.sender, self.id)

    @property
    def amount(self):
        if self.receiver in ("deploy", "invoke"):
            return self.payload[-1]
        return self.payload

    @property
    def is_genesis(self):
        return self.sender == GENESIS_SENDER

    def sign_with(self, private_key: SigningKey):
        self.sign = private_key.sign(str(self).encode())
        return self.sign

    def is_valid_signature(self):
        return verify_signature(self.sender, self.sign, str(self).encode())

    @classmethod
    def from_dict(cls, tx_dict, allow_genesis=False, require_sign=True):
        """
            Strictly rebuilds a transaction from its dictionary form.
            Raises ValidationError for anything that isn't a well formed transaction,
            so handlers never have to deal with half-valid data.
        """
        if not isinstance(tx_dict, dict):
            raise ValidationError("transaction must be an object")
        extra = set(tx_dict) - set(cls.FIELDS) - {"sign"}
        missing = [k for k in cls.FIELDS if k not in tx_dict]
        if missing or extra:
            raise ValidationError(f"transaction fields invalid (missing={missing}, extra={sorted(extra)})")

        tx = cls(tx_dict["payload"], tx_dict["sender"], tx_dict["receiver"], tx_dict["id"], tx_dict["ts"])
        tx.id, tx.ts = tx_dict["id"], tx_dict["ts"]  # never substitute defaults for received values
        validate_transaction_structure(tx, allow_genesis=allow_genesis)
        if tx.is_genesis:
            if "sign" in tx_dict:
                raise ValidationError("genesis transaction must not carry a signature")
        elif require_sign or "sign" in tx_dict:
            tx.sign = b64decode_strict(tx_dict.get("sign"))
        return tx


def validate_transaction_structure(tx: Transaction, allow_genesis=False):
    if not isinstance(tx.id, str) or not tx.id or len(tx.id) > MAX_ID_LEN:
        raise ValidationError("transaction id must be a non-empty string")
    if not is_number(tx.ts) or tx.ts <= 0:
        raise ValidationError("transaction timestamp must be a positive number")
    if not isinstance(tx.receiver, str) or not isinstance(tx.sender, str):
        raise ValidationError("sender and receiver must be strings")

    if tx.is_genesis:
        if not allow_genesis:
            raise ValidationError("genesis transactions are only allowed in the genesis block")
        load_public_key(tx.receiver)
        if not is_number(tx.payload) or tx.payload <= 0:
            raise ValidationError("invalid genesis amount")
        return

    load_public_key(tx.sender)
    if tx.receiver == "deploy":
        if not (isinstance(tx.payload, list) and len(tx.payload) == 2):
            raise ValidationError("deploy payload must be [code, fee]")
        code, fee = tx.payload
        if not isinstance(code, str) or not code.strip() or len(code) > MAX_CONTRACT_CODE_LEN:
            raise ValidationError("invalid contract code")
        if not is_number(fee) or fee <= 0:
            raise ValidationError("invalid deploy fee")
    elif tx.receiver == "invoke":
        if not (isinstance(tx.payload, list) and len(tx.payload) == 5):
            raise ValidationError("invoke payload must be [contract_id, function, args, state, fee]")
        contract_id, func_name, args, _state, fee = tx.payload
        if not isinstance(contract_id, str) or len(contract_id) != 64:
            raise ValidationError("invalid contract id")
        if (not isinstance(func_name, str) or not func_name.isidentifier()
                or func_name.startswith("_") or keyword.iskeyword(func_name)):
            raise ValidationError("invalid contract function name")
        if not isinstance(args, list):
            raise ValidationError("contract arguments must be a list")
        if not is_number(fee) or fee <= 0:
            raise ValidationError("invalid invoke fee")
    else:
        load_public_key(tx.receiver)
        if tx.receiver == tx.sender:
            raise ValidationError("sender and receiver must differ")
        if not is_number(tx.payload) or tx.payload <= 0:
            raise ValidationError("transaction amount must be a positive number")


def parse_wire_transaction(msg) -> Transaction:
    """
        Parses a `new_tx` message. The signature is over the exact transaction string,
        so we only accept the canonical encoding (the same string every other node will
        re-create from the block), and we require sender_pem (if present) to be the sender.
    """
    tx_str = msg.get("transaction")
    if not isinstance(tx_str, str) or len(tx_str) > MAX_CONTRACT_CODE_LEN * 2:
        raise ValidationError("missing transaction")
    try:
        tx_dict = json.loads(tx_str)
    except (ValueError, TypeError):
        raise ValidationError("transaction is not valid JSON")
    if not isinstance(tx_dict, dict):
        raise ValidationError("transaction must be an object")
    tx_dict = dict(tx_dict)
    if "sign" in tx_dict:
        raise ValidationError("signature must not be embedded in the signed transaction")
    tx_dict["sign"] = msg.get("sign")
    tx = Transaction.from_dict(tx_dict)
    if str(tx) != tx_str:
        raise ValidationError("non-canonical transaction encoding")
    sender_pem = msg.get("sender_pem")
    if sender_pem is not None and sender_pem != tx.sender:
        raise ValidationError("sender_pem does not match transaction sender")
    return tx


def make_wire_transaction(tx: Transaction, msg_id=None):
    return {
        "type": "new_tx",
        "id": msg_id or str(uuid.uuid4()),
        "transaction": str(tx),
        "sign": base64.b64encode(tx.sign).decode(),
        "sender_pem": tx.sender,
    }


def txs_to_json_digestable_form(transactions: List[Transaction]):
    return [transaction.to_wire_dict() for transaction in transactions]


def check_no_duplicate_transactions(transactions: Iterable[Transaction]):
    seen = set()
    for transaction in transactions:
        if transaction.key in seen:
            raise ValidationError("duplicate transaction inside block")
        seen.add(transaction.key)


class BaseBlock:
    def __init__(self, prevHash: str, transactions: List[Transaction], ts=None, id=None):
        self.prevHash = prevHash
        self.transactions = transactions
        self.id = id or str(uuid.uuid4())
        self.ts = ts if ts is not None else int(datetime.now().timestamp() * 1000)
        self.files: Dict[str, str] = {}

    def transaction_exists_in_block(self, transaction: Transaction):
        for tx in self.transactions:
            if tx.key == transaction.key:
                return True
        return False

    def cid_exists_in_block(self, cid: str):
        return cid in self.files


def parse_block_common(block_dict, required_fields):
    """Validates the fields every block type shares and returns (transactions, files)."""
    if not isinstance(block_dict, dict):
        raise ValidationError("block must be an object")
    missing = [k for k in required_fields if k not in block_dict]
    if missing:
        raise ValidationError(f"block missing fields {missing}")
    if not isinstance(block_dict["id"], str) or not block_dict["id"] or len(block_dict["id"]) > MAX_ID_LEN:
        raise ValidationError("invalid block id")
    if block_dict["prevHash"] is not None and not isinstance(block_dict["prevHash"], str):
        raise ValidationError("invalid prevHash")
    ts = block_dict["ts"]
    if not isinstance(ts, int) or isinstance(ts, bool) or ts <= 0:
        raise ValidationError("block timestamp must be a positive integer (ms)")
    tx_dicts = block_dict["transactions"]
    if not isinstance(tx_dicts, list):
        raise ValidationError("transactions must be a list")
    is_genesis = block_dict["prevHash"] is None
    transactions = [Transaction.from_dict(d, allow_genesis=is_genesis) for d in tx_dicts]
    files = block_dict.get("files") or {}
    if not isinstance(files, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in files.items()):
        raise ValidationError("invalid files map")
    return transactions, files


def validate_genesis_transactions(block):
    if len(block.transactions) != 1 or not block.transactions[0].is_genesis:
        raise ValidationError("genesis block must contain exactly one genesis transaction")


class CommonChain:

    def __init__(self, genesis_block=None, block_list=None):
        if genesis_block is not None:
            self.chain = [genesis_block]
        elif block_list is not None:
            if not block_list:
                raise ValueError("Invalid initialization")
            self.chain = list(block_list)
        else:
            raise ValueError("Invalid initialization")

    @property
    def lastBlock(self):
        return self.chain[-1]

    @property
    def genesis_hash(self):
        return self.chain[0].hash

    def to_block_dict_list(self):
        return [block.to_dict() for block in self.chain]

    def transaction_exists_in_chain(self, transaction: Transaction):
        for block in reversed(self.chain):
            if block.transaction_exists_in_block(transaction):
                return True
        return False

    def cid_exists_in_chain(self, cid: str):
        for block in reversed(self.chain):
            if block.cid_exists_in_block(cid):
                return True
        return False

    def common_prefix_length(self, block_list) -> int:
        n = 0
        for mine, theirs in zip(self.chain, block_list):
            if mine.hash != theirs.hash:
                break
            n += 1
        return n


class Wallet:
    def __init__(self, private_key_pem: str = None):
        if not private_key_pem:
            self.private_key = SigningKey.generate(curve=SECP256k1)
        else:
            self.private_key = SigningKey.from_pem(private_key_pem)

        self.private_key_pem = self.private_key.to_pem().decode()

        self.public_key = self.private_key.get_verifying_key()

        self.public_key_pem = self.public_key.to_pem().decode()

    def __repr__(self):
        # Never leak the private key through logging / debugging output
        return f"Wallet(public_key={self.public_key_pem[27:60]}...)"


def transaction_exists_in_block_list(blockList, transaction_tc: Transaction, idx):
    """Returns True if the transaction appears in any of blockList[0:idx]."""
    for i in range(min(idx, len(blockList))):
        for transaction in blockList[i].transactions:
            if transaction.key == transaction_tc.key:
                # We sign the id of the transaction, a replayed transaction reuses the id
                return True
    return False


def valid_chain_length(i):
    valid_chain_len = i  # because we use zero indexing
    # We must be careful in how we choose which blocks are valid, since a block that was valid in before a new block is added shouldn't then become of undecided nature
    # i.e for exapmple when length is 9 say the first 7 blocks are considered valid then when length becomes 10, it shouldn't become 5 or something like that
    # For larger chains of length greater than 250 we assume blocks of depth greater than 50 is valid
    if(valid_chain_len < 250):
        valid_chain_len = valid_chain_len - (valid_chain_len // 5)
    else:
        valid_chain_len -= 50
    return valid_chain_len


class ContractContext:
    """
        Tracks deployed contracts and their latest state while validating a sequence of
        transactions, so that several deploy/invoke transactions in one block are
        validated against each other and not only against the chain.
    """

    def __init__(self, contracts: Dict[str, str], states: Dict[str, object]):
        self.contracts = dict(contracts)
        self.states = dict(states)

    @classmethod
    def from_blocks(cls, blocks):
        contracts, states = {}, {}
        for block in blocks:
            for tx in block.transactions:
                if tx.receiver == "deploy":
                    contracts.setdefault(calculate_contract_id(tx.sender, tx.ts), tx.payload[0])
                elif tx.receiver == "invoke":
                    states[tx.payload[0]] = tx.payload[3]
        return cls(contracts, states)

    def apply(self, tx: Transaction):
        if tx.receiver == "deploy":
            self.contracts.setdefault(calculate_contract_id(tx.sender, tx.ts), tx.payload[0])
        elif tx.receiver == "invoke":
            self.states[tx.payload[0]] = tx.payload[3]


def validate_contract_transaction(tx: Transaction, ctx: ContractContext, run_contract: Optional[Callable]):
    """
        Contract specific checks. This must only be called after structure, signature and
        balance checks passed, so unsigned / unfunded transactions never execute code.
        run_contract(code, func_name, args, state) -> {"error", "state", "gas_used"}
        If run_contract is None the (expensive) re-execution is skipped - only used for
        blocks we have already fully validated before (our own stored chain prefix).
    """
    if tx.receiver == "deploy":
        if tx.payload[-1] != deploy_fee(tx.payload[0]):
            raise ValidationError("deploy fee does not match contract size")
        if calculate_contract_id(tx.sender, tx.ts) in ctx.contracts:
            raise ValidationError("contract id already deployed")
    elif tx.receiver == "invoke":
        contract_id, func_name, args, state, fee = tx.payload
        code = ctx.contracts.get(contract_id)
        if code is None:
            raise ValidationError(f"unknown contract '{contract_id}'")
        if run_contract is not None:
            response = run_contract(code, func_name, args, ctx.states.get(contract_id, {}))
            if response.get("error") is not None:
                raise ValidationError(f"contract execution failed: {response['error']}")
            if normalize_json_value(response.get("state")) != state:
                raise ValidationError("contract state does not match execution result")
            if fee != response.get("gas_used", 0) * GAS_PRICE:
                raise ValidationError("invoke fee does not match gas used")


def validate_transaction_list(transactions: List[Transaction], prior_blocks, balance_fn: Callable,
                              ctx: ContractContext, run_contract: Optional[Callable]):
    """
        Shared transaction validation for a block (all consensus types).
        Order per transaction: duplicate -> signature -> balance -> contract execution.
        balance_fn(public_key, pending_transactions) must return the spendable balance.
    """
    check_no_duplicate_transactions(transactions)
    pending: List[Transaction] = []
    for tx in transactions:
        if tx.is_genesis:
            raise ValidationError("genesis transaction outside genesis block")
        validate_transaction_structure(tx)
        if transaction_exists_in_block_list(prior_blocks, tx, len(prior_blocks)):
            raise ValidationError("duplicate transaction (already in chain)")
        if not tx.is_valid_signature():
            raise ValidationError("invalid transaction signature")
        if tx.amount > balance_fn(tx.sender, pending):
            raise ValidationError("insufficient balance")
        validate_contract_transaction(tx, ctx, run_contract)
        ctx.apply(tx)
        pending.append(tx)
