"""
    Phase 1: targeted tests for the audit findings (CB-xx / SV-xx) and the validation fixes.
    These are deterministic unit / handler level tests (no network, no sleeps).
"""
import asyncio, base64, copy, json, os, stat, time, threading
import pytest
import psutil

from conftest import run, FakeWebSocket, free_port

from shared_blockchain_structures import (
    Transaction, Wallet, ValidationError, CommonChain, ContractContext, transaction_exists_in_block_list,
    parse_wire_transaction, make_wire_transaction, validate_transaction_list, deploy_fee,
    calculate_contract_id, GAS_PRICE,
)
from consensus.pow import blockchain_structures as powbs
from consensus.pos import blockchain_structures as posbs
from consensus.poa import blockchain_structures as poabs
from consensus.pow.p2p import Peer as PowPeer
from consensus.pos.p2p import Peer as PosPeer
from consensus.poa.p2p import Peer as PoaPeer
from consensus.pow.mal_node import Peer as PowMalPeer
from consensus.base_peer import SeenMessageCache, sign_peer_record, verify_peer_record
from consensus import base_peer

EPOCH = 1  # seconds; PoS tests set timestamps explicitly


def signed_tx(wallet, receiver, amount, **kw):
    tx = Transaction(amount, wallet.public_key_pem, receiver, **kw)
    tx.sign_with(wallet.private_key)
    return tx


def roundtrip(tx):
    return Transaction.from_dict(json.loads(json.dumps(tx.to_wire_dict())))


# ---------------------------------------------------------------- shared structures

def test_transaction_exists_in_block_list_returns_true_when_found():  # CB-01
    a, b = Wallet(), Wallet()
    chain = powbs.Chain(publicKey=a.public_key_pem, difficulty=1)
    tx = signed_tx(a, b.public_key_pem, 5)
    block = powbs.Block(chain.lastBlock.hash, [tx], miner=a.public_key_pem)
    blocks = chain.chain + [block]
    assert transaction_exists_in_block_list(blocks, tx, 2) is True
    assert transaction_exists_in_block_list(blocks, tx, 1) is False
    assert transaction_exists_in_block_list(blocks, signed_tx(a, b.public_key_pem, 5), 2) is False


def test_common_chain_last_block_is_property():  # CB-08
    a = Wallet()
    chain = powbs.Chain(publicKey=a.public_key_pem, difficulty=1)
    assert isinstance(CommonChain.__dict__["lastBlock"], property)
    assert chain.lastBlock is chain.chain[0]


@pytest.mark.parametrize("amount", [-5, 0, True, "10", None, [1]])
def test_transaction_structure_rejects_bad_amounts(amount):
    a, b = Wallet(), Wallet()
    d = {"id": "x", "payload": amount, "sender": a.public_key_pem, "receiver": b.public_key_pem, "ts": 1.0,
         "sign": base64.b64encode(b"sig").decode()}
    with pytest.raises(ValidationError):
        Transaction.from_dict(d)


def test_transaction_structure_rejects_malformed_fields():
    a, b = Wallet(), Wallet()
    good = signed_tx(a, b.public_key_pem, 3).to_wire_dict()
    assert Transaction.from_dict(good).is_valid_signature()
    for mutate in [
        lambda d: d.pop("ts"),
        lambda d: d.update(extra=1),
        lambda d: d.update(sender="not a key"),
        lambda d: d.update(receiver="nobody"),
        lambda d: d.update(sign="%%%not-base64"),
        lambda d: d.update(sender="Genesis"),
        lambda d: d.update(id=""),
    ]:
        d = copy.deepcopy(good)
        mutate(d)
        with pytest.raises(ValidationError):
            Transaction.from_dict(d)
    with pytest.raises(ValidationError):
        Transaction.from_dict(["not", "a", "dict"])


def test_wire_transaction_rejects_non_canonical_encoding():
    a, b = Wallet(), Wallet()
    tx = signed_tx(a, b.public_key_pem, 3)
    msg = make_wire_transaction(tx)
    assert parse_wire_transaction(msg).key == tx.key
    reordered = dict(msg)
    reordered["transaction"] = json.dumps(dict(reversed(list(tx.to_dict().items()))))
    with pytest.raises(ValidationError, match="non-canonical"):
        parse_wire_transaction(reordered)
    spaced = dict(msg, transaction=json.dumps(tx.to_dict(), indent=1))
    with pytest.raises(ValidationError):
        parse_wire_transaction(spaced)


def test_sender_pem_must_match_transaction_sender():  # CB-10
    attacker, victim, c = Wallet(), Wallet(), Wallet()
    tx = Transaction(5, victim.public_key_pem, c.public_key_pem)
    tx.sign = attacker.private_key.sign(str(tx).encode())
    msg = make_wire_transaction(tx)
    msg["sender_pem"] = attacker.public_key_pem
    with pytest.raises(ValidationError, match="sender_pem"):
        parse_wire_transaction(msg)
    msg["sender_pem"] = victim.public_key_pem
    assert parse_wire_transaction(msg).is_valid_signature() is False


# -------------------------------------------------------------------------- PoW

def pow_chain(wallet, difficulty=1):
    return powbs.Chain(publicKey=wallet.public_key_pem, difficulty=difficulty)


def pow_block(chain, miner_wallet, txs):
    block = powbs.Block(chain.lastBlock.hash, txs, miner=miner_wallet.public_key_pem)
    chain.mine(block)
    return block


def test_pow_rejects_negative_amount_in_block():  # CB-02
    a, b = Wallet(), Wallet()
    chain = pow_chain(a)
    neg = signed_tx(a, b.public_key_pem, -10)
    with pytest.raises(ValidationError):
        chain.validate_block(pow_block(chain, a, [neg]))
    # and it can't even be parsed from the wire
    with pytest.raises(ValidationError):
        roundtrip(neg)


def test_pow_rejects_overspend_and_accepts_valid():
    a, b = Wallet(), Wallet()
    chain = pow_chain(a)
    with pytest.raises(ValidationError, match="insufficient"):
        chain.validate_block(pow_block(chain, a, [signed_tx(a, b.public_key_pem, 30), signed_tx(a, b.public_key_pem, 30)]))
    good = pow_block(chain, a, [signed_tx(a, b.public_key_pem, 30)])
    chain.validate_block(good)


@pytest.mark.parametrize("consensus", ["pow", "pos", "poa"])
def test_duplicate_transaction_within_block_rejected(consensus):
    a, b = Wallet(), Wallet()
    tx = signed_tx(a, b.public_key_pem, 1)
    with pytest.raises(ValidationError, match="duplicate"):
        validate_transaction_list([tx, tx], [], lambda pk, pending: 100, ContractContext({}, {}), None)
    if consensus == "pow":
        chain = pow_chain(a)
        with pytest.raises(ValidationError, match="duplicate"):
            chain.validate_block(pow_block(chain, a, [tx, roundtrip(tx)]))
    elif consensus == "pos":
        chain = pos_chain(a)
        s = make_stake(a, 5, chain.lastBlock.hash)
        with pytest.raises(ValidationError, match="duplicate"):
            chain.validate_block(pos_block(chain, a, [tx, roundtrip(tx)], [s]))
    else:
        chain = poa_chain(a)
        with pytest.raises(ValidationError, match="duplicate"):
            chain.validate_block(poa_block(chain, a, [tx, roundtrip(tx)]))


@pytest.mark.parametrize("consensus", ["pow", "pos", "poa"])
def test_duplicate_transaction_across_blocks_rejected(consensus):
    a, b = Wallet(), Wallet()
    tx = signed_tx(a, b.public_key_pem, 1)
    if consensus == "pow":
        chain = pow_chain(a)
        b1 = pow_block(chain, a, [tx])
        chain.validate_block(b1)
        chain.chain.append(b1)
        b2 = pow_block(chain, a, [roundtrip(tx)])
        with pytest.raises(ValidationError, match="duplicate"):
            chain.validate_block(b2)
        with pytest.raises(ValidationError, match="duplicate"):
            powbs.validate_chain(chain.chain + [b2], 1)
    elif consensus == "pos":
        chain = pos_chain(a)
        b1 = pos_block(chain, a, [tx], [make_stake(a, 5, chain.lastBlock.hash)])
        chain.validate_block(b1)
        chain.chain.append(b1)
        b2 = pos_block(chain, a, [roundtrip(tx)], [make_stake(a, 5, chain.lastBlock.hash)])
        with pytest.raises(ValidationError, match="duplicate"):
            chain.validate_block(b2)
        with pytest.raises(ValidationError, match="duplicate"):
            posbs.validate_chain(chain.chain + [b2], EPOCH)
    else:
        chain = poa_chain(a)
        b1 = poa_block(chain, a, [tx])
        chain.validate_block(b1)
        chain.chain.append(b1)
        b2 = poa_block(chain, a, [roundtrip(tx)])
        with pytest.raises(ValidationError, match="duplicate"):
            chain.validate_block(b2)
        with pytest.raises(ValidationError, match="duplicate"):
            poabs.validate_chain(chain.chain + [b2], ROUND)


def test_pow_miner_is_part_of_block_hash_and_survives_sync():
    a, b = Wallet(), Wallet()
    chain = pow_chain(a)
    block = pow_block(chain, a, [signed_tx(a, b.public_key_pem, 5)])
    h = block.hash
    block.miner = b.public_key_pem
    assert block.hash != h  # a relay can't redirect the reward
    block.miner = a.public_key_pem
    rebuilt = powbs.Block.from_dict(json.loads(json.dumps(block.to_dict())))
    assert rebuilt.miner == a.public_key_pem and rebuilt.hash == h


def test_pow_chain_validation_and_genesis():  # CB-22 / CB-09
    a, b = Wallet(), Wallet()
    chain = pow_chain(a)
    b1 = pow_block(chain, a, [signed_tx(a, b.public_key_pem, 5)])
    chain.chain.append(b1)
    assert powbs.validate_chain(chain.chain, 1)
    tampered = [powbs.Block.from_dict(d) for d in chain.to_block_dict_list()]
    tampered[1].transactions[0].payload = 40  # breaks signature and PoW
    with pytest.raises(ValidationError):
        powbs.validate_chain(tampered, 1)


def test_pow_mining_can_be_cancelled():
    a = Wallet()
    chain = pow_chain(a)
    chain.difficulty = 12
    block = powbs.Block(chain.lastBlock.hash, [], miner=a.public_key_pem)
    stop = threading.Event()
    stop.set()
    assert chain.mine(block, stop) is False


# -------------------------------------------------------------------------- PoS

def pos_chain(wallet):
    chain = posbs.Chain(publicKey=wallet.public_key_pem, privatekey=wallet.private_key, epoch_time=EPOCH)
    chain.chain[0].ts = posbs.now_ms() - 120_000
    chain.chain[0].sign_with(wallet.private_key)
    return chain


def make_stake(wallet, amt, prev_hash):
    stake = posbs.Stake(wallet.public_key_pem, amt, prev_hash)
    stake.sign_with(wallet.private_key)
    return stake


def pos_block(chain, creator, txs, stakes, ts=None, staked_amt=None):
    prev = chain.lastBlock
    ts = ts if ts is not None else prev.ts + EPOCH * 1000
    seed = posbs.compute_seed(chain.chain, len(chain.chain), ts, EPOCH)
    block = posbs.Block(prev.hash, txs, ts)
    block.creator = creator.public_key_pem
    own = [s for s in stakes if s.staker == creator.public_key_pem]
    block.staked_amt = staked_amt if staked_amt is not None else (own[0].amt if own else 0)
    block.stakers = list(stakes)
    block.seed = seed
    block.vrf_proof = creator.private_key.sign(seed.encode())
    block.sign_with(creator.private_key)
    return block


def funded_pos_chain():
    """Genesis by a (56 coins), block 1 gives b 30 coins. Returns chain, a, b."""
    a, b = Wallet(), Wallet()
    chain = pos_chain(a)
    b1 = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 30)], [make_stake(a, 5, chain.lastBlock.hash)])
    chain.validate_block(b1)
    chain.chain.append(b1)
    return chain, a, b


def find_ts(chain, predicate):
    prev = chain.lastBlock
    for i in range(1, 500):
        ts = prev.ts + i * EPOCH * 1000
        seed = posbs.compute_seed(chain.chain, len(chain.chain), ts, EPOCH)
        if predicate(seed):
            return ts
    raise AssertionError("no suitable round found")


def test_pos_vrf_winner_accepted_loser_rejected_both_sides():  # CB-16 / previous item 1
    chain, a, b = funded_pos_chain()
    stakes = [make_stake(a, 1, chain.lastBlock.hash), make_stake(b, 25, chain.lastBlock.hash)]
    total = 26
    ts = find_ts(chain, lambda seed: posbs.wins_lottery(seed, b.public_key_pem, 25, total)
                 and not posbs.wins_lottery(seed, a.public_key_pem, 1, total))
    winner = pos_block(chain, b, [signed_tx(b, a.public_key_pem, 1)], stakes, ts=ts)
    chain.validate_block(winner)  # receiver accepts the creator side's winner
    loser = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 1)], stakes, ts=ts)
    with pytest.raises(ValidationError, match="lottery"):
        chain.validate_block(loser)


def test_pos_lottery_depends_on_seed_and_identity_only():  # previous item 2
    a, b = Wallet(), Wallet()
    seed = "ab" * 32
    # ECDSA signatures are randomised: two proofs for the same seed differ...
    assert a.private_key.sign(seed.encode()) != a.private_key.sign(seed.encode())
    # ...but the lottery draw can't be re-rolled by re-signing
    assert posbs.lottery_value(seed, a.public_key_pem) == posbs.lottery_value(seed, a.public_key_pem)
    assert posbs.lottery_value(seed, a.public_key_pem) != posbs.lottery_value(seed, b.public_key_pem)
    chain = pos_chain(a)
    block = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 1)], [make_stake(a, 5, chain.lastBlock.hash)])
    block.vrf_proof = b.private_key.sign(block.seed.encode())  # proof by somebody else
    block.sign_with(a.private_key)
    with pytest.raises(ValidationError, match="vrf"):
        chain.validate_block(block)


def test_pos_seed_progresses_each_epoch():  # previous item 3
    chain, a, b = funded_pos_chain()
    prev_ts = chain.lastBlock.ts
    seeds = {posbs.compute_seed(chain.chain, len(chain.chain), prev_ts + r * EPOCH * 1000, EPOCH) for r in range(1, 6)}
    assert len(seeds) == 5
    # with two stakers there is an epoch in which nobody wins, followed by one where somebody does
    stakes = {a.public_key_pem: 10, b.public_key_pem: 10}
    outcomes = []
    for r in range(1, 200):
        seed = posbs.compute_seed(chain.chain, len(chain.chain), prev_ts + r * EPOCH * 1000, EPOCH)
        outcomes.append(any(posbs.wins_lottery(seed, pk, amt, 20) for pk, amt in stakes.items()))
    assert True in outcomes and False in outcomes


def test_pos_block_must_contain_signed_stakes_and_matching_creator_stake():  # previous item 4/5
    chain, a, b = funded_pos_chain()
    prev = chain.lastBlock.hash
    tx = [signed_tx(a, b.public_key_pem, 1)]
    with pytest.raises(ValidationError, match="no stakes"):
        chain.validate_block(pos_block(chain, a, tx, []))
    with pytest.raises(ValidationError, match="creator stake"):
        chain.validate_block(pos_block(chain, a, tx, [make_stake(a, 5, prev)], staked_amt=6))
    unsigned = make_stake(b, 5, prev)
    unsigned.sign = a.private_key.sign(str(unsigned).encode())
    with pytest.raises(ValidationError, match="signature on stake"):
        chain.validate_block(pos_block(chain, a, tx, [make_stake(a, 5, prev), unsigned]))
    with pytest.raises(ValidationError, match="another epoch"):
        chain.validate_block(pos_block(chain, a, tx, [make_stake(a, 5, chain.chain[0].hash)]))
    with pytest.raises(ValidationError, match="duplicate staker"):
        chain.validate_block(pos_block(chain, a, tx, [make_stake(a, 5, prev), make_stake(a, 5, prev)]))
    ts = find_ts(chain, lambda seed: posbs.wins_lottery(seed, a.public_key_pem, 5, 505))
    with pytest.raises(ValidationError, match="exceeds balance"):
        chain.validate_block(pos_block(chain, a, tx, [make_stake(a, 5, prev), make_stake(b, 500, prev)], ts=ts))
    # stakes / seed / vrf proof are covered by the block signature and hash
    good = pos_block(chain, a, tx, [make_stake(a, 5, prev)])
    h = good.hash
    good.stakers.append(make_stake(b, 5, prev))
    assert good.hash != h and not good.is_valid_signature()


def test_pos_live_block_omitting_known_stakes_rejected():  # previous item 4
    chain, a, b = funded_pos_chain()
    prev = chain.lastBlock.hash
    b_stake = make_stake(b, 20, prev)
    block = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 1)], [make_stake(a, 5, prev)],
                      ts=posbs.now_ms())
    chain.validate_block(block)  # fine for chain sync (no local knowledge)
    with pytest.raises(ValidationError, match="omits stakes"):
        chain.validate_block(block, live=True, local_stakes={b.public_key_pem: b_stake})


def make_pos_node(chain_wallet=None, chain=None, staker=True):
    node = PosPeer("127.0.0.1", free_port(), "node", staker, "n", "n", epoch_time=EPOCH)
    if chain_wallet is not None:
        node.wallet = chain_wallet
    node.chain = chain
    return node


def test_pos_stake_announcement_keeps_signature_and_rejects_duplicates():  # previous item 5 / SV-16
    chain, a, b = funded_pos_chain()
    node = make_pos_node(a, chain)
    stake = make_stake(b, 10, chain.lastBlock.hash)
    ws = FakeWebSocket()

    async def scenario():
        await node.handle_messages(ws, {"type": "stake_announcement", "id": "s1", "stake": stake.to_wire_dict()})
        assert node.current_stakes[b.public_key_pem].sign == stake.sign
        dup = make_stake(b, 11, chain.lastBlock.hash)
        await node.process_raw(ws, json.dumps({"type": "stake_announcement", "id": "s2", "stake": dup.to_wire_dict()}))
        assert node.current_stakes[b.public_key_pem].amt == 10
        assert any("duplicate stake" in r["reason"] for r in node.recent_rejections)
        stale = make_stake(Wallet(), 1, chain.chain[0].hash)
        await node.process_raw(ws, json.dumps({"type": "stake_announcement", "id": "s3", "stake": stale.to_wire_dict()}))
        assert len(node.current_stakes) == 1
    run(scenario())


def test_pos_double_sign_detected_and_slashed():  # CB-04 / CB-05 / previous item 6
    chain, a, b = funded_pos_chain()
    node = make_pos_node(b, chain)
    prev = chain.lastBlock.hash
    stakes = [make_stake(a, 5, prev)]
    ts = chain.lastBlock.ts + EPOCH * 1000
    blk1 = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 1)], stakes, ts=ts)
    blk2 = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 2)], stakes, ts=ts)
    chain.chain.append(blk1)
    ws = FakeWebSocket()
    node.server_connections.add(ws)

    async def scenario():
        await node.handle_messages(ws, {"type": "new_block", "id": "nb", "block": blk2.to_dict_with_stakers()})
    run(scenario())
    assert node.chain.chain[2].is_valid is False and node.chain.chain[2].slash_creator is True
    assert len(node.chain.chain) == 3  # chain not trimmed / rewritten
    slash = ws.of_type("slash_announcement")
    assert len(slash) == 1
    ev = {slash[0]["evidence1"]["id"], slash[0]["evidence2"]["id"]}
    assert ev == {blk1.id, blk2.id}  # both blocks, not block1 twice
    assert node.spendable_balance(a.public_key_pem) < chain.calc_balance(a.public_key_pem) + 1


def test_pos_slash_announcement_requires_real_evidence():  # SV-20
    chain, a, b = funded_pos_chain()
    node = make_pos_node(b, chain)
    prev = chain.lastBlock.hash
    c = Wallet()
    blk_a = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 1)], [make_stake(a, 5, prev)])
    blk_c = pos_block(chain, c, [signed_tx(a, b.public_key_pem, 2)], [make_stake(c, 5, prev)])
    forged = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 3)], [make_stake(a, 5, prev)])
    forged.transactions = [signed_tx(a, b.public_key_pem, 4)]  # signature no longer matches
    ws = FakeWebSocket()
    node.server_connections.add(ws)
    original = list(node.chain.chain)

    async def scenario():
        for i, (e1, e2) in enumerate([(blk_a, blk_c), (blk_a, forged), (blk_a, blk_a)]):
            await node.process_raw(ws, json.dumps({
                "type": "slash_announcement", "id": f"sl{i}", "pos": 0,
                "evidence1": e1.to_dict_with_stakers(), "evidence2": e2.to_dict_with_stakers()}))
    run(scenario())
    assert node.chain.chain == original and all(blk.is_valid for blk in node.chain.chain)
    assert ws.of_type("slash_announcement") == []


def test_pos_fork_with_different_creators_uses_heaviest_chain():  # previous item 6
    chain, a, b = funded_pos_chain()
    node = make_pos_node(b, pos_chain_copy(chain))
    prev = chain.lastBlock.hash
    light = pos_block(chain, a, [signed_tx(a, b.public_key_pem, 1)], [make_stake(a, 5, prev)])
    heavy_stakes = [make_stake(a, 1, prev), make_stake(b, 25, prev)]
    ts = find_ts(chain, lambda seed: posbs.wins_lottery(seed, b.public_key_pem, 25, 26))
    heavy = pos_block(chain, b, [signed_tx(b, a.public_key_pem, 1)], heavy_stakes, ts=ts)
    node.chain.chain.append(light)
    heavy_chain = chain.chain + [heavy]
    ws = FakeWebSocket()

    async def scenario():
        await node.handle_messages(ws, {"type": "chain", "id": "c1", "chain": [x.to_dict_with_stakers() for x in heavy_chain]})
    run(scenario())
    assert node.chain.lastBlock.hash == heavy.hash
    assert all(blk.is_valid for blk in node.chain.chain)  # ordinary fork: nobody slashed


def pos_chain_copy(chain):
    return posbs.Chain(blockList=[posbs.Block.from_dict(json.loads(json.dumps(b.to_dict_with_stakers())))
                                  for b in chain.chain], epoch_time=EPOCH)


@pytest.mark.parametrize("consensus", ["pow", "pos", "poa"])
def test_chain_sync_rejects_different_genesis(consensus):  # previous item 7
    a, b = Wallet(), Wallet()
    if consensus == "pow":
        node = PowPeer("127.0.0.1", free_port(), "n", False, "n", "n", difficulty=1)
        node.chain = pow_chain(a)
        other = pow_chain(b)
        other.chain.append(pow_block(other, b, [signed_tx(b, a.public_key_pem, 1)]))
        dicts = other.to_block_dict_list()
    elif consensus == "pos":
        node = make_pos_node(a, pos_chain(a))
        other = pos_chain(b)
        other.chain.append(pos_block(other, b, [signed_tx(b, a.public_key_pem, 1)], [make_stake(b, 5, other.lastBlock.hash)]))
        dicts = other.to_block_dict_list()
    else:
        node = PoaPeer("127.0.0.1", free_port(), "n", "n", "n", round_time=ROUND)
        node.chain = poa_chain(a)
        other = poa_chain(b)
        other.chain.append(poa_block(other, b, [signed_tx(b, a.public_key_pem, 1)]))
        dicts = other.to_block_dict_list()
    before = node.chain.genesis_hash
    run(node.process_raw(FakeWebSocket(), json.dumps({"type": "chain", "id": "g", "chain": dicts})))
    assert node.chain.genesis_hash == before and len(node.chain.chain) == 1
    assert any("different genesis" in r["reason"] for r in node.recent_rejections)


def test_invalid_chain_is_not_adopted():  # CB-09
    a, b = Wallet(), Wallet()
    node = PowPeer("127.0.0.1", free_port(), "n", False, "n", "n", difficulty=1)
    node.chain = pow_chain(a)
    dicts = node.chain.to_block_dict_list()
    bad = pow_block(node.chain, a, [signed_tx(a, b.public_key_pem, 1000)])  # overspend
    run(node.process_raw(FakeWebSocket(), json.dumps({"type": "chain", "id": "c", "chain": dicts + [bad.to_dict()]})))
    assert len(node.chain.chain) == 1


# -------------------------------------------------------------------------- PoA

ROUND = 5


def poa_chain(wallet, node_id="admin-node"):
    chain = poabs.Chain(publicKey=wallet.public_key_pem, privatekey=wallet.private_key, node_id=node_id, round_time=ROUND)
    chain.chain[0].ts = poabs.now_ms() - 3_600_000
    chain.chain[0].sign_with(wallet.private_key)
    return chain


def poa_block(chain, miner, txs, node_id=None, miners_list=None, admin_update=None, ts=None):
    height = len(chain.chain)
    miners_list = miners_list or chain.lastBlock.miners_list
    if ts is None:
        # first timestamp whose slot belongs to this miner
        ts = chain.lastBlock.ts + 1
        while poabs.required_miner(miners_list, chain.chain, height, ts, ROUND)["public_key"] != miner.public_key_pem:
            ts += ROUND * 1000
    entry = next(e for e in miners_list if e["public_key"] == miner.public_key_pem) if node_id is None else {"node_id": node_id}
    block = poabs.Block(chain.lastBlock.hash, txs, ts)
    block.miner_node_id = entry["node_id"]
    block.miner_public_key = miner.public_key_pem
    block.miners_list = miners_list
    block.admin_update = admin_update
    block.sign_with(miner.private_key)
    return block


def test_poa_genesis_signature_validates():  # CB-17
    a = Wallet()
    chain = poa_chain(a)
    assert poabs.validate_chain(chain.chain, ROUND)
    chain.chain[0].signature = None
    with pytest.raises(ValidationError):
        poabs.validate_chain(chain.chain, ROUND)


def test_poa_unscheduled_or_unauthorised_miner_rejected():  # SV-22
    admin, mallory, b = Wallet(), Wallet(), Wallet()
    chain = poa_chain(admin)
    tx = [signed_tx(admin, b.public_key_pem, 1)]
    forged = poa_block(chain, mallory, tx, node_id="admin-node", ts=chain.lastBlock.ts + 1)
    with pytest.raises(ValidationError, match="scheduled authority"):
        chain.validate_block(forged)
    chain.validate_block(poa_block(chain, admin, tx))


def test_poa_authority_change_requires_admin_signed_block_update():  # previous items 10/11
    admin, bob, mallory = Wallet(), Wallet(), Wallet()
    chain = poa_chain(admin)
    new_list = chain.lastBlock.miners_list + [{"node_id": "bob-node", "public_key": bob.public_key_pem}]
    tx = [signed_tx(admin, bob.public_key_pem, 1)]
    with pytest.raises(ValidationError, match="without an admin update"):
        chain.validate_block(poa_block(chain, admin, tx, miners_list=new_list))
    fake = poabs.sign_admin_update(mallory.private_key, 1, new_list, 1)
    with pytest.raises(ValidationError, match="admin"):
        chain.validate_block(poa_block(chain, admin, tx, miners_list=new_list, admin_update=fake))
    real = poabs.sign_admin_update(admin.private_key, 1, new_list, 1)
    blk = poa_block(chain, admin, tx, miners_list=new_list, admin_update=real)
    chain.validate_block(blk)
    chain.chain.append(blk)
    replay = poa_block(chain, admin, [signed_tx(admin, bob.public_key_pem, 2)], miners_list=new_list, admin_update=real)
    with pytest.raises(ValidationError, match="sequence"):
        chain.validate_block(replay)


def test_poa_network_details_cannot_install_admin():  # CB-15 / previous item 10
    admin, mallory = Wallet(), Wallet()
    node = PoaPeer("127.0.0.1", free_port(), "n", "n", "n", round_time=ROUND)
    node.chain = poa_chain(admin)
    evil = poabs.sign_admin_update(mallory.private_key, 1,
                                   [{"node_id": "m", "public_key": mallory.public_key_pem}], 1)
    ws = FakeWebSocket()
    run(node.handle_messages(ws, {"type": "network_details", "id": "nd", "admin": "m", "miners": [[["m"], 1]],
                                  "pending_updates": [evil]}))
    assert node.pending_updates == []
    assert node.chain.admin_public_key == admin.public_key_pem
    assert node.get_current_miners_list() == node.chain.lastBlock.miners_list
    run(node.process_raw(ws, json.dumps({"type": "miners_list_update", "id": "mu", "update": evil})))
    assert node.pending_updates == []


def test_poa_transaction_propagation_uses_ts_field():  # previous item 9
    admin, b = Wallet(), Wallet()
    node = PoaPeer("127.0.0.1", free_port(), "n", "n", "n", round_time=ROUND)
    node.wallet = admin
    node.chain = poa_chain(admin)
    tx = signed_tx(admin, b.public_key_pem, 5)
    ws = FakeWebSocket()
    run(node.handle_messages(ws, make_wire_transaction(tx)))
    assert [t.key for t in node.mem_pool] == [tx.key]


# -------------------------------------------------------------------- contracts

CONTRACT = """
def add(a, state):
    state['total'] = state.get('total', 0) + a
    return state, 'ok'

def spin(a, state):
    while True:
        a = a + 1
    return state, 'never'

def escape(a, state):
    return getattr(a, '__class__'), 'x'

def fact(a, state):
    return factorial(a), 'x'

def write(a, state):
    a.attr = 1
    return state, 'x'
"""


def pow_node_with_contract(run_contract=None):
    a, b = Wallet(), Wallet()
    node = PowPeer("127.0.0.1", free_port(), "n", False, "n", "n", difficulty=1)
    node.wallet = a
    node.chain = pow_chain(a)
    deploy = Transaction([CONTRACT, deploy_fee(CONTRACT)], a.public_key_pem, "deploy")
    deploy.sign_with(a.private_key)
    blk = pow_block(node.chain, a, [deploy])
    node.chain.validate_block(blk)
    node.chain.chain.append(blk)
    node.apply_block_side_effects(blk)
    cid = calculate_contract_id(a.public_key_pem, deploy.ts)
    if run_contract is not None:
        node.run_contract_sync = run_contract
    return node, a, b, cid


def test_contract_validation_order_signature_and_balance_before_execution():  # CB-11 / previous item 12
    calls = []

    def recording_runner(code, func, args, state):
        calls.append(func)
        return {"error": None, "state": {"total": 1}, "gas_used": 10}

    node, a, b, cid = pow_node_with_contract(recording_runner)
    mallory = Wallet()
    unsigned = Transaction([cid, "add", [1], {"total": 1}, 0.01], a.public_key_pem, "invoke")
    unsigned.sign = mallory.private_key.sign(str(unsigned).encode())
    with pytest.raises(ValidationError, match="signature"):
        run(node.process_new_tx_message(make_wire_transaction(unsigned)))
    poor = Transaction([cid, "add", [1], {"total": 1}, 500.0], a.public_key_pem, "invoke")
    poor.sign_with(a.private_key)
    with pytest.raises(ValidationError, match="balance"):
        run(node.process_new_tx_message(make_wire_transaction(poor)))
    malformed = {"type": "new_tx", "id": "m", "transaction": json.dumps({"payload": [cid], "sender": a.public_key_pem,
                                                                          "receiver": "invoke", "id": "1", "ts": 1}),
                 "sign": base64.b64encode(b"x").decode()}
    with pytest.raises(ValidationError):
        run(node.process_new_tx_message(malformed))
    assert calls == []  # the contract never ran
    ok = Transaction([cid, "add", [1], {"total": 1}, 10 * GAS_PRICE], a.public_key_pem, "invoke")
    ok.sign_with(a.private_key)
    run(node.process_new_tx_message(make_wire_transaction(ok)))
    assert calls == ["add"]


def test_contract_execution_runs_off_the_event_loop():  # previous item 13
    def slow_runner(code, func, args, state):
        time.sleep(1.0)
        return {"error": None, "state": {"total": 1}, "gas_used": 10}

    node, a, b, cid = pow_node_with_contract(slow_runner)
    tx = Transaction([cid, "add", [1], {"total": 1}, 10 * GAS_PRICE], a.public_key_pem, "invoke")
    tx.sign_with(a.private_key)
    ticks = []

    async def ticker():
        while True:
            ticks.append(time.monotonic())
            await asyncio.sleep(0.05)

    async def scenario():
        t = asyncio.create_task(ticker())
        await node.process_new_tx_message(make_wire_transaction(tx))
        t.cancel()
    run(scenario())
    assert len(ticks) >= 10  # the loop kept running while the contract executed


def test_real_contract_invoke_end_to_end_and_unknown_contract():  # previous item 15
    node, a, b, cid = pow_node_with_contract()
    tx = run(node.submit_invoke(cid, "add", [5]))
    assert tx.payload[3] == {"total": 5}
    with pytest.raises(ValidationError, match="unknown contract|no such contract"):
        run(node.submit_invoke("f" * 64, "add", [5]))
    bogus = Transaction(["f" * 64, "add", [1], {}, 0.01], a.public_key_pem, "invoke")
    bogus.sign_with(a.private_key)
    with pytest.raises(ValidationError, match="unknown contract"):
        run(node.process_new_tx_message(make_wire_transaction(bogus)))


def test_runaway_contract_is_killed_and_reaped():  # previous item 14 / SV-13
    from smart_contract.secure_executor import SecureContractExecutor
    before = {p.pid for p in psutil.Process().children(recursive=True)}
    start = time.monotonic()
    result = SecureContractExecutor("def spin(a, state):\n    x = 10 ** 10 ** 9\n    return state, 'x'\n",
                                    timeout=2).run("spin", [1], {})
    assert result["success"] is False
    assert time.monotonic() - start < 15
    leftover = [p for p in psutil.Process().children(recursive=True) if p.pid not in before]
    assert [p for p in leftover if p.status() == psutil.STATUS_ZOMBIE] == []
    assert leftover == []


def test_sandbox_guards():  # SV-08 / SV-09 / SV-11
    from smart_contract.secure_executor import SecureContractExecutor
    ex = SecureContractExecutor(CONTRACT.replace("def escape(a, state):\n    return getattr(a, '__class__'), 'x'\n", ""))
    assert ex.run("add", [2], {})["state"] == {"total": 2}
    assert "gas" in ex.run("spin", [1], {})["error"].lower()
    assert "factorial" in ex.run("fact", [10], {})["error"]
    assert ex.run("write", [[1]], {})["success"] is False
    assert SecureContractExecutor("def f(a, state):\n    return a.__class__, 'x'\n").run("f", [1], {})["success"] is False
    assert "getattr" in SecureContractExecutor(CONTRACT).run("escape", [1], {})["error"]


def test_contracts_rebuilt_after_chain_sync_and_immutable():  # previous item 21 / SV-14 / SV-15
    from smart_contract.contracts_db import SmartContractDatabase, ContractAlreadyDeployed
    source, a, b, cid = pow_node_with_contract()
    node = PowPeer("127.0.0.1", free_port(), "n2", False, "n", "n", difficulty=1)
    run(node.process_raw(FakeWebSocket(), json.dumps({"type": "chain", "id": "c", "chain": source.chain.to_block_dict_list()})))
    assert node.contractsDB.get_contract(cid) == CONTRACT
    db = SmartContractDatabase()
    db.store_contract("x", "code")
    with pytest.raises(ContractAlreadyDeployed):
        db.store_contract("x", "other code")


# ------------------------------------------------------------- node / messaging

def test_malformed_messages_do_not_crash_handlers():  # CB-13
    node = PowPeer("127.0.0.1", free_port(), "n", False, "n", "n", difficulty=1)
    node.chain = pow_chain(node.wallet)
    ws = FakeWebSocket()

    async def scenario():
        for raw in ["not json", "[1,2]", "null", json.dumps({"type": 5, "id": "x"}),
                    json.dumps({"type": "new_tx", "id": "1"}),
                    json.dumps({"type": "new_block", "id": "2", "block": {"id": 1}}),
                    json.dumps({"type": "chain", "id": "3", "chain": [{"junk": True}]}),
                    json.dumps({"type": "peer_info", "id": "4", "data": {"host": "x"}}),
                    json.dumps({"type": "known_peers", "id": "5", "peers": [1, "a", {}]}),
                    json.dumps({"type": "file", "id": "6", "cid": "-rf", "desc": "x"})]:
            await node.process_raw(ws, raw)
        await node.process_raw(ws, json.dumps({"type": "ping", "id": "7"}))
    run(scenario())
    assert ws.of_type("pong")  # still processing messages afterwards
    assert node.file_hashes == {}


def test_change_name_only_from_bootstrap_connection():  # SV-03
    node = PowPeer("127.0.0.1", free_port(), "alice", False, "n", "n", difficulty=1)
    stranger, bootstrap = FakeWebSocket(), FakeWebSocket()
    node.pending_add_peer_ws = bootstrap
    run(node.handle_messages(stranger, {"type": "change_name", "id": "c1", "new_name": "evil", "new_peer_msg_id": "z"}))
    assert node.name == "alice"
    run(node.handle_messages(bootstrap, {"type": "change_name", "id": "c2", "new_name": "alice1", "new_peer_msg_id": "z"}))
    assert node.name == "alice1"
    run(node.handle_messages(bootstrap, {"type": "change_name", "id": "c3", "new_name": "again", "new_peer_msg_id": "z"}))
    assert node.name == "alice1"


def test_peer_records_must_be_signed_by_announced_key():  # SV-04 / SV-19
    node = PowPeer("127.0.0.1", free_port(), "alice", False, "n", "n", difficulty=1)
    victim, attacker = Wallet(), Wallet()
    record = {"host": "127.0.0.1", "port": 12345, "name": "victim", "public_key": victim.public_key_pem, "node_id": "v"}
    forged = sign_peer_record(record, attacker)
    with pytest.raises(ValidationError):
        verify_peer_record(forged)
    ws = FakeWebSocket()
    run(node.process_raw(ws, json.dumps({"type": "peer_info", "id": "p1", "data": forged})))
    assert node.known_peers == {}
    genuine = sign_peer_record(record, victim)
    run(node.process_raw(ws, json.dumps({"type": "peer_info", "id": "p2", "data": genuine})))
    assert list(node.name_to_public_key_dict.values()) == [victim.public_key_pem]
    me = node.peer_record()
    run(node.process_raw(ws, json.dumps({"type": "new_peer", "id": "p3", "data": me})))
    assert all(r["node_id"] != node.node_id for r in node.known_peers.values())  # never adds itself


def test_seen_message_ids_are_bounded():  # SV-18
    cache = SeenMessageCache(max_size=100, ttl=1000)
    for i in range(1000):
        cache.add(str(i))
    assert len(cache) == 100 and "999" in cache and "0" not in cache
    short = SeenMessageCache(max_size=100, ttl=0.01)
    short.add("x")
    time.sleep(0.02)
    assert "x" not in short


def test_chain_request_rate_limited():  # SV-17
    node = PowPeer("127.0.0.1", free_port(), "n", False, "n", "n", difficulty=1)
    node.chain = pow_chain(node.wallet)
    ws = FakeWebSocket()
    run(node.handle_messages(ws, {"type": "chain_request", "id": "r1"}))
    run(node.handle_messages(ws, {"type": "chain_request", "id": "r2"}))
    assert len(ws.of_type("chain")) == 1
    other = FakeWebSocket(("127.0.0.1", 40001))
    run(node.handle_messages(other, {"type": "chain_request", "id": "r3"}))
    assert len(other.of_type("chain")) == 1


def test_bad_mempool_transaction_does_not_stall_block_production():  # previous item 20
    node = PowPeer("127.0.0.1", free_port(), "n", True, "n", "n", difficulty=1)
    b = Wallet()
    node.chain = pow_chain(node.wallet)
    overspend = signed_tx(node.wallet, b.public_key_pem, 45)
    also = signed_tx(node.wallet, b.public_key_pem, 45)  # together they overspend
    forged = signed_tx(node.wallet, b.public_key_pem, 1)
    forged.sign = b.private_key.sign(str(forged).encode())
    node.mem_pool = [forged, overspend, also]
    block = run(node.build_block())
    assert block is not None and [t.key for t in block.transactions] == [overspend.key]
    node.chain.validate_block(block)
    assert [t.key for t in node.mem_pool] == [overspend.key]


def test_mempool_fully_pruned_after_block():  # CB-07
    node = PowPeer("127.0.0.1", free_port(), "n", True, "n", "n", difficulty=1)
    b = Wallet()
    node.chain = pow_chain(node.wallet)
    txs = [signed_tx(node.wallet, b.public_key_pem, 1) for _ in range(5)]
    node.mem_pool = list(txs)
    block = pow_block(node.chain, node.wallet, txs)
    node.chain.chain.append(block)
    node.apply_block_side_effects(block)
    assert node.mem_pool == []


def test_pow_malicious_node_sends_two_different_blocks():  # CB-06
    mal = PowMalPeer("127.0.0.1", free_port(), "mal", True, "n", "n", difficulty=1)
    mal.chain = pow_chain(mal.wallet)
    x, y = Wallet(), Wallet()
    mal.name_to_public_key_dict = {"x": x.public_key_pem, "y": y.public_key_pem}
    blocks = mal.make_double_spend_blocks()
    for blk in blocks:
        mal.chain.mine(blk)
    ws1, ws2 = FakeWebSocket(), FakeWebSocket(("127.0.0.1", 40002))
    mal.server_connections = {ws1, ws2}
    run(mal.send_split(mal.new_msg("new_block", block=blocks[0].to_dict()),
                       mal.new_msg("new_block", block=blocks[1].to_dict())))
    got = {ws1.sent[0]["block"]["id"], ws2.sent[0]["block"]["id"]}
    assert got == {blocks[0].id, blocks[1].id}
    # an honest node that received block 1 rejects the conflicting spend of block 2 on top of it
    honest = pow_chain(mal.wallet)
    honest.chain = list(mal.chain.chain[:1]) + [blocks[0]]
    with pytest.raises(ValidationError):
        honest.validate_block(blocks[1])


# ------------------------------------------------------------ persistence etc.

def test_restart_with_saved_peers_and_chain(tmp_path, monkeypatch):  # CB-03 / previous item 17
    from storage import storage_manager
    monkeypatch.setattr(storage_manager, "BASE_STORAGE_DIR", str(tmp_path))
    port = free_port()
    node = PowPeer("127.0.0.1", port, "alice", False, "n", "y", difficulty=1, data_profile="alice")
    node.create_genesis()
    friend = Wallet()
    record = sign_peer_record({"host": "127.0.0.1", "port": 23456, "name": "Friend",
                               "public_key": friend.public_key_pem, "node_id": "f"}, friend)
    node.register_peer(record)
    tx = Transaction([CONTRACT, deploy_fee(CONTRACT)], node.wallet.public_key_pem, "deploy")
    tx.sign_with(node.wallet.private_key)
    blk = pow_block(node.chain, node.wallet, [tx])
    node.chain.chain.append(blk)
    node.apply_block_side_effects(blk)
    node.persist_chain()

    restarted = PowPeer("127.0.0.1", port, "alice", False, "y", "y", difficulty=1, data_profile="alice")
    assert restarted.wallet.public_key_pem == node.wallet.public_key_pem
    assert restarted.node_id == node.node_id
    assert restarted.name_to_public_key_dict["friend"] == friend.public_key_pem
    assert [b.hash for b in restarted.chain.chain] == [b.hash for b in node.chain.chain]
    assert restarted.contractsDB.get_contract(calculate_contract_id(tx.sender, tx.ts)) == CONTRACT


def test_private_key_file_permissions_and_encryption(tmp_path, monkeypatch):  # SV-01
    from storage import storage_manager
    monkeypatch.setattr(storage_manager, "BASE_STORAGE_DIR", str(tmp_path))
    w = Wallet()
    storage_manager.save_key(w.private_key_pem, "pos", "k")
    path = os.path.join(storage_manager.get_consensus_dir("pos", "k"), "keys.json")
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    monkeypatch.setenv(storage_manager.KEY_PASSPHRASE_ENV, "s3cret")
    storage_manager.save_key(w.private_key_pem, "pos", "k")
    assert w.private_key_pem not in open(path).read()
    assert Wallet(storage_manager.load_key("pos", "k")).public_key_pem == w.public_key_pem
    monkeypatch.delenv(storage_manager.KEY_PASSPHRASE_ENV)
    with pytest.raises(ValueError):
        storage_manager.load_key("pos", "k")


@pytest.mark.parametrize("consensus,profile", [("../../etc", None), ("pos/../..", None), ("pos", "../x"), ("pos", "a/b")])
def test_storage_path_traversal_rejected(consensus, profile):  # SV-02
    from storage import storage_manager
    with pytest.raises(ValueError):
        storage_manager.get_consensus_dir(consensus, profile)


def test_ipfs_path_and_cid_validation(tmp_path):  # SV-05 / SV-06 / SV-07
    from ipfs import ipfs
    assert ipfs.is_valid_cid("Qm" + "a" * 44)
    for bad in ["-v", "--help", "Qm; rm -rf /", "", None]:
        assert not ipfs.is_valid_cid(bad)
        with pytest.raises(ValueError):
            ipfs.download_ipfs_file_subprocess(bad, "file.txt")
    with pytest.raises(ValueError):
        ipfs.resolve_download_path("../../../etc/cron.d/evil")
    with pytest.raises(ValueError):
        ipfs.resolve_upload_path(os.path.join(os.path.dirname(ipfs.__file__), "..", "storage", "pos", "keys.json"))
    assert ipfs.addToIpfs("/etc/passwd") == (None, None)


def test_upload_without_ipfs_returns_none(monkeypatch):  # CB-14
    node = PowPeer("127.0.0.1", free_port(), "n", False, "n", "n", difficulty=1)
    monkeypatch.setattr(node, "ipfs_available", lambda: False)
    assert run(node.uploadFile("desc", "/etc/passwd")) is None
