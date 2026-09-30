"""
    Phase 4: malicious nodes against honest nodes over real P2P websockets (discovered via a
    real signalling server). The malicious behaviour must not get past honest validation.
"""
import asyncio, json
import pytest

from conftest import run, wait_for, free_port, FakeWebSocket
from shared_blockchain_structures import Transaction, ValidationError
from signalling.server import SignallingServer
from consensus.pow.p2p import Peer as PowPeer
from consensus.pow.mal_node import Peer as PowMal
from consensus.pow.blockchain_structures import validate_chain as pow_validate_chain
from consensus.pos.p2p import Peer as PosPeer
from consensus.pos.mal_node import Peer as PosMal
from consensus.pos.blockchain_structures import Stake
from consensus.poa.p2p import Peer as PoaPeer
from consensus.poa.mal_node import Peer as PoaMal


async def network(node_specs):
    """node_specs: list of (factory, name, create_room). Returns (server, nodes)."""
    server = await SignallingServer("127.0.0.1", 0).start()
    url = f"ws://127.0.0.1:{server.port}"
    nodes = []
    for factory, name, create in node_specs:
        node = factory(name)
        await node.start_network(signalling_url=url, create_room="mal" if create else None,
                                 join_room=None if create else "mal")
        nodes.append(node)
    ok = await wait_for(lambda: all(n.chain is not None and len(n.connected_endpoints()) == len(nodes) - 1
                                    for n in nodes), 20)
    assert ok, "network did not form"
    return server, nodes


async def shutdown(server, nodes):
    for n in nodes:
        await n.stop()
    await server.stop()


def test_pow_malicious_invalid_transactions_rejected():
    async def scenario():
        server, (honest, mal) = await network([
            (lambda n: PowPeer("127.0.0.1", free_port(), n, False, "n", "n", difficulty=1), "honest", True),
            (lambda n: PowMal("127.0.0.1", free_port(), n, True, "n", "n", difficulty=1, block_interval=3600), "mal", False),
        ])
        try:
            victim = honest.wallet.public_key_pem
            overspend = await mal.create_and_broadcast_tx(victim, 1000)   # mal has no coins
            negative = await mal.create_and_broadcast_tx(victim, -5)
            await wait_for(lambda: len(honest.recent_rejections) >= 2, 10)
            assert honest.mem_pool == []
            reasons = " ".join(r["reason"] for r in honest.recent_rejections)
            assert "balance" in reasons and "positive" in reasons
        finally:
            await shutdown(server, [honest, mal])
    run(scenario())


def test_pow_malicious_double_spend_fork_never_lands_both_spends():
    async def scenario():
        mk_honest = lambda n: PowPeer("127.0.0.1", free_port(), n, False, "n", "n", difficulty=2)
        server, (mal, h1, h2) = await network([
            (lambda n: PowMal("127.0.0.1", free_port(), n, True, "n", "n", difficulty=2, block_interval=0.5), "mal", True),
            (mk_honest, "h1", False), (mk_honest, "h2", False),
        ])
        try:
            assert await wait_for(lambda: len(h1.chain.chain) >= 2 and len(h2.chain.chain) >= 2, 30)
            for honest in (h1, h2):
                chain = honest.chain.chain
                pow_validate_chain(chain, 2)  # never contains both conflicting spends (balance would go negative)
                spent = sum(tx.amount for b in chain for tx in b.transactions if tx.sender == mal.wallet.public_key_pem)
                assert spent <= 56
        finally:
            await shutdown(server, [mal, h1, h2])
    run(scenario())


def test_pos_malicious_double_sign_is_slashed_by_honest_nodes():
    async def scenario():
        mk_honest = lambda n: PosPeer("127.0.0.1", free_port(), n, True, "n", "n", epoch_time=2)
        server, (mal, h1, h2) = await network([
            (lambda n: PosMal("127.0.0.1", free_port(), n, True, "n", "n", epoch_time=2), "mal", True),
            (mk_honest, "h1", False), (mk_honest, "h2", False),
        ])
        try:
            while True:
                try:
                    await mal.stake(20)
                    break
                except ValidationError:
                    await asyncio.sleep(0.2)
            ok = await wait_for(lambda: h1.slashed_block_hashes and h2.slashed_block_hashes, 20)
            assert ok, (h1.recent_rejections, h2.recent_rejections)
            for honest in (h1, h2):
                slashed = [b for b in honest.chain.chain if not b.is_valid]
                assert slashed and all(b.creator == mal.wallet.public_key_pem and b.slash_creator for b in slashed)
                # the double signer loses its stake on every honest node
                assert honest.spendable_balance(mal.wallet.public_key_pem) < 56
        finally:
            await shutdown(server, [mal, h1, h2])
    run(scenario())


def test_pos_malicious_block_omitting_stakes_rejected():
    from test_phase1 import funded_pos_chain, pos_chain_copy, make_stake, signed_tx, EPOCH
    from consensus.pos import blockchain_structures as posbs
    chain, mal_wallet, honest_wallet = funded_pos_chain()
    mal = PosMal("127.0.0.1", free_port(), "mal", True, "n", "n", epoch_time=EPOCH, attack="omit_stakes")
    honest = PosPeer("127.0.0.1", free_port(), "honest", True, "n", "n", epoch_time=EPOCH)
    mal.wallet, honest.wallet = mal_wallet, honest_wallet
    mal.chain, honest.chain = pos_chain_copy(chain), pos_chain_copy(chain)
    tip = chain.lastBlock.hash
    mal_stake, honest_stake = make_stake(mal_wallet, 1, tip), make_stake(honest_wallet, 25, tip)
    honest.current_stakes = {mal_wallet.public_key_pem: mal_stake, honest_wallet.public_key_pem: honest_stake}
    now = posbs.now_ms()
    # a round in which the attacker would lose with the real total stake (26) but wins alone (1)
    ts = next(t for t in range(now - 1500, now + 9000, 500)
              if not posbs.wins_lottery(posbs.compute_seed(chain.chain, 2, t, EPOCH), mal_wallet.public_key_pem, 1, 26))
    seed = posbs.compute_seed(chain.chain, 2, ts, EPOCH)
    mal.staked_amt = 1
    block = mal.build_candidate_block([signed_tx(mal_wallet, honest_wallet.public_key_pem, 1)], [mal_stake], ts, seed)
    run(honest.process_raw(FakeWebSocket(), json.dumps({"type": "new_block", "id": "omit",
                                                        "block": block.to_dict_with_stakers()})))
    assert len(honest.chain.chain) == 2
    assert any("omits stakes" in r["reason"] for r in honest.recent_rejections)
    # including the honest stake doesn't help the attacker: then it simply lost the lottery
    full = mal.build_candidate_block([signed_tx(mal_wallet, honest_wallet.public_key_pem, 1)],
                                     [mal_stake, honest_stake], ts, seed)
    run(honest.process_raw(FakeWebSocket(), json.dumps({"type": "new_block", "id": "full",
                                                        "block": full.to_dict_with_stakers()})))
    assert len(honest.chain.chain) == 2
    assert any("lottery" in r["reason"] for r in honest.recent_rejections)


def test_poa_malicious_node_cannot_mine_or_change_authorities():
    async def scenario():
        server, (admin, mal) = await network([
            (lambda n: PoaPeer("127.0.0.1", free_port(), n, "n", "n", round_time=3, block_interval=3600), "admin", True),
            (lambda n: PoaMal("127.0.0.1", free_port(), n, "n", "n", round_time=3, block_interval=0.3), "mal", False),
        ])
        try:
            await mal.change_miners("mal", True)          # forged authority update
            await mal.create_and_broadcast_tx(admin.wallet.public_key_pem, 500)  # overspend
            await asyncio.sleep(2)                        # mal keeps sending unscheduled blocks meanwhile
            assert len(admin.chain.chain) == 1
            assert admin.pending_updates == []
            assert admin.mem_pool == []
            reasons = " ".join(r["reason"] for r in admin.recent_rejections)
            assert "admin" in reasons and "balance" in reasons
            assert "scheduled authority" in reasons or "does not extend" in reasons
        finally:
            await shutdown(server, [admin, mal])
    run(scenario())
