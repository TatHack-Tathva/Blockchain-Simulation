"""Legacy entry points kept working: bootstrap-node mode and the interactive prompts."""
import subprocess, sys

from conftest import run, wait_for, free_port, ROOT
from consensus.pos.p2p import Peer as PosPeer


def test_bootstrap_mode_with_duplicate_name_is_renamed_by_bootstrap_only():
    async def scenario():
        a = PosPeer("127.0.0.1", free_port(), "alice", True, "n", "n", epoch_time=2)
        b = PosPeer("127.0.0.1", free_port(), "alice", True, "n", "n", epoch_time=2)
        await a.start_network()                      # creates the network (genesis)
        await b.start_network("127.0.0.1", a.port)   # joins through the bootstrap node
        try:
            assert await wait_for(lambda: b.chain is not None and b.name == "alice1"
                                  and "alice1" in a.name_to_public_key_dict, 20)
            assert b.chain.genesis_hash == a.chain.genesis_hash
            assert a.name_to_public_key_dict["alice1"] == b.wallet.public_key_pem
            tx = await a.submit_payment("alice1", 5)
            assert await wait_for(lambda: any(t.key == tx.key for t in b.mem_pool), 10)
        finally:
            await a.stop(); await b.stop()
    run(scenario())


def test_interactive_prompt_mode_starts_and_quits():
    answers = "\n".join(["127.0.0.1", str(free_port()), "alice", "pos", "n", "n", "create", "n", "y",
                         "2", "7", "0"]) + "\n"
    out = subprocess.run([sys.executable, "start_peer.py"], input=answers, capture_output=True, text=True,
                         cwd=ROOT, timeout=60)
    assert out.returncode == 0, out.stderr
    assert "Account Balance = 56" in out.stdout and "Quitting..." in out.stdout
