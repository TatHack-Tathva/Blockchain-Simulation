"""
    Start a node.

    Without arguments the original interactive prompts are used. With arguments the node
    can run non-interactively, find peers through the signalling server (no bootstrap
    node needed) and serve the web dashboard, e.g.

        python start_peer.py --consensus pos --host 127.0.0.1 --port 6001 --name alice \\
            --signalling ws://127.0.0.1:8765 --create-room demo --web-port 7001 --headless
"""
import argparse, asyncio, logging, signal, sys

from consensus.poa.p2p import Peer as PoAPeer
from consensus.pos.p2p import Peer as PoSPeer
from consensus.pow.p2p import Peer as PoWPeer
from consensus.poa.mal_node import Peer as PoaMalPeer
from consensus.pos.mal_node import Peer as PosMalPeer
from consensus.pow.mal_node import Peer as PowMalPeer

CONSENSUS_TYPES = ("pow", "pos", "poa")


def build_peer(consensus, host, port, name, *, malicious=False, staker=True, miner=True, load="n", save="n",
               data_profile=None, epoch_time=None, difficulty=None, block_interval=None, round_time=None):
    kwargs = {"data_profile": data_profile}
    if consensus == "poa":
        if round_time is not None:
            kwargs["round_time"] = round_time
        if block_interval is not None:
            kwargs["block_interval"] = block_interval
        cls = PoaMalPeer if malicious else PoAPeer
        return cls(host, port, name, load, save, **kwargs)
    if consensus == "pos":
        if epoch_time is not None:
            kwargs["epoch_time"] = epoch_time
        if malicious:
            return PosMalPeer(host, port, name, True, load, save, **kwargs)
        return PoSPeer(host, port, name, staker, load, save, **kwargs)
    if difficulty is not None:
        kwargs["difficulty"] = difficulty
    if block_interval is not None:
        kwargs["block_interval"] = block_interval
    if malicious:
        return PowMalPeer(host, port, name, True, load, save, **kwargs)
    return PoWPeer(host, port, name, miner, load, save, **kwargs)


def yes(prompt, default=False):
    raw = input(prompt).strip().lower()
    return default if raw == "" else raw == "y"


def start_peer_interactive():
    host = input("Enter Host: ").strip() or "127.0.0.1"
    port = int(input("Enter Port: "))
    name = input("Enter Name: ").strip() or f"node{port}"
    consensus = input("Enter Consensus[poa/pos/pow] (default : pow): ").strip().lower()
    if consensus not in CONSENSUS_TYPES:
        consensus = "pow"
    activate_disk_load = "y" if yes("Do you like to load saved data if any(y/n): ") else "n"
    activate_disk_save = "y" if yes("Do you like to continuously backup data to disk(y/n): ") else "n"
    action = input("Enter 'create' to create a network and 'connect' to connect to a network (default: create): ").strip()
    bootstrap_host = bootstrap_port = None
    if action == "connect":
        bootstrap_host = input("Enter host to connect: ").strip()
        bootstrap_port = int(input("Enter port to connect: "))
    malicious = yes("Malcious? (y/n) ")
    staker = miner = True
    if consensus == "pos" and not malicious:
        staker = yes("Staker? (y/n) ", True)
    if consensus == "pow" and not malicious:
        miner = yes("Miner? (y/n) ", True)
    peer = build_peer(consensus, host, port, name, malicious=malicious, staker=staker, miner=miner,
                      load=activate_disk_load, save=activate_disk_save)
    try:
        asyncio.run(peer.start(bootstrap_host, bootstrap_port, interactive=True,
                               create_network=(action != "connect")))
    except KeyboardInterrupt:
        print("\nShutting Down...")


def parse_args(argv):
    p = argparse.ArgumentParser(description="Blockchain simulation node")
    p.add_argument("--consensus", choices=CONSENSUS_TYPES, default="pow")
    p.add_argument("--host", default="127.0.0.1", help="P2P listen address (also announced to peers)")
    p.add_argument("--port", type=int, required=True, help="P2P websocket port")
    p.add_argument("--name", required=True)
    p.add_argument("--signalling", metavar="URL", help="signalling server, e.g. ws://127.0.0.1:8765")
    room = p.add_mutually_exclusive_group()
    room.add_argument("--create-room", metavar="ROOM", help="create a room (this node creates the genesis block)")
    room.add_argument("--join-room", metavar="ROOM", help="join an existing room")
    p.add_argument("--bootstrap", metavar="HOST:PORT", help="legacy mode: connect to a known node instead of signalling")
    p.add_argument("--create-network", action="store_true", help="legacy mode: start a new chain without signalling")
    p.add_argument("--malicious", action="store_true")
    p.add_argument("--no-staker", dest="staker", action="store_false", help="PoS: do not stake")
    p.add_argument("--no-miner", dest="miner", action="store_false", help="PoW: do not mine")
    p.add_argument("--load", action="store_true", help="load saved key, chain and peers")
    p.add_argument("--save", action="store_true", help="persist key, chain and peers")
    p.add_argument("--data-dir", metavar="PROFILE", help="storage sub directory (storage/<consensus>/<PROFILE>)")
    p.add_argument("--web-port", type=int, help="serve the web dashboard / API on this port")
    p.add_argument("--web-host", default="127.0.0.1")
    p.add_argument("--headless", action="store_true", help="no interactive menu")
    p.add_argument("--epoch-time", type=float, help="PoS epoch length in seconds (default 60)")
    p.add_argument("--difficulty", type=int, help="PoW difficulty in hex zeros (default 5)")
    p.add_argument("--block-interval", type=float, help="PoW/PoA seconds between block attempts (default 30)")
    p.add_argument("--round-time", type=float, help="PoA seconds per miner slot (default 90)")
    p.add_argument("--discovery-interval", type=float, help="seconds between peer discovery rounds (default 30)")
    p.add_argument("--sync-interval", type=float, help="seconds between chain sync requests (default 60)")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    if (args.create_room or args.join_room) and not args.signalling:
        p.error("--create-room/--join-room need --signalling")
    return args


async def run_node(args):
    peer = build_peer(args.consensus, args.host, args.port, args.name, malicious=args.malicious,
                      staker=args.staker, miner=args.miner, load="y" if args.load else "n",
                      save="y" if args.save else "n", data_profile=args.data_dir, epoch_time=args.epoch_time,
                      difficulty=args.difficulty, block_interval=args.block_interval, round_time=args.round_time)
    if args.discovery_interval:
        peer.discovery_interval = args.discovery_interval
    if args.sync_interval:
        peer.sync_interval = args.sync_interval
    bootstrap_host = bootstrap_port = None
    if args.bootstrap:
        bootstrap_host, _, port = args.bootstrap.rpartition(":")
        bootstrap_port = int(port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    await peer.start_network(bootstrap_host, bootstrap_port, signalling_url=args.signalling,
                             create_room=args.create_room, join_room=args.join_room,
                             create_network=args.create_network or not (args.signalling or args.bootstrap))
    if args.web_port is not None:
        from web.app import WebServer
        peer.web_server = WebServer(peer, args.web_host, args.web_port).start()
    logging.getLogger("node").info("[%s] node ready: %s p2p=%s:%s node_id=%s", peer.name, peer.CONSENSUS,
                                   peer.host, peer.port, peer.node_id)
    try:
        if args.headless:
            await stop.wait()
        else:
            menu = asyncio.ensure_future(peer.user_input_handler())
            waiter = asyncio.ensure_future(stop.wait())
            await asyncio.wait({menu, waiter}, return_when=asyncio.FIRST_COMPLETED)
            for t in (menu, waiter):
                t.cancel()
    finally:
        await peer.stop()


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        return start_peer_interactive()
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    try:
        asyncio.run(run_node(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
