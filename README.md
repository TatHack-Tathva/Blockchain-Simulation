# Blockchain App
A web and terminal blockchain implementation in Python from scratch

## Features
- Peer-to-Peer network with decentralized communication
- Public/private key-based account system
- Digital signature verification
- Selectable consensus mechanism - PoW, PoS, PoA
- Smart contract deployment
- IPFS integration
- Persistent storage
- Malicious node to test security
- Command line & web interface
- Room based signalling server for peer discovery (no bootstrap node needed)

## Contents
- [Theory](#theory)
- [About this project](#about-this-project)
- [How to run this project](#how-to-run-this-project)

## Theory
### What is blockchain?
A blockchain is a decentralized, distributed digital ledger where data is stored in blocks linked together in a chain
- A block is made of list of transactions
### Peer-to-Peer Network
Since, there is no central authority, network is formed in a peer-to-peer fashion.
### Consensus Mechanism
Blockchain involves transactions in a trustless environment. So there is need for a mechanism to ensure integrity of the chain. There comes the need of consensus mechanisms. Each consensus mechanism ensures integrity of the chain in their own way.
#### Proof of Work(PoW)
- Nodes compete to solve a cryptographic puzzle
- The winner gets to add the next block to the chain
#### Proof of Stake(PoS)
- Nodes run vrf to generate vrf output and vrf proof to simulate a lottery system
- The winner of the lottery gets to generate the block.
#### Proof of Authority(PoA)
- A limited set of trusted nodes(authorities) validate and create new blocks
### Smart Contracts
- A smart contract is like a digital agreement written in code
- It sits on the blockchain and runs automatically when certain rules are met
### IPFS
Blockchains are not designed for storing large amount of data. That's where IPFS comes in.
- It's a decentralized file storage system
- Each file is identified by its content
- A unique hash called CID(Content Identifiers) is generated based on the content(Files with same content will have same CID)
- IPFS uses a Distributed Hash Table(DHT), similar to BitTorrent's Kademlia DHT
- When you request a CID, your node queries the DHT to ask "Which peers are providing this CID?"
- Nodes that have previously announced that CID to the DHT will be returned as providers
- Your node then directly connects to those providers via IPFS's peer-to-peer transport protocols(libp2p)

## About this project
### Basic Structure
Each **node** contains its own set of
- Known peers list (members of the network)
- Client connections (connection established by your node to other nodes)
- Server connections (connection established by other nodes to your node)
- Wallet (acts as your account in the network)
- Transaction pool (contains all transactions pending to be mined)
- Chain (personal copy of the blockchain)

Each **account** contains
- Private key
- Public key

**Transactions** are of 3 types

- **Coin Transaction** - To transfer money
  - Timestamp
  - Public key of the sender
  - Public key of the receiver
  - Transaction amount
- **Deploy Transaction** - To deploy contract
  - Timestamp
  - Public key of the sender
  - Contract code
  - Deploy charge

- **Invoke Transaction** - To invoke contract
  - Timestamp  
  - Public key of the sender  
  - Contract ID  
  - Function name  
  - Arguments  
  - New state  
  - Invoke charge  

Each **block** contains
- Timestamp
- List of transactions
- Hash of previous block
- Current block hash
- Miner info
- List of files
### Handshake Protocol
- Client: Sends ping
- Server: Receives ping &rightarrow; sends pong
- Client: Receives pong &rightarrow; Sends peer info (information about itself)
- Server: Receives peer info &rightarrow; adds it to its known peers (if not already present) &rightarrow; sends back known_peers (list of all nodes it knows)
- Client: Receives known_peers &rightarrow; adds new peers to its own known_peers &rightarrow; requests the chain
- Server: Receives chain request &rightarrow; sends its current chain
- Client: Receives the chain &rightarrow; replaces its own if the length of new chain is longer than the current one
### Peer-to-Peer Network
If the total number of nodes in the network is less than 10, it forms a mesh network. If the node count exceeds 9, Gossip-based Random Peer Sampling is used
- Each node maintains a list of 8 connected peers
- At regular intervals, a node drops one connection and connects to a new, previously unconnected peer from the known peers list
- This prevents network congestion by limiting the number of connections per node
- It also prevents sub-network formation by randomly switching connections  

**Implemented Using:** python websockets, asyncio
### Consensus Mechanism
Users can select their prefered consensus mechanism from the list of three available
#### Proof of Work(PoW)
- A new block is mined every 30 secs, if there are pending transactions in the transaction pool
- Mining nodes collect transactions into a block
- Node that first finds a valid hash gets the chance to mine
- Difficulty is set to 5. That means, a valid hash is the one which starts with five zeroes
- Nonce is incremented until finding a valid hash
- Once mined, the block is broadcasted to the network
- All nodes validate the block before adding it to their chain
#### Proof of Stake(PoS)
- One node is mined every epoch
- The timings are synchronized between peers based on the time since last block was created
- Each node can stake a certain amount of their cryptocurrency in order to run vrf.
- VRF is a verifiable random function. The stakers generate a vrf proof and vrf output
- If the vrf output generated by a node is less than (max value of vrf output) * (amount staked by this node/total amount staked by all nodes), then
- This node wins the lottery and create a block. (Notice that the greater the amount staked the greater the chance of winning the lottery)
- VRF Proof is used to check whether the one who claims to win the lottery actually generated the vrf output from the correct seed
- If node attempts double signing their stake is slashed.
- If multiple nodes win and creates blocks then the chain is forked.
- We use the heaviest chain rule to arrive at a consensus. (i.e the chain with the most amount staked is the valid chain)
#### Proof of Authority(PoA)
- Initially, admin, the one who started the chain is the only miner
- Admin can add or remove miners
- Each block can be mined only by the assigned miner
- If that miner is inactive, mining will be handed over to the next miner
### Smart Contracts
In this project
- Smart Contracts are written in python
- Users can write their own smart contracts and deploy
- Users can also invoke the deployed contract using their deployed address
- They run inside a sandboxed environment with time limit, memory limit and operation limit  

**Implemented Using:** RestrictedPython, multiprocessing
### IPFS
IPFS is integrated as a wrapper for the existing IPFS network. IPFS hashes of the user uploaded files are stored in the blocks. Other users can use this to download the file.  

**Implemented Using:** IPFS
### Persistent Storage
Persistent storage is implemented to enable nodes to reconnect to the network using there previous network data
### Malicious Node
- To test the security and robustness of our networks we created a malicious node that attempts
    1. Generate invalid transactions (amt>account balance or amount<=0)
    2. Double Sign
- We have tested our blockchain networks using this malicious nodes to verify that our protocols are working and that our network is functional

## How to run this project
### Prerequisites
- `python 3.10+`
- `pip` (python package manager)
- `venv` (for creating virtual environment)
- optional: the `ipfs` CLI (file sharing is disabled when it is not installed)

### Installation & Setup
```bash
git clone https://github.com/nithamanikandan0708/Blockchain-Simulation.git
cd Blockchain-Simulation
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt   # only needed to run the tests
```

### Architecture
```
signalling server (discovery only)      node process (start_peer.py)
  rooms / members / peer_joined   <-->    SignallingClient ── finds peers, auto reconnect + rejoin
  peer_left / small relay                  BasePeer (P2P websockets, direct node <-> node)
                                             └─ PoW / PoS / PoA Peer ── block creation + validation
                                           Flask web layer ──> node API ──> same validation path
```
- The signalling server never sees blocks or transactions and takes no part in consensus. It only
  holds room membership and each node's **signed** peer record (host, P2P port, name, public key,
  node id). Nodes re-verify those records and then connect to each other directly; the P2P
  connections keep working if the signalling server goes away.
- Rooms are bound to a consensus type and its network parameters (`epoch_time` for PoS,
  `difficulty` for PoW, `round_time` for PoA) and to the genesis block hash, so incompatible nodes
  are rejected when they try to join.
- The web layer only calls the node's thread-safe API. Transactions submitted from the browser go
  through exactly the same validation function as transactions received from peers.

### Default addresses used below
| What | Address |
|---|---|
| Signalling server | `ws://127.0.0.1:8765` |
| Room id | `demo` (1-64 chars: letters, digits, `_ . -`) |
| Node A (alice) | P2P `127.0.0.1:6001`, web `http://127.0.0.1:7001` |
| Node B (bob) | P2P `127.0.0.1:6002`, web `http://127.0.0.1:7002` |

### 1. Start the signalling server
```bash
python -m signalling.server --host 127.0.0.1 --port 8765
```

### 2. Create a room (node A) and 3. join it (node B) — two nodes, no bootstrap node
Each command runs in its own terminal. Neither node is given the other's address; they find each
other through the room and then connect directly.
```bash
# terminal 2 - node A creates room "demo" (and the genesis block)
python start_peer.py --consensus pos --host 127.0.0.1 --port 6001 --name alice \
    --signalling ws://127.0.0.1:8765 --create-room demo --web-port 7001 --epoch-time 10 --headless

# terminal 3 - node B joins room "demo" (gets the chain from alice over P2P)
python start_peer.py --consensus pos --host 127.0.0.1 --port 6002 --name bob \
    --signalling ws://127.0.0.1:8765 --join-room demo --web-port 7002 --epoch-time 10 --headless
```
Leave out `--headless` to also get the interactive terminal menu. Open `http://127.0.0.1:7001` and
`http://127.0.0.1:7002` to watch both nodes. Rooms can also be created / joined from the dashboard
(start the node with `--signalling ...` but without `--create-room/--join-room`).

### 4. Running a node - all options
```bash
python start_peer.py --help
```
| Option | Meaning |
|---|---|
| `--consensus pow\|pos\|poa` | consensus type (default `pow`) |
| `--host`, `--port`, `--name` | P2P listen address / port announced to peers, node name |
| `--signalling URL` + `--create-room ROOM` / `--join-room ROOM` | discovery through the signalling server |
| `--bootstrap HOST:PORT` / `--create-network` | legacy mode without signalling |
| `--web-port N` (`--web-host`, default 127.0.0.1) | serve dashboard + JSON API |
| `--malicious` | run the malicious node of the chosen consensus |
| `--no-staker` (PoS), `--no-miner` (PoW) | passive node |
| `--save`, `--load`, `--data-dir PROFILE` | persist / reload key, node id, chain, peers in `storage/<consensus>/<PROFILE>/` |
| `--epoch-time S` (PoS, default 60), `--difficulty N` (PoW, default 5), `--block-interval S` (PoW/PoA, default 30), `--round-time S` (PoA, default 90) | network parameters (must match inside a room) |
| `--headless` | no interactive menu |

`python start_peer.py` without arguments still asks the original interactive questions.

### 5. PoW
```bash
python start_peer.py --consensus pow --port 6001 --name alice --signalling ws://127.0.0.1:8765 \
    --create-room powdemo --web-port 7001 --difficulty 4 --block-interval 10 --headless
python start_peer.py --consensus pow --port 6002 --name bob --signalling ws://127.0.0.1:8765 \
    --join-room powdemo --web-port 7002 --difficulty 4 --block-interval 10 --headless
curl -X POST http://127.0.0.1:7001/api/transactions -H 'Content-Type: application/json' \
    -d '{"receiver": "bob", "amount": 10}'
```
Miners mine a block every `--block-interval` seconds when valid transactions are pending.

### 6. PoS
Start alice and bob as in step 2, then:
```bash
curl -X POST http://127.0.0.1:7001/api/transactions -H 'Content-Type: application/json' \
    -d '{"receiver": "bob", "amount": 10}'
curl -X POST http://127.0.0.1:7001/api/stake -H 'Content-Type: application/json' -d '{"amount": 20}'
```
A stake is only accepted during the first 5/6 of an epoch; outside it the API answers `409` with
`retry_after` seconds. At the end of the epoch the lottery runs; the winner creates the block and
every other node validates it (signature, seed, VRF proof, lottery, stakes, transactions).
The interactive menu offers the same (option `10) Stake`).

### 7. PoA
```bash
python start_peer.py --consensus poa --port 6001 --name admin --signalling ws://127.0.0.1:8765 \
    --create-room poademo --web-port 7001 --round-time 20 --block-interval 5 --headless
python start_peer.py --consensus poa --port 6002 --name bob --signalling ws://127.0.0.1:8765 \
    --join-room poademo --web-port 7002 --round-time 20 --block-interval 5
```
The room creator is the admin (derived from the genesis block). Adding / removing authorities is
done from the admin's interactive menu (`9) Add Miner`, `10) Remove Miner`); the admin signs the
change and it takes effect through a block that carries the signed update.

### 8. Web interface
Add `--web-port PORT` to any node and open `http://127.0.0.1:PORT`. The dashboard polls the node
every 2 s and shows node id / address / port / consensus / honest-or-malicious status, the
signalling room and connection state, the peer list, blocks (click one for its transactions),
recent transactions with their status and recently rejected data. It can create / join rooms,
submit coin transfers and stake (PoS). JSON API:

| Method | Path | |
|---|---|---|
| GET | `/api/node`, `/api/network`, `/api/blocks?limit=N`, `/api/blocks/<height>`, `/api/transactions?limit=N`, `/api/rooms` | read only snapshots |
| POST | `/api/transactions` `{"receiver": name-or-public-key, "amount": number}` | coin transfer (validated by the node) |
| POST | `/api/stake` `{"amount": int}` | PoS only |
| POST | `/api/rooms/create`, `/api/rooms/join` `{"room": id}` | signalling rooms |

The web layer never exposes private keys, files, contract deployment or any kind of code / shell
execution, and binds to 127.0.0.1 by default. Browsers disconnecting has no effect on the node.

### Enhanced Monitoring Dashboard
The web interface (`--web-port`) is a network monitoring console built from the node's existing JSON API
(vanilla JS, no build step, 2 s polling with a stale/offline indicator and automatic retry):
- **Network topology** - SVG view of this node, its known peers, direct P2P links (in/out) and signalling
  room membership, with honest / malicious (self-declared) status and consensus type.
- **Live network activity** - event console of the latest 20 changes observed between polls (transactions
  validated and confirmed, blocks created or received, stakes, peers joining / leaving, P2P links,
  signalling reconnects, rejected data).
- **Consensus / validator panel** - PoS epoch progress and stake registration window, validators and total
  stake; PoW difficulty and miner; PoA authorities, admin and round time.
- **Network statistics** - height, confirmed / pending / rejected transactions, peers, active nodes,
  average block interval and a transactions-per-block chart from the local chain.
- **Network security** - honest / malicious nodes, rejected data, invalid blocks and transactions,
  chain validation, genesis check against the room, slashed blocks.
- **Blockchain explorer** - expandable blocks (hash, previous hash, timestamp, validator / miner, stake,
  transactions) and a transaction list with CONFIRMED / PENDING / REJECTED status and copy buttons.

### 9. Tests
```bash
python -m pytest tests -q                              # everything (~40 s)
python -m pytest tests/test_phase1.py -q               # audit fixes (validation, PoS/PoW/PoA, contracts, storage)
python -m pytest tests/test_signalling.py -q           # signalling server + client on real sockets
python -m pytest tests/test_multinode.py -q            # REAL multi-process integration (see below)
python -m pytest tests/test_web.py -q                  # web interface
python -m pytest tests/test_malicious.py -q            # malicious nodes vs honest nodes
python -m pytest tests/test_legacy.py -q               # bootstrap mode + interactive prompts
```

### 10. Multi-node integration test
```bash
python -m pytest tests/test_multinode.py -q -s
```
It starts a real signalling server process and separate `start_peer.py` node processes (no
bootstrap peer), and checks through their HTTP APIs: discovery via the room, direct P2P
connections, transaction propagation, PoS staking / winning / block propagation and validation in
both directions, P2P working with the signalling server stopped, automatic reconnect + rejoin when
it returns, stale peer removal, PoW and PoA block propagation, and restart from disk.

### Storage and keys
With `--save` a node writes `storage/<consensus>/<profile>/{keys,node_id,chain,peers}.json`
(`BLOCKCHAIN_STORAGE_DIR` overrides the base directory). The key file is created with mode 0600;
set `BLOCKCHAIN_KEY_PASSPHRASE` to store it encrypted. Contracts are rebuilt from the chain on load
and after chain synchronisation. IPFS uploads are limited to `ipfs/uploads/`, downloads are written
inside `ipfs/retreived/`.

### Known limitations
- Slashing state (after double-sign evidence) is kept by each node and not written into blocks; a
  node that never received the evidence computes the offender's balance without the penalty.
- The PoS lottery draw is `sha256(seed : staker key)`: it can't be re-rolled, but it is public
  (not a real VRF) and the seed comes from block hashes that a block creator influences.
- PoS epochs and PoA miner slots use timestamps, so node clocks must be roughly synchronised
  (10 s tolerance).
- Peer records are signed by their key, but nothing proves that a node really owns the address
  it announces (the signalling server rejects address collisions inside a room).
- P2P and signalling websockets are not encrypted (no TLS).
- Amounts are floating point numbers.
- The gas meter counts executed Python lines; C-level work inside a contract is only bounded by
  the sandbox CPU / memory limits and timeout (rlimits are not available on Windows).

## Authors

**Rahan M**
[GitHub](https://github.com/Rahan-M) | [LinkedIn](https://www.linkedin.com/in/rahan-m-077a32254?utm_source=share&utm_campaign=share_via&utm_content=profile&utm_medium=android_app)

**Jefin Joji**
[GitHub](https://github.com/JefinCodes) | [LinkedIn](https://www.linkedin.com/in/jefin-joji-659354313?utm_source=share&utm_campaign=share_via&utm_content=profile&utm_medium=android_app)
