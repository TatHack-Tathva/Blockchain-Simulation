import os
import re
import json

BASE_STORAGE_DIR = os.path.dirname(os.path.abspath(__file__))
VALID_CONSENSUS = ("pow", "pos", "poa")
KEY_PASSPHRASE_ENV = "BLOCKCHAIN_KEY_PASSPHRASE"
_PROFILE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def get_consensus_dir(consensus, profile=None):
    """
        Returns the storage directory for the given consensus type (and optional node
        profile, so several nodes on one machine don't overwrite each other's data).
        Both values are validated so they can't be used to escape BASE_STORAGE_DIR.
    """
    if consensus not in VALID_CONSENSUS:
        raise ValueError(f"Invalid consensus '{consensus}'")
    path = os.path.join(BASE_STORAGE_DIR, consensus)
    if profile is not None:
        if not isinstance(profile, str) or not _PROFILE_RE.match(profile) or profile in (".", ".."):
            raise ValueError(f"Invalid storage profile '{profile}'")
        path = os.path.join(path, profile)
    resolved = os.path.realpath(path)
    if os.path.commonpath([resolved, os.path.realpath(BASE_STORAGE_DIR)]) != os.path.realpath(BASE_STORAGE_DIR):
        raise ValueError("Storage path escapes the storage directory")
    os.makedirs(resolved, mode=0o700, exist_ok=True)
    return resolved


def _write_json(path, data, private=False):
    tmp = path + ".tmp"
    mode = 0o600 if private else 0o644
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=4)
    if private:
        os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def _read_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (OSError, ValueError):
        print(f"Ignoring unreadable storage file {path}")
        return None


# == Node ID ===

def save_node_id(node_id, consensus, profile=None):
    _write_json(os.path.join(get_consensus_dir(consensus, profile), "node_id.json"), {"node_id": node_id})


def load_node_id(consensus, profile=None):
    data = _read_json(os.path.join(get_consensus_dir(consensus, profile), "node_id.json"))
    return data.get("node_id") if isinstance(data, dict) else None


# === Keys ===

def _encrypt_private_key(private_key_pem, passphrase):
    from cryptography.hazmat.primitives import serialization
    key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(passphrase.encode()),
    ).decode()


def _decrypt_private_key(encrypted_pem, passphrase):
    from cryptography.hazmat.primitives import serialization
    key = serialization.load_pem_private_key(encrypted_pem.encode(), password=passphrase.encode())
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()


def save_key(private_key_pem, consensus, profile=None):
    """
        The key file is always created owner-read/write only (0600). If the
        BLOCKCHAIN_KEY_PASSPHRASE environment variable is set the key is also encrypted.
    """
    passphrase = os.environ.get(KEY_PASSPHRASE_ENV)
    if passphrase:
        data = {"encrypted_private_key_pem": _encrypt_private_key(private_key_pem, passphrase)}
    else:
        data = {"private_key_pem": private_key_pem}
    _write_json(os.path.join(get_consensus_dir(consensus, profile), "keys.json"), data, private=True)


def load_key(consensus, profile=None):
    data = _read_json(os.path.join(get_consensus_dir(consensus, profile), "keys.json"))
    if not isinstance(data, dict):
        return None
    if data.get("encrypted_private_key_pem"):
        passphrase = os.environ.get(KEY_PASSPHRASE_ENV)
        if not passphrase:
            raise ValueError(f"Stored key is encrypted, set {KEY_PASSPHRASE_ENV}")
        return _decrypt_private_key(data["encrypted_private_key_pem"], passphrase)
    return data.get("private_key_pem")


# === Chain ===

def save_chain(chain, consensus, profile=None):
    _write_json(os.path.join(get_consensus_dir(consensus, profile), "chain.json"), chain)


def load_chain(consensus, profile=None):
    return _read_json(os.path.join(get_consensus_dir(consensus, profile), "chain.json"))


# === Peers ===

def save_peers(peer_list, consensus, profile=None):
    _write_json(os.path.join(get_consensus_dir(consensus, profile), "peers.json"), peer_list)


def load_peers(consensus, profile=None):
    return _read_json(os.path.join(get_consensus_dir(consensus, profile), "peers.json"))
