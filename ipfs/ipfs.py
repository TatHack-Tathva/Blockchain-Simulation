import subprocess, re, os
from pathlib import Path

_BASE_DIR = Path(__file__).resolve().parent
# Only files inside the upload directory may be published, and downloads are only
# written inside the download directory (both overridable through the environment).
UPLOAD_DIR = Path(os.environ.get("BLOCKCHAIN_IPFS_UPLOAD_DIR", _BASE_DIR / "uploads")).resolve()
DOWNLOAD_DIR = Path(os.environ.get("BLOCKCHAIN_IPFS_DOWNLOAD_DIR", _BASE_DIR / "retreived")).resolve()

_CID_RE = re.compile(r"^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{50,100}|z[1-9A-HJ-NP-Za-km-z]{40,100})$")


def is_valid_cid(cid) -> bool:
    return isinstance(cid, str) and bool(_CID_RE.match(cid))


def _inside(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def resolve_upload_path(file_path) -> Path:
    resolved = Path(file_path).expanduser().resolve()
    if not _inside(resolved, UPLOAD_DIR):
        raise ValueError(f"Only files inside {UPLOAD_DIR} can be uploaded")
    if not resolved.is_file():
        raise ValueError("File doesn't exist")
    return resolved


def resolve_download_path(destination_path) -> Path:
    if not isinstance(destination_path, str) or not destination_path or "\x00" in destination_path:
        raise ValueError("Invalid destination path")
    resolved = (DOWNLOAD_DIR / destination_path).resolve()
    if not _inside(resolved, DOWNLOAD_DIR) or resolved == DOWNLOAD_DIR:
        raise ValueError("Path traversal detected")
    return resolved


def addToIpfs(file_path):
    try:
        safe_path = resolve_upload_path(file_path)
        result = subprocess.run(
            ['ipfs', 'add', '--', str(safe_path)],
            capture_output=True,
            text=True,
            check=True
        )

        for line in reversed(result.stdout.strip().split('\n')):
            match = re.match(r'added\s+(\S+)\s+(.+)', line)
            if match and is_valid_cid(match.group(1)):
                return match.group(1), match.group(2)

        print("Error: Could not parse IPFS add output for hash and name.")
        return None, None

    except subprocess.CalledProcessError as e:
        print(f"Error adding file to IPFS: {e}")
        print(f"Stderr: {e.stderr}")
        return None, None

    except Exception as e:
        print(f"Error adding file to IPFS: {e}")
        return None, None


def download_ipfs_file_subprocess(cid: str, destination_path: str):
    """
        Downloads a file from IPFS using the 'ipfs get' CLI command.
        destination_path is relative to DOWNLOAD_DIR and may not escape it; the CID is
        validated so it can never be interpreted as a command line flag.
    """
    if not is_valid_cid(cid):
        raise ValueError("Invalid CID")
    safe_path = resolve_download_path(destination_path)
    safe_path.parent.mkdir(parents=True, exist_ok=True)

    command = ["ipfs", "get", "-o", str(safe_path), "--", cid]
    try:
        subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"Successfully downloaded CID {cid} to: {safe_path}")
        return str(safe_path)
    except FileNotFoundError:
        print("Error: 'ipfs' command not found. Please ensure IPFS CLI is installed and in your PATH.")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading file with CID {cid}: exit code {e.returncode}")
        print("STDERR:", (e.stderr or "").strip())
    return None
