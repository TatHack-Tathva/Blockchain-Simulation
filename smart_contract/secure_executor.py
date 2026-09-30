import json
import multiprocessing
import time
import psutil
from smart_contract.sandbox_runner import sandbox_contract_runner

TIMEOUT = 10.0
MEMORY_LIMIT_MB = 500
POLL_INTERVAL = 0.05

# "spawn" gives the sandbox a fresh interpreter: forking a process that runs asyncio,
# worker threads and a web server can deadlock the child on inherited locks.
_ctx = multiprocessing.get_context("spawn")


def _failure(error):
    return {"success": False, "error": error, "state": None, "msg": None, "gas_used": 0}


class SecureContractExecutor:
    def __init__(self, code: str, timeout: float = TIMEOUT, memory_limit_mb: int = MEMORY_LIMIT_MB):
        self.code = code
        self.timeout = timeout
        self.memory_limit_mb = memory_limit_mb

    def run(self, func_name: str, args, state):
        """
            Runs the contract in a separate process. Blocking - call it from a worker thread
            (asyncio.to_thread) and never directly from the event loop.
            The child is always reaped: on timeout / memory overuse it is SIGKILLed (a
            contract can ignore SIGTERM) and joined, so no zombie or orphan is left behind.
        """
        parent_conn, child_conn = _ctx.Pipe(duplex=False)
        process = _ctx.Process(
            target=sandbox_contract_runner,
            args=(self.code, func_name, args, state, child_conn,
                  max(1, int(self.timeout)), self.memory_limit_mb * 2),
            daemon=True,
        )
        process.start()
        child_conn.close()
        start_time = time.monotonic()
        result = None
        try:
            try:
                proc = psutil.Process(process.pid)
            except psutil.NoSuchProcess:
                proc = None

            while True:
                if parent_conn.poll(POLL_INTERVAL):
                    try:
                        result = json.loads(parent_conn.recv())
                    except (EOFError, ValueError, OSError):
                        result = None
                    break
                if not process.is_alive():
                    break
                if time.monotonic() - start_time > self.timeout:
                    return _failure("Execution timeout")
                if proc is not None:
                    try:
                        mem_usage_mb = proc.memory_info().rss / (1024 * 1024)
                        if mem_usage_mb > self.memory_limit_mb:
                            return _failure(f"Memory limit exceeded ({int(mem_usage_mb)} MB)")
                    except psutil.Error:
                        pass
        finally:
            if process.is_alive():
                process.kill()
            process.join()
            parent_conn.close()

        if result is None:
            return _failure(f"Contract process exited without a result (exit code {process.exitcode})")
        return {
            "success": result.get("error") is None,
            "error": result.get("error"),
            "state": result.get("state"),
            "msg": result.get("msg"),
            "gas_used": result.get("gas_used") or 0,
        }
