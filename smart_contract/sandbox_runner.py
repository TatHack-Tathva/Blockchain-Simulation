import json


def _apply_resource_limits(cpu_seconds, memory_mb):
    try:
        import resource
    except ImportError:  # Windows: rely on the parent's timeout / memory polling
        return
    try:
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds + 1))
    except (ValueError, OSError):
        pass
    try:
        limit = memory_mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (ValueError, OSError):
        pass


def sandbox_contract_runner(code, func_name, args, state, conn, cpu_seconds, memory_mb):
    """Child process entry point. Sends exactly one JSON result over `conn`."""
    _apply_resource_limits(cpu_seconds, memory_mb)
    try:
        from smart_contract.smart_contract import ContractEnvironment
        env = ContractEnvironment(code)
        state, msg, gas_used = env.run_contract(func_name, args, state)
        result = {"state": state, "msg": msg, "gas_used": gas_used, "error": None}
        payload = json.dumps(result)
    except BaseException as e:  # MemoryError / RecursionError / contract errors
        payload = json.dumps({"state": None, "msg": None, "gas_used": 0, "error": f"{type(e).__name__}: {e}"})
    try:
        conn.send(payload)
    finally:
        conn.close()
