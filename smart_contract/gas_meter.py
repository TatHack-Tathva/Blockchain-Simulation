import sys

GAS_LIMIT = 10000


class OutOfGas(Exception):
    pass


class GasMeter:
    """
        Counts executed Python lines of contract code. C level work (e.g. a huge sum())
        is not visible to sys.settrace, which is why the executor additionally enforces a
        CPU rlimit, a memory limit and a wall-clock timeout on the sandbox process.
    """
    def __init__(self):
        self.gas_used = 0

    def tracer(self, frame, event, arg):
        if event in ("line", "call"):
            self.gas_used += 1
            if self.gas_used > GAS_LIMIT:
                raise OutOfGas("Out of gas")
        return self.tracer

    def start(self):
        sys.settrace(self.tracer)

    def stop(self):
        sys.settrace(None)
