from RestrictedPython import compile_restricted
from RestrictedPython.Eval import default_guarded_getiter, default_guarded_getitem
from RestrictedPython.Guards import (
    safe_builtins,
    safer_getattr,
    full_write_guard,
    guarded_iter_unpack_sequence,
    guarded_unpack_sequence,
)
from smart_contract.gas_meter import GasMeter
import math
import operator

MAX_SEQUENCE_REPEAT = 100_000

_INPLACE_OPS = {
    "+=": operator.iadd,
    "-=": operator.isub,
    "*=": operator.imul,
    "/=": operator.itruediv,
    "//=": operator.ifloordiv,
    "%=": operator.imod,
    "&=": operator.iand,
    "|=": operator.ior,
    "^=": operator.ixor,
}


def _inplacevar_(op, x, y):
    # Only plain arithmetic/bitwise augmented assignment is allowed (no **= / <<= which
    # can create huge integers inside a single C call that the gas meter can't see).
    func = _INPLACE_OPS.get(op)
    if func is None:
        raise Exception(f"Operator {op} not allowed in contracts")
    if op == "*=" and isinstance(x, (str, list, tuple)) and isinstance(y, int) and y > MAX_SEQUENCE_REPEAT:
        raise Exception("Sequence too large")
    return func(x, y)


class ContractEnvironment:
    def __init__(self, code: str):
        self.code = code

        extended_builtins = dict(safe_builtins)
        extended_builtins.update({
            'set': set,
            'dict': dict,
            'list': list,
            'len': len,
            'range': range,
            'min': min,
            'max': max,
            'sum': sum,
            'abs': abs,
            'sorted': sorted,
            'enumerate': enumerate,
            'zip': zip,
            'any': any,
            'all': all,

            # math functions (factorial / exp / pow removed: they run unbounded inside a
            # single C call, invisible to the line based gas meter)
            'sqrt': math.sqrt,
            'ceil': math.ceil,
            'floor': math.floor,
            'fabs': math.fabs,
            'log': math.log,
            'log10': math.log10,
            'sin': math.sin,
            'cos': math.cos,
            'tan': math.tan,
            'degrees': math.degrees,
            'radians': math.radians,
            'pi': math.pi,
            'e': math.e,
            'isclose': math.isclose,
            'gcd': math.gcd,
        })
        extended_builtins.pop('pow', None)

        self.globals = {
            '__builtins__': extended_builtins,
            '__name__': 'contract',
            '__metaclass__': type,
            '_getattr_': safer_getattr,
            '_getiter_': default_guarded_getiter,
            '_getitem_': default_guarded_getitem,
            '_write_': full_write_guard,
            '_inplacevar_': _inplacevar_,
            '_iter_unpack_sequence_': guarded_iter_unpack_sequence,
            '_unpack_sequence_': guarded_unpack_sequence,
        }
        self._compile()

    def _compile(self):
        self.compiled = compile_restricted(self.code, filename='<contract>', mode='exec')
        exec(self.compiled, self.globals)

    def run_contract(self, func_name: str, args, state):
        if not isinstance(func_name, str) or func_name.startswith("_"):
            raise Exception("Invalid function name")
        func = self.globals.get(func_name)
        if not callable(func):
            raise Exception(f"Function '{func_name}' not found in contract.")

        gas_meter = GasMeter()

        try:
            gas_meter.start()
            state, msg = func(*args, state)
        finally:
            gas_meter.stop()

        return state, msg, gas_meter.gas_used
