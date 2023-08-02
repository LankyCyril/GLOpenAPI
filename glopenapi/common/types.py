from glopenapi.common.exceptions import GLOpenAPILogger
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from collections.abc import Callable


class ExtNaN(float):
    """Extended nan: math.nan represented as NaN and comparable against any types"""
    def __new__(self): return float.__new__(self, "nan")
    def __str__(self): return "NaN"
    def __repr__(self): return "NaN"
    def __eq__(self, other): return False
    def __lt__(self, other): return not isinstance(other, float)
    def __gt__(self, other): return False
    def __hash__(self): return hash(float("nan"))
NaN = ExtNaN()


class FuncTee():
    """Used similar to `itertools.tee()`, but re-runs the generating function instead of keeping yielded elements in memory"""
    def __init__(self, func, *args, **kwargs):
        self.func, self.args, self.kwargs = func, args, kwargs
        self.n_invocations = 0
    def __iter__(self):
        self.n_invocations += 1
        desc = f"invocation no. {self.n_invocations}: {self.func!r}"
        GLOpenAPILogger.debug(f"Starting {desc}")
        result = self.func(*self.args, **self.kwargs)
        GLOpenAPILogger.debug(f"Finished {desc}")
        return result


class Adapter():
    """Base class for database adapters""" # TODO: documentation for `get_accessions` and `get_files_by_accession`
 
    def __init__(self):
        """Validate subclassed Adapter"""
        for method_name in "get_accessions", "get_files_by_accession":
            if not isinstance(getattr(self, method_name, None), Callable):
                msg = "Adapter must define method"
                _kw = dict(adapter=type(self).__name__, method=method_name)
                raise GLOpenAPIConfigurationException(msg, **_kw)
 
    def best_sample_name_matches(self, name, names, return_positions=False):
        """Test sample name identity (fallback behavior)"""
        if return_positions:
            positions_and_matches = [
                (p, ns) for p, ns in enumerate(names) if ns == name
            ]
            return (
                [ns for p, ns in positions_and_matches],
                [p for p, ns in positions_and_matches],
            )
        else:
            return [ns for ns in names if ns == name]
