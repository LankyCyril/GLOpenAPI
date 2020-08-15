from numpy import nan
from argparse import Namespace
from genefab3.exceptions import GeneLabJSONException
from pandas import DataFrame, concat


Any, Atom = "Any", "Atom"


class TurtleDict(float):
    """Empty dictionary with infinite descent that masquerades as numpy.nan"""
    def __new__(self):
        return float.__new__(self, nan)
    def __getitem__(self, x):
        return TurtleDict()
    def __len__(self):
        return 0


class TurtleSpace(Namespace):
    """Namespace with infinite descent"""
    def __getattr__(self, x):
        return getattr(super(), x, TurtleSpace())


def SparseTable(entries):
    """Combines 'header' and 'raw' fields into two-level DataFrame"""
    try:
        raw_header = DataFrame(entries["header"])
        raw_values = DataFrame(entries["raw"])
    except (KeyError, TypeError):
        raise GeneLabJSONException("Malformed sparse table passed")
    if raw_header["encoded"].any():
        raise GeneLabJSONException("Encoded fields are not supported")
    else:
        return (
            concat([
                raw_header[["field", "title"]].set_index("field").T,
                raw_values
            ])
            .T.reset_index()
            .set_index(["title", "index"]).T
        )
