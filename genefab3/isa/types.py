from genefab3.exceptions import GeneLabJSONException
from pandas import DataFrame, concat


def ToSparseTable(entries):
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
