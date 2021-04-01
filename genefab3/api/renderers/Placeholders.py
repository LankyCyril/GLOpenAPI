from pandas import DataFrame, MultiIndex
from itertools import cycle


def generic_dataframe(*level_values):
    """Return an empty dataframe with specificed column names"""
    maxlen = max(map(len, level_values))
    cyclers = [
        cycle(values) if (len(values) < maxlen) else iter(values)
        for values in level_values
    ]
    return DataFrame(columns=MultiIndex.from_tuples(zip(*cyclers)))


def metadata_dataframe(include=()):
    """Return an empty dataframe that matches metadata format"""
    return generic_dataframe(
        ["info"], [
            "accession", "assay",
            *(c.lstrip("info.").strip(".") for c in include),
        ],
    )


def data_dataframe():
    """Return an empty dataframe that matches data format"""
    return generic_dataframe(["info"], ["info"], ["entry"])
