from pandas import DataFrame, MultiIndex
from itertools import cycle
from genefab3.common.utils import set_attributes


def generic_dataframe(*level_values):
    """Return an empty dataframe with specificed column names"""
    maxlen = max(map(len, level_values))
    cyclers = [
        cycle(values) if (len(values) < maxlen) else iter(values)
        for values in level_values
    ]
    return DataFrame(columns=MultiIndex.from_tuples(zip(*cyclers)))


def metadata_dataframe(*, include=(), genefab_type="annotation"):
    """Return an empty dataframe that matches metadata format"""
    dataframe = generic_dataframe(
        ["info"], [
            "accession", "assay",
            *(c.lstrip("info.").strip(".") for c in include),
        ],
    )
    set_attributes(dataframe, genefab_type=genefab_type)
    return dataframe


def data_dataframe(*, genefab_type="datatable"):
    """Return an empty dataframe that matches data format"""
    dataframe = generic_dataframe(["info"], ["info"], ["entry"])
    set_attributes(dataframe, genefab_type=genefab_type)
    return dataframe
