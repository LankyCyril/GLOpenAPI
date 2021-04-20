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


def metadata_dataframe(*, id_fields, object_type="annotation"):
    """Return an empty dataframe that matches metadata format"""
    dataframe = generic_dataframe(["id"], id_fields)
    set_attributes(dataframe, object_type=object_type)
    return dataframe


def data_dataframe(*, object_type="datatable"):
    """Return an empty dataframe that matches data format"""
    dataframe = generic_dataframe(["*"], ["*"], ["index"])
    set_attributes(dataframe, object_type=object_type)
    return dataframe
