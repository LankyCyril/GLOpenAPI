from pandas import DataFrame, MultiIndex
from genefab3.common.types import AnnotationDataFrame, DataDataFrame
from itertools import cycle


def EmptyDataFrame(DataFrameType=DataFrame, *level_values):
    """Return an empty dataframe with specificed column names"""
    maxlen = max(map(len, level_values))
    cyclers = [cycle(v) if (len(v) < maxlen) else iter(v) for v in level_values]
    return DataFrameType(columns=MultiIndex.from_tuples(zip(*cyclers)))


def EmptyAnnotationDataFrame(*, id_fields):
    """Return an empty dataframe that matches metadata format"""
    return EmptyDataFrame(AnnotationDataFrame, ["id"], id_fields)


def EmptyDataDataFrame():
    """Return an empty dataframe that matches data format"""
    return EmptyDataFrame(DataDataFrame, ["*"], ["*"], ["index"])
