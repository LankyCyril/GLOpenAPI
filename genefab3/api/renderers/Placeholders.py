from pandas import DataFrame, MultiIndex, Index
from itertools import cycle
from genefab3.common.types import AnnotationDataFrame, DataDataFrame
from genefab3.common.types import StreamedAnnotationTable


def EmptyDataFrame(*level_values):
    """Return an empty dataframe with specificed column names and no index"""
    maxlen = max(map(len, level_values))
    cyclers = [cycle(v) if (len(v) < maxlen) else iter(v) for v in level_values]
    return DataFrame(columns=MultiIndex.from_tuples(zip(*cyclers)))


def EmptyAnnotationDataFrame(*, id_fields):
    """Return an empty dataframe that matches metadata format"""
    dataframe = AnnotationDataFrame(
        index=MultiIndex.from_tuples((), names=[("id", f) for f in id_fields]),
        columns=MultiIndex.from_tuples([("*", "*")]),
    )
    dataframe.drop(columns="*", inplace=True)
    return dataframe


def EmptyDataDataFrame(*, index_name="index"):
    """Return an empty dataframe that matches data format"""
    dataframe = DataDataFrame(
        index=Index([], name=index_name),
        columns=MultiIndex.from_tuples([("*", "*", "*")]),
    )
    dataframe.drop(columns="*", inplace=True)
    return dataframe


class EmptyStreamedAnnotationTable(StreamedAnnotationTable):
    def __init__(self, *, id_fields):
        self.shape = (0, 0)
        self.index_levels = [["id"] * len(id_fields), id_fields]
        self.column_levels = [[], []]
        self.index = self.values = self.rows = [[]]
        self.header = [["id", f] for f in id_fields]
