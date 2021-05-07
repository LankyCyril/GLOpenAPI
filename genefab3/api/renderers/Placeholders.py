from pandas import MultiIndex, Index
from genefab3.common.types import StreamedAnnotationTable, DataDataFrame


class EmptyStreamedAnnotationTable(StreamedAnnotationTable):
    def __init__(self, *, id_fields):
        self.shape = (0, 0)
        self.index_levels = [["id"] * len(id_fields), id_fields]
        self.column_levels = [[], []]
        self.index = self.values = self.rows = [[]]
        self.header = [["id", f] for f in id_fields]


def EmptyDataDataFrame(*, index_name="index"):
    """Return an empty dataframe that matches data format"""
    dataframe = DataDataFrame(
        index=Index([], name=index_name),
        columns=MultiIndex.from_tuples([("*", "*", "*")]),
    )
    dataframe.drop(columns="*", inplace=True)
    return dataframe
