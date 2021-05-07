from genefab3.common.types import StreamedAnnotationTable, DataDataFrame, NaN
from pandas import Index, MultiIndex


class EmptyStreamedAnnotationTable(StreamedAnnotationTable):
    """Return an empty StreamedAnnotationTable-like"""
    def __init__(self):
        self.shape, self.n_index_levels = (0, 0), 1
        self.index_levels, self.column_levels = ["*"], ["*", "*"]
        self.index_names, self.columns = [["*"]], [["*", "*"]]
        self.index, self.values = [[NaN]], [[NaN]]
        self.move_index_boundary = lambda *a, **k: None


def EmptyDataDataFrame(*, index_name="index"):
    """Return an empty dataframe that matches data format"""
    dataframe = DataDataFrame(
        index=Index([], name=index_name),
        columns=MultiIndex.from_tuples([("*", "*", "*")]),
    )
    dataframe.drop(columns="*", inplace=True)
    return dataframe
