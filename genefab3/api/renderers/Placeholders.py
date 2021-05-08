from genefab3.common.types import StreamedAnnotationTable, NaN


class EmptyStreamedAnnotationTable(StreamedAnnotationTable):
    """Return an empty StreamedAnnotationTable-like"""
    def __init__(self):
        self.shape, self.n_index_levels = (0, 0), 1
        self.index_levels, self.column_levels = ["*"], ["*", "*"]
        self.index_names, self.columns = [["*"]], [["*", "*"]]
        self.index, self.values = [[NaN]], [[NaN]]
        self.move_index_boundary = lambda *a, **k: None
