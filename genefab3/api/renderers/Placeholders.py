from genefab3.common.types import StreamedAnnotationTable, NaN


class EmptyStreamedAnnotationTable(StreamedAnnotationTable):
    """Return an empty StreamedAnnotationTable-like"""
    __init__ = move_index_boundary = lambda *a, **k: None
    shape, n_index_levels = (0, 0), 1
    index_levels, column_levels = ["*"], ["*", "*"]
    index = values = [[NaN]]
