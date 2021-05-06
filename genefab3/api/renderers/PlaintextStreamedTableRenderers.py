from io import StringIO
from csv import writer as CSVWriter
from flask import Response


def _iter_formatted_chunks(chunks, prefix="", delimiter=",", quoting=2, lineterminator=None):
    """Iterate chunks in `delimiter`-separated format"""
    fmtparams = dict(delimiter=delimiter, quoting=quoting)
    if lineterminator:
        fmtparams["lineterminator"] = lineterminator
    with StringIO() as handle:
        writer = CSVWriter(handle, **fmtparams)
        for chunk in chunks:
            writer.writerow(chunk)
            handle.seek(0)
            yield prefix + handle.getvalue()
            handle.truncate()


def _xsv(obj, delimiter):
    """Display StreamedTable in plaintext `delimiter`-separated format"""
    def _iter_chained_formatted_chunks():
        def _header():
            for left, right in zip(obj.index_levels, obj.column_levels):
                yield left + right
        yield from _iter_formatted_chunks(_header(), "#", delimiter, 0)
        yield from _iter_formatted_chunks(obj.rows, "", delimiter, 2)
    return Response(_iter_chained_formatted_chunks(), mimetype="text/plain")


def _iter_json_chunks(obj):
    """Iterate StreamedTable as JSON chunks"""
    def _iter_header(levels, width):
        width = width or len(levels) + 1
        for i, level in enumerate(zip(*levels)):
            yield from _iter_formatted_chunks([level], "[", ",", 2, "]")
            yield "," if i < width - 1 else ""
    def _iter_index_or_values(levels):
        chunks = (next(levels) for _ in range(obj.shape[0]-1))
        yield from _iter_formatted_chunks(chunks, "[", ",", 2, "],")
        yield from _iter_formatted_chunks([next(levels)], "[", ",", 2, "]")
    yield '{"meta":{"index_names":['
    yield from _iter_header(list(obj.index_levels), None)
    yield ']},"columns":['
    yield from _iter_header(list(obj.column_levels), obj.shape[1])
    yield '],"index":['
    yield from _iter_index_or_values(obj.index)
    yield '],"data":['
    yield from _iter_index_or_values(obj.values)
    yield "]}"


def csv(obj, context=None, indent=None):
    """Display StreamedTable as CSV"""
    return _xsv(obj, delimiter=",")

def tsv(obj, context=None, indent=None):
    """Display StreamedTable as TSV"""
    return _xsv(obj, delimiter="\t")

def json(obj, context=None, indent=None):
    """Display StreamedTable as JSON"""
    return Response(_iter_json_chunks(obj), mimetype="application/json")
