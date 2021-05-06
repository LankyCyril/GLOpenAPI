from io import StringIO
from csv import writer as CSVWriter
from flask import Response


def _iter_formatted_chunks(chunks, prefix, delimiter, quoting, lineterminator=None):
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
    def _iter_all_formatted_chunks():
        def _header():
            for left, right in zip(obj.index_levels, obj.column_levels):
                yield left + right
        yield from _iter_formatted_chunks(_header(), "#", delimiter, 0)
        yield from _iter_formatted_chunks(obj.rows, "", delimiter, 2)
    return Response(_iter_all_formatted_chunks(), mimetype="text/plain")


def _iter_json_chunks(obj):
    """Iterate StreamedTable as JSON chunks"""
    def _iter_header(levels, width, end):
        width = width or len(levels) + 1
        for i, level in enumerate(zip(*levels)):
            yield from _iter_formatted_chunks([level], "[", ",", 2, "]")
            yield "," if i < width - 1 else end
    def _iter_index_or_values(levels, end):
        chunks = (next(levels) for _ in range(obj.shape[0]-1))
        yield from _iter_formatted_chunks(chunks, "[", ",", 2, "],")
        yield from _iter_formatted_chunks([next(levels)], "[", ",", 2, "]"+end)
    yield '{"meta":{"index_names":['
    yield from _iter_header(list(obj.index_levels), None, end="]},")
    yield '"columns":['
    yield from _iter_header(list(obj.column_levels), obj.shape[1], end="],")
    yield '"index":['
    yield from _iter_index_or_values(obj.index, end="],")
    yield '"data":['
    yield from _iter_index_or_values(obj.values, end="]}")


def csv(obj, context=None, indent=None):
    """Display StreamedTable as CSV"""
    return _xsv(obj, delimiter=",")

def tsv(obj, context=None, indent=None):
    """Display StreamedTable as TSV"""
    return _xsv(obj, delimiter="\t")

def json(obj, context=None, indent=None):
    """Display StreamedTable as JSON"""
    return Response(_iter_json_chunks(obj), mimetype="application/json")
