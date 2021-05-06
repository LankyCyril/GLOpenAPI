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


def _iter_bracketed(prefix="", d=None, n=None, postfix=""):
    """Iterate chunks in bracketed `delimiter`-separated format"""
    leveliter = iter(d)
    chunks = (next(leveliter) for _ in range(n-1))
    yield f"{prefix}["
    yield from _iter_formatted_chunks(chunks, "[", ",", 2, "],")
    yield from _iter_formatted_chunks([next(leveliter)], "[", ",", 2, "]")
    yield f"]{postfix}"


def _xsv(obj, delimiter):
    """Display StreamedTable in plaintext `delimiter`-separated format"""
    obj.move_index_boundary(to=0)
    def _iter_chained_formatted_chunks():
        yield from _iter_formatted_chunks(obj.column_levels, "#", delimiter, 0)
        yield from _iter_formatted_chunks(obj.values, "", delimiter, 2)
    return Response(_iter_chained_formatted_chunks(), mimetype="text/plain")


def _iter_json_chunks(obj):
    """Iterate StreamedTable as JSON chunks"""
    _iw, _h, _w = obj.n_index_levels, *obj.shape
    yield '{"meta":{'
    yield from _iter_bracketed('"index_names":', obj.index_names, _iw, "},")
    yield from _iter_bracketed('"columns":', obj.columns, _w, ",")
    yield from _iter_bracketed('"index":', obj.index, _h, ",")
    yield from _iter_bracketed('"data":', obj.values, _h, "}")


def csv(obj, context=None, indent=None):
    """Display StreamedTable as CSV"""
    return _xsv(obj, delimiter=",")

def tsv(obj, context=None, indent=None):
    """Display StreamedTable as TSV"""
    return _xsv(obj, delimiter="\t")

def json(obj, context=None, indent=None):
    """Display StreamedTable as JSON"""
    return Response(_iter_json_chunks(obj), mimetype="application/json")
