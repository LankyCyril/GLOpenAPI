from io import StringIO
from csv import writer as CSVWriter
from re import sub
from genefab3.common.exceptions import GeneFabFormatException
from genefab3.common.utils import as_is
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


def _list_continuous_cls(obj, target, target_name):
    """Iterate over entire `obj` before returning CLS-formatted data; fails if cannot be represented as continuous"""
    return [
        "#numeric\n", f"#{target_name}\n",
        "\t".join(str(float(row[target])) for row in obj.values),
    ]


def _iter_discrete_cls(obj, target, space_formatter):
    """Iterate lines one by one as discrete CLS-formatted data"""
    classes, _classes_set = [], set()
    for row in obj.values:
        v = row[target]
        if v not in _classes_set:
            classes.append(v)
            _classes_set.add(v)
    class2id = {c: str(i) for i, c in enumerate(classes)}
    yield f"{obj.shape[0]}\t{len(classes)}\t1\n# "
    yield "\t".join(space_formatter(c) for c in classes) + "\n"
    yield "\t".join(class2id[row[target]] for row in obj.values) + "\n"


def cls(obj, context=None, continuous=None, space_formatter=lambda s: sub(r'\s', "_", s), indent=None):
    """Display presumed annotation/factor StreamedAnnotationTable in plaintext CLS format"""
    if getattr(obj, "cls_valid", None) is not True:
        msg = "Exactly one target assay/study metadata field must be present"
        _kw = dict(target_columns=getattr(obj, "metadata_columns", []))
        raise GeneFabFormatException(msg, **_kw, format="cls")
    else:
        target_name = ".".join(obj.metadata_columns[0])
        target = obj._column_key_dispatcher[target_name]
    if (continuous is None) or (continuous is True):
        try:
            lines = _list_continuous_cls(obj, target, target_name)
        except ValueError:
            if continuous is True:
                msg = "Cannot represent target annotation as continuous"
                _kw = dict(target=target_name, format="cls")
                raise GeneFabFormatException(msg, **_kw)
            else:
                continuous = False
    if continuous is False:
        space_formatter = space_formatter or as_is
        lines = _iter_discrete_cls(obj, target, space_formatter)
    return Response(iter(lines), mimetype="text/plain")


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
