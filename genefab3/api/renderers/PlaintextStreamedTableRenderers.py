from genefab3.common.utils import json_permissive_default
from json import dumps
from io import StringIO
from csv import writer as CSVWriter
from re import sub
from genefab3.common.exceptions import GeneFabFormatException
from genefab3.common.utils import as_is
from genefab3.common.types import StreamedDataTable
from genefab3.common.exceptions import GeneFabConfigurationException


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


def _iter_json_chunks(prefix="", data=None, length=None, postfix="", default=json_permissive_default):
    """Iterate chunks in bracketed `delimiter`-separated format"""
    def _foreach(chunks, end):
        for chunk in chunks:
            yield dumps(chunk, separators=(",", ":"), default=default) + end
    leveliter = iter(data)
    chunks = (next(leveliter) for _ in range(length-1))
    yield f"{prefix}["
    try:
        yield from _foreach(chunks, end=",")
        yield from _foreach([next(leveliter)], end="")
    except StopIteration:
        pass
    yield f"]{postfix}"


def _iter_xsv_chunks(chunks, prefix="", delimiter=",", quoting=2, lineterminator=None):
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
    obj.move_index_boundary(to=0)
    def content():
        yield from _iter_xsv_chunks(obj.column_levels, "#", delimiter, 0)
        yield from _iter_xsv_chunks(obj.values, "", delimiter, 2)
    return content, "text/plain"


def cls(obj, context=None, continuous=None, space_formatter=lambda s: sub(r'\s', "_", s), indent=None):
    """Display presumed annotation/factor StreamedAnnotationTable in plaintext CLS format"""
    if getattr(obj, "cls_valid", None) is not True:
        msg = "Exactly one target assay/study metadata field must be present"
        _kw = dict(target_columns=getattr(obj, "metadata_columns", []))
        raise GeneFabFormatException(msg, **_kw, format="cls")
    else:
        target_name = ".".join(obj.metadata_columns[0])
        target = obj._column_key_dispatcher[target_name]
    def content(continuous=continuous, space_formatter=space_formatter):
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
        yield from lines
    return content, "text/plain"


def gct(obj, context=None, indent=None, level_formatter="/".join):
    """Display StreamedDataTable in plaintext GCT format, if supported"""
    if (not isinstance(obj, StreamedDataTable)) or (len(obj.datatypes) == 0):
        msg = "No datatype information associated with retrieved data"
        raise GeneFabConfigurationException(msg)
    elif len(obj.datatypes) > 1:
        msg = "GCT format does not support mixed datatypes"
        raise GeneFabFormatException(msg, datatypes=obj.datatypes)
    elif not obj.gct_valid:
        msg = "GCT format is not valid for given datatype"
        raise GeneFabFormatException(msg, datatype=obj.datatypes.pop())
    else:
        def content():
            obj.na_rep = "" # https://www.genepattern.org/file-formats-guide#GCT
            yield "#1.2\n{}\t{}\n".format(*obj.shape)
            yield "Name\tDescription"
            for level in obj.columns:
                yield "\t" + level_formatter(level)
            yield "\n"
            _iter_value_lines = _iter_xsv_chunks(obj.values, "", "\t", 0)
            for (index, *_), value_line in zip(obj.index, _iter_value_lines):
                yield f"{index}\t{index}\t{value_line}"
    return content, "text/plain"


def csv(obj, context=None, indent=None):
    """Display StreamedTable as CSV"""
    return _xsv(obj, delimiter=",")


def tsv(obj, context=None, indent=None):
    """Display StreamedTable as TSV"""
    return _xsv(obj, delimiter="\t")


def json(obj, context=None, indent=None):
    """Display StreamedTable as JSON"""
    _iw, _h, _w = obj.n_index_levels, *obj.shape
    def content():
        yield '{"meta":{"index_names":'
        yield from _iter_json_chunks('', obj.index_names, _iw, "},")
        yield from _iter_json_chunks('"columns":', obj.columns, _w, ",")
        yield from _iter_json_chunks('"index":', obj.index, _h, ",")
        yield from _iter_json_chunks('"data":', obj.values, _h, "}")
    return content, "application/json"
