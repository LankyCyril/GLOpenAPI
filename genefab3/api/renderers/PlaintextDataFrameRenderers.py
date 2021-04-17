from re import sub, MULTILINE
from genefab3.common.exceptions import GeneFabFormatException
from pandas import Series
from flask import Response
from functools import partial
from genefab3.common.utils import get_attribute, JSONByteEncoder
from genefab3.common.exceptions import GeneFabConfigurationException
from json import dumps


GCT_ALLOWED_DATATYPES = {
    "processed microarray data", "normalized counts",
    "unnormalized counts",
}


def cls(obj, context=None, continuous=None, space_sub=lambda s: sub(r'\s', "", s), indent=None):
    """Display presumed annotation/factor dataframe in plaintext CLS format"""
    columns = [c for c in obj.columns if c[0] not in {"info", "file"}]
    if len(columns) != 1:
        msg = "Exactly one target assay/study metadata field must be present"
        raise GeneFabFormatException(msg, target_columns=columns, format="cls")
    target, sample_count = columns[0], obj.shape[0]
    if (continuous is None) or (continuous is True):
        try:
            _data = [
                ["#numeric"], ["#" + (".".join(target))],
                obj[target].astype(float),
            ]
        except ValueError:
            if continuous is True:
                msg = "Cannot represent target annotation as continuous"
                raise GeneFabFormatException(msg, target=target, format="cls")
            else:
                continuous = False
    if continuous is False:
        _sub, classes = space_sub or (lambda s: s), obj[target].unique()
        class2id = Series(index=classes, data=range(len(classes)))
        _data = [
            [sample_count, len(classes), 1],
            ["# "+_sub(classes[0])] + [_sub(c) for c in classes[1:]],
            [class2id[v] for v in obj[target]]
        ]
    content = "\n".join(["\t".join([str(f) for f in fs]) for fs in _data])
    return Response(content, mimetype="text/plain")


def gct(obj, context=None, indent=None):
    """Display presumed data dataframe in plaintext GCT format"""
    datatypes = get_attribute(obj, "datatypes", set())
    if len(datatypes) == 0:
        msg = "No datatype information associated with retrieved data"
        raise GeneFabConfigurationException(msg)
    elif len(datatypes) > 1:
        msg = "GCT format does not support mixed datatypes"
        raise GeneFabFormatException(msg, datatypes=datatypes)
    elif next(iter(datatypes)) not in GCT_ALLOWED_DATATYPES:
        msg = "GCT format is not valid for given datatype"
        raise GeneFabFormatException(msg, datatype=datatypes.pop())
    else:
        text = obj.to_csv(sep="\t", index=False, header=False, na_rep="")
        # NaNs left empty: https://www.genepattern.org/file-formats-guide#GCT
        content = (
            "#1.2\n{}\t{}\n".format(obj.shape[0], obj.shape[1]-1) +
            "Name\tDescription\t" +
            "\t".join("/".join(levels) for levels in obj.columns[1:]) +
            "\n" + sub(r'^(.+?\t)', r'\1\1', text, flags=MULTILINE)
        )
        return Response(content, mimetype="text/plain")


def xsv(obj, context=None, sep=None, indent=None):
    """Display dataframe in plaintext `sep`-separated format"""
    _kws = dict(sep=sep, index=False, header=False, na_rep="NaN")
    raw_header = obj.columns.to_frame().T.to_csv(**_kws)
    header = sub(r'^', "#", sub(r'\n(.)', r'\n#\1', raw_header))
    return Response(header + obj.to_csv(**_kws), mimetype="text/plain")


csv = partial(xsv, sep=",")
tsv = partial(xsv, sep="\t")


def json(obj, context=None, indent=None):
    """Display dataframe as JSON"""
    if get_attribute(obj, "object_type") == "datatable":
        _json = {
            "columns": obj.columns.tolist(), "index": obj.values[:,0].tolist(),
            "data": obj.values[:,1:].tolist(),
        }
    else:
        _json = {
            "columns": obj.columns.tolist(), "data": obj.values.tolist(),
        }
    content = dumps(_json, indent=indent, cls=JSONByteEncoder)
    return Response(content, mimetype="text/json")
