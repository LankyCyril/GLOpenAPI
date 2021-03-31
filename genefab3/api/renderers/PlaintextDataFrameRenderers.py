from re import sub, MULTILINE
from genefab3.common.exceptions import GeneFabFormatException
from pandas import Series
from flask import Response
from json import dumps
from genefab3.common.utils import JSONByteEncoder


def cls(obj, continuous=None, space_sub=lambda s: sub(r'\s', "", s), indent=None):
    """Display presumed annotation/factor dataframe in plaintext CLS format"""
    columns = [(l0, l1) for (l0, l1) in obj.columns if l0 != "info"]
    if len(columns) != 1:
        m = "Exactly one metadata field must be requested"
        raise GeneFabFormatException(m, columns=columns, format="cls")
    target, sample_count = columns[0], obj.shape[0]
    if (continuous is None) or (continuous is True):
        try:
            _data = [["#numeric"], ["#"+target], obj[target].astype(float)]
        except ValueError:
            if continuous is True:
                m = "Cannot represent target annotation as continuous"
                raise GeneFabFormatException(m, target=target, format="cls")
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
    response = "\n".join(["\t".join([str(f) for f in fs]) for fs in _data])
    return Response(response, mimetype="text/plain")


def gct(obj, indent=None):
    """Display presumed data dataframe in plaintext GCT format"""
    text = obj.to_csv(sep="\t", index=False, header=False)
    response = (
        "#1.2\n{}\t{}\n".format(obj.shape[0], obj.shape[1]-1) +
        "Name\tDescription\t" +
        "\t".join("/".join(levels) for levels in obj.columns[1:]) +
        "\n" + sub(r'^(.+?\t)', r'\1\1', text, flags=MULTILINE)
    )
    return Response(response, mimetype="text/plain")


def xsv(obj, sep=",", indent=None):
    """Display dataframe in plaintext `sep`-separated format"""
    _kws = dict(sep=sep, index=False, header=False, na_rep="NaN")
    header = sub(r'^', "#", sub(r'\n(.)', r'\n#\1',
        obj.columns.to_frame().T.to_csv(**_kws),
    ))
    return Response(header + obj.to_csv(**_kws), mimetype="text/plain")


def json(obj, indent=None):
    """Display dataframe as JSON"""
    raw_json = {
        "columns": obj.columns.tolist(), "data": obj.values.tolist(),
    }
    return Response(
        dumps(raw_json, indent=indent, cls=JSONByteEncoder),
        mimetype="text/json",
    )
