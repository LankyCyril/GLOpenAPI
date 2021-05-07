from re import sub, MULTILINE
from genefab3.common.exceptions import GeneFabFormatException
from flask import Response
from functools import partial
from genefab3.common.types import DataDataFrame
from genefab3.common.exceptions import GeneFabConfigurationException
from json import dumps


def gct(obj, context=None, indent=None, level_formatter="/".join):
    """Display presumed data dataframe in plaintext GCT format"""
    if (not isinstance(obj, DataDataFrame)) or (len(obj.datatypes) == 0):
        msg = "No datatype information associated with retrieved data"
        raise GeneFabConfigurationException(msg)
    elif len(obj.datatypes) > 1:
        msg = "GCT format does not support mixed datatypes"
        raise GeneFabFormatException(msg, datatypes=obj.datatypes)
    elif not obj.gct_valid:
        msg = "GCT format is not valid for given datatype"
        raise GeneFabFormatException(msg, datatype=obj.datatypes.pop())
    else:
        text = obj.to_csv(sep="\t", header=False, na_rep="")
        # NaNs left empty: https://www.genepattern.org/file-formats-guide#GCT
        content = (
            "#1.2\n{}\t{}\n".format(*obj.shape),
            "Name\tDescription\t" +
            "\t".join(level_formatter(levels) for levels in obj.columns) +
            "\n" + sub(r'^(.+?\t)', r'\1\1', text, flags=MULTILINE)
        )
        return Response(content, mimetype="text/plain")


def get_index_and_columns(obj, col_fill="*"):
    """Quickly reset index inplace and return new columns as pandas columns"""
    index_nestedness_set = {len(ixn[0]) for ixn in obj.index.names}
    if index_nestedness_set == {obj.columns.nlevels}:
        return obj[:0].reset_index().columns
    elif index_nestedness_set == {1}:
        return obj[:0].reset_index(col_level=-1, col_fill=col_fill).columns
    else:
        raise GeneFabConfigurationException(
            "Index level depth does not match column level count",
            index_nestedness_set=index_nestedness_set,
            column_levels=obj.columns.nlevels,
        )


def xsv(obj, context=None, sep=None, indent=None, na_rep="NaN"):
    """Display dataframe in plaintext `sep`-separated format"""
    index_and_columns = get_index_and_columns(obj)
    _na_rep = type("UnquotedNaN", (float,), dict(__str__=lambda _: na_rep))()
    _kws = dict(sep=sep, index=True, header=False, quoting=2, na_rep=_na_rep)
    _head_kws = dict(sep=sep, index=False, header=False)
    raw_header = index_and_columns.to_frame().T.to_csv(**_head_kws)
    header = sub(r'^', "#", raw_header.rstrip(), flags=MULTILINE)
    return Response(header + "\n" + obj.to_csv(**_kws), mimetype="text/plain")


csv = partial(xsv, sep=",")
tsv = partial(xsv, sep="\t")


def json(obj, context=None, indent=None):
    """Display dataframe as JSON"""
    col_fill = ["*"] * (obj.columns.nlevels - 1)
    index_names = dumps(indent=indent, separators=(",", ":"), obj=[
        list(ixn) if isinstance(ixn, tuple) else [*col_fill, ixn]
        for ixn in obj.index.names
    ])
    meta_prefix = f'{{"meta":{{"index_names":{index_names}}},'
    raw_json = obj.to_json(orient="split")
    return Response(meta_prefix + raw_json[1:], mimetype="application/json")
