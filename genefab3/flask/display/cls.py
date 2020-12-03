from genefab3.config import INFO
from genefab3.exceptions import GeneLabFormatException
from re import sub
from pandas import DataFrame, Series
from flask import Response


OBJECT_TYPE_ERROR = "'fmt=cls' requires a two-level dataframe (/samples/)"
FIELD_COUNT_ERROR = "'fmt=cls' requires exactly one metadata field"


def is_continuous(continuous, dataframe, target):
    """Return value of `continuous` if boolean; if "infer", return True if `dataframe[target]` is numeric, otherwise return False"""
    if continuous == "infer":
        try:
            dataframe[target].astype(float)
        except ValueError:
            return False
        else:
            return True
    else:
        return continuous


def render_cls(obj, context, continuous="infer", space_sub=lambda s: sub(r'\s', "", s)):
    """Convert a presumed annotation/factor dataframe to CLS format"""
    if (not isinstance(obj, DataFrame)) or (obj.columns.nlevels != 2):
        raise GeneLabFormatException(OBJECT_TYPE_ERROR)
    target_columns = [(l0, l1) for (l0, l1) in obj.columns if l0 != INFO]
    if len(target_columns) != 1:
        raise GeneLabFormatException(FIELD_COUNT_ERROR)
    else:
        target = target_columns[0]
    sample_count = obj.shape[0]
    if is_continuous(continuous, obj, target):
        cls_data = [["#numeric"], ["#"+target], obj[target].astype(float)]
    else:
        if space_sub is None:
            space_sub = lambda s: s
        classes = obj[target].unique()
        class2id = Series(index=classes, data=range(len(classes)))
        cls_data = [
            [sample_count, len(classes), 1],
            ["# "+space_sub(classes[0])] + [space_sub(c) for c in classes[1:]],
            [class2id[v] for v in obj[target]]
        ]
    return Response(
        "\n".join([
            "\t".join([str(f) for f in fields]) for fields in cls_data
        ]),
        mimetype="text/plain",
    )
