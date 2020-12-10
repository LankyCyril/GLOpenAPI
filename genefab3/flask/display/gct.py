from genefab3.exceptions import GeneLabFormatException
from pandas import DataFrame
from re import sub, MULTILINE
from flask import Response


OBJECT_TYPE_ERROR = (
    "Internal. Three levels of columns "
    "(accession, assay name, sample name) expected"
)

DF_REPR_KWS = dict(sep="\t", index=False, header=False)
GCT_SUB_KWS = dict(pattern=r'^(.+?\t)', repl=r'\1\1', flags=MULTILINE)


def render_gct(obj, context):
    """Convert a presumed data dataframe to GCT format"""
    if (not isinstance(obj, DataFrame)) or (obj.columns.nlevels != 3):
        raise GeneLabFormatException(OBJECT_TYPE_ERROR, format="gct")
    return Response(
        response=(
            "#1.2\n{}\t{}\n".format(obj.shape[0], obj.shape[1]-1) +
            "Name\tDescription\t" +
            "\t".join("/".join(levels) for levels in obj.columns[1:]) +
            "\n" + sub(string=obj.to_csv(**DF_REPR_KWS), **GCT_SUB_KWS)
        ),
        mimetype="text/plain",
    )
