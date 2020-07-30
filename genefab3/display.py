from flask import Response
from pandas import DataFrame


DF_KWS = dict(index=False, na_rep="NA")

DF_CSS = """<style>
    table {
        border-spacing: 0;
        border-left: 1pt solid black; border-top: 1pt solid black;
    }
    th, td {
        font-family: sans; font-size: 12pt;
        text-align: left; padding-right: 16pt; white-space: nowrap;
        border-right: 1pt solid black; border-bottom: 1pt solid black;
    }
    th {
        background: #DDD;
        position: sticky; position: -webkit-sticky; top: 0;
    }
    td:first-child, td:nth-child(2) {
        background: #F2F2F2;
    }
</style>"""


def color_bool(x):
    if x == True:
        return "color: green"
    elif x == False:
        return "color: #FAA"
    else:
        return ""


def display_dataframe(df, fmt):
    """Display dataframe with specified format"""
    if fmt == "tsv":
        return Response(df.to_csv(sep="\t", **DF_KWS), mimetype="text/plain")
    elif fmt == "csv":
        return Response(df.to_csv(sep=",", **DF_KWS), mimetype="text/plain")
    elif fmt == "html":
        html = df.style.applymap(color_bool).hide_index().render()
        return Response(DF_CSS + html, mimetype="text/html")
    else:
        raise NotImplementedError("fmt='{}'".format(fmt))


def display(obj_and_rargs):
    """Dispatch object and trailing request arguments to display handler"""
    obj, rargs = obj_and_rargs
    fmt = rargs.get("fmt", "tsv")
    if isinstance(obj, DataFrame):
        return display_dataframe(obj, fmt)
    else:
        raise NotImplementedError(
            "Display of {}".format(str(type(obj).strip("<>")))
        )
