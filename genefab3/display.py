from flask import Response
from pandas import DataFrame, isnull
from re import sub


DF_KWS = dict(index=False, header=False, na_rep="NA")

DF_CSS = """<style>
    table {
        border-spacing: 0;
        border-right: 1pt solid black; border-bottom: 1pt solid black;
    }
    th, td {
        font-family: sans; font-size: 12pt;
        height: 16pt; min-height: 16pt; max-height: 16pt;
        text-align: left; padding-right: 16pt; white-space: nowrap;
        border-left: 1pt solid black; border-top: 1pt solid black;
    }
    tr:hover {background: #BFB !important}
    th {background: #DDD; position: sticky; position: -webkit-sticky;}
    th.level0 {top: 0; border-bottom: 1pt solid black;}
    th.level1 {top: 17pt; border-top: 0; border-bottom: 1pt solid black;}
    td.row0 {border-top: 0 !important;}
    td {background: #FFFFFF99;}
    /* SHADEDCOLS */
</style>"""

DF_CSS_SHADING = "    td.col{} {{background: #E3E3E399 !important;}}"


def get_dataframe_css(columns):
    """Add shading of 'info' columns to default DF_CSS"""
    shading_css_lines = []
    for i, col in enumerate(columns.get_level_values(0)):
        if col == "info":
            shading_css_lines.append(DF_CSS_SHADING.format(i))
        else:
            break
    if shading_css_lines:
        return sub(
            r'    \/\* SHADEDCOLS \*\/', "\n".join(shading_css_lines), DF_CSS,
        )
    else:
        return DF_CSS


def annotated_cols(dataframe, sep):
    """Format (multi)-columns of dataframe with '#' prepended"""
    return sub(
        r'^', "#", sub(
            r'\n(.)', r'\n#\1',
            dataframe.columns.to_frame().T.to_csv(sep=sep, **DF_KWS),
        ),
    )


def color_bool(x):
    """Highlight True in green and False in pale red, NA in lightgray italics, keep everything else as-is"""
    if x == True:
        return "color: green"
    elif x == False:
        return "color: #FAA"
    elif isnull(x):
        return "color: #BBB; font-style: italic;"
    else:
        return ""


def display_dataframe(df, fmt):
    """Display dataframe with specified format"""
    if fmt == "tsv":
        content = annotated_cols(df, sep="\t") + df.to_csv(sep="\t", **DF_KWS)
        mimetype = "text/plain"
    elif fmt == "csv":
        content = annotated_cols(df, sep=",") + df.to_csv(sep=",", **DF_KWS)
        mimetype = "text/plain"
    elif fmt == "html":
        content = get_dataframe_css(df.columns) + (
            df.style.format(None, na_rep="NA").applymap(color_bool)
            .hide_index().render()
        )
        mimetype = "text/html"
    else:
        raise NotImplementedError("fmt='{}'".format(fmt))
    return Response(content, mimetype=mimetype)


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
