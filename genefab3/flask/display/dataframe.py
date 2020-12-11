from re import sub
from os import path
from collections.abc import Iterable
from pandas import isnull
from genefab3.flask.display.formatters import get_browser_formatters, build_url
from genefab3.common.utils import map_replace
from flask import Response
from genefab3.common.exceptions import GeneLabFormatException


DF_KWS = dict(index=False, header=False, na_rep="NA")


def annotate_cols(dataframe, sep):
    """Format (multi)-columns of dataframe with '#' prepended"""
    return sub(
        r'^', "#", sub(
            r'\n(.)', r'\n#\1',
            dataframe.columns.to_frame().T.to_csv(sep=sep, **DF_KWS),
        ),
    )


def walk_up(from_path, n_steps):
    if n_steps >= 1:
        return walk_up(path.split(from_path)[0], n_steps-1)
    else:
        return from_path


def get_browser_html(): # TODO in prod: make HTML template static / preload on app start
    filename = path.join(
        walk_up(path.abspath(__file__), 4), "html/slick-df.html",
    )
    with open(filename) as html:
        return html.read()


def na_repr(x):
    """For format=browser, convert empty entries to 'NA'"""
    if isinstance(x, Iterable):
        if hasattr(x, "__len__") and (len(x) == 0):
            return "NA"
        else:
            return str(x)
    elif isnull(x):
        return "NA"
    else:
        return str(x)


def get_browser_dataframe_twolevel(df, context, frozen=0):
    """Display dataframe with two-level columns using SlickGrid"""
    shortnames = []
    def generate_short_names(*args):
        s, j = "", len(shortnames) + 1
        while j > 0:
            s, j = chr(((j % 26) or 26) + 96) + s, (j - 1) // 26
        shortnames.append(s)
        return s
    rowdata = (
        df.droplevel(0, axis=1)
        .rename(generate_short_names, axis=1)
        .applymap(na_repr)
        .to_json(orient="records")
    )
    cdm = "{{id:'{}',field:'{}',columnGroup:'{}',name:'{}'}},"
    columndata = "\n".join(
        cdm.format(n, n, a, b) for (a, b), n in zip(df.columns, shortnames)
    )
    formatters = get_browser_formatters(df, context, shortnames)
    return map_replace(
        get_browser_html(), {
            "// FROZENCOLUMN": "undefined" if frozen is None else str(frozen),
            "// FORMATTERS": formatters,
            "HTMLINK": build_url(context, drop={"format"}) + "format=html",
            "CSVLINK": build_url(context, drop={"format"}) + "format=csv",
            "TSVLINK": build_url(context, drop={"format"}) + "format=tsv",
            "JSONLINK": build_url(context, drop={"format"}) + "format=json",
            "ASSAYSVIEW": build_url(context, "/assays/"),
            "SAMPLESVIEW": build_url(context, "/samples/"),
            "DATAVIEW": build_url(context, "/data/"),
            "// COLUMNDATA": columndata, "// ROWDATA": rowdata,
        }
    )


def get_browser_dataframe_threelevel(df, context):
    """Squash two top levels of dataframe columns and display as two-level"""
    def renamer_generator():
        for l0, l1, _ in df.columns:
            yield "{}:{}".format(l0, l1)
    renamer = renamer_generator()
    def renamer_wrapper(*args):
        return next(renamer)
    return get_browser_dataframe_twolevel(
        df.droplevel(0, axis=1).rename(renamer_wrapper, axis=1, level=0),
        context,
    )


def get_browser_dataframe(df, context):
    """Display dataframe using SlickGrid"""
    if df.columns.nlevels == 2:
        return get_browser_dataframe_twolevel(df, context)
    elif df.columns.nlevels == 3:
        return get_browser_dataframe_threelevel(df, context)
    else:
        raise NotImplementedError("Dataframe with {} column levels".format(
            df.columns.nlevels,
        ))


def render_dataframe(df, context):
    """Display dataframe with specified format"""
    if context.kwargs["format"] == "tsv":
        content = annotate_cols(df, sep="\t") + df.to_csv(sep="\t", **DF_KWS)
        mimetype = "text/plain"
    elif context.kwargs["format"] == "csv":
        content = annotate_cols(df, sep=",") + df.to_csv(sep=",", **DF_KWS)
        mimetype = "text/plain"
    elif context.kwargs["format"] in {"interactive", "browser"}:
        content = get_browser_dataframe(df, context)
        mimetype = "text/html"
    else:
        raise GeneLabFormatException(
            "Unknown format", format=context.kwargs["format"],
        )
    return Response(content, mimetype=mimetype)
