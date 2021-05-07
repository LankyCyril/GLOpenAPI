from pandas import DataFrame, MultiIndex
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import lru_cache
from pathlib import Path
from genefab3.common.logger import GeneFabLogger
from genefab3.api.renderers.PlaintextDataFrameRenderers import get_index_and_columns
from json import dumps
from genefab3.common.utils import map_replace
from flask import Response


def _assert_type(obj, nlevels):
    """Check validity of `obj` for converting as a multi-column-level dataframe"""
    passed_nlevels = getattr(getattr(obj, "columns", None), "nlevels", 0)
    if (not isinstance(obj, DataFrame)) or (passed_nlevels != nlevels):
        msg = "Data cannot be represented as an interactive dataframe"
        _kw = dict(type=type(obj).__name__, nlevels=passed_nlevels)
        raise GeneFabConfigurationException(msg, **_kw)


@lru_cache(maxsize=None)
def _get_browser_html():
    """Return text of HTML template"""
    return (Path(__file__).parent / "dataframe.html").read_text()


def build_url(context, target_view=None, drop=set()):
    """Rebuild URL from request, alter based on `replace` and `drop`"""
    return "".join(sum((
        [f"{arg}={v}&" if v else f"{arg}&" for v in values]
        for arg, values in context.complete_kwargs.items() if arg not in drop),
        [context.url_root.rstrip("/")+"/", (target_view or context.view), "/?"],
    ))


SQUASHED_PREHEADER_CSS = """
.slick-preheader-panel .slick-header-column {font-size: 9pt; line-height: .8}
.slick-preheader-panel .slick-column-name {position: relative; top: -1pt}
"""


def get_view_dependent_links(obj, context):
    """Add CLS/GCT links to samples and data views, respectively"""
    if getattr(obj, "cls_valid", None) is True:
        url = build_url(context, drop={"format"}) + "format=cls"
        return f", <a style='color:#D10' href='{url}'>cls</a>"
    elif getattr(obj, "gct_valid", None) is True:
        url = build_url(context, drop={"format"}) + "format=gct"
        return f", <a style='color:#D10' href='{url}'>gct</a>"
    else:
        return ""


def twolevel(obj, context, indent=None, frozen=0, col_fill="*", squash_preheader=False):
    """Display dataframe with two-level columns using SlickGrid"""
    _assert_type(obj, nlevels=2)
    title_postfix = f"{context.view} {context.complete_kwargs}"
    GeneFabLogger().info("HTML: converting DataFrame into interactive table")
    index_and_columns = get_index_and_columns(obj, col_fill=col_fill)
    columndata = dumps(index_and_columns.to_list(), separators=(",", ":"))
    rowdata = obj.reset_index().to_json(orient="values")
    content = map_replace(_get_browser_html(), {
        "$APPNAME": f"{context.app_name}: {title_postfix}",
        "$SQUASH_PREHEADER": SQUASHED_PREHEADER_CSS if squash_preheader else "",
        "$CSVLINK": build_url(context, drop={"format"}) + "format=csv",
        "$TSVLINK": build_url(context, drop={"format"}) + "format=tsv",
        "$JSONLINK": build_url(context, drop={"format"}) + "format=json",
        "$VIEWDEPENDENTLINKS": get_view_dependent_links(obj, context),
        "$ASSAYSVIEW": build_url(context, "assays"),
        "$SAMPLESVIEW": build_url(context, "samples"),
        "$DATAVIEW": build_url(context, "data"),
        "$COLUMNDATA": columndata,
        "$ROWDATA": rowdata,
        "$CONTEXTURL": build_url(context),
        "$FORMATTERS": "",
        "$FROZENCOLUMN": "undefined" if frozen is None else str(frozen),
    })
    return Response(content, mimetype="text/html")


def threelevel(obj, context, indent=None):
    """Squash two top levels of dataframe columns and display as two-level"""
    _assert_type(obj, nlevels=3)
    if len(obj.columns):
        obj.columns = MultiIndex.from_tuples((
            (f"{a}<br>{b}", c) for (a, b, c) in obj.columns
        ))
    else:
        obj.columns = MultiIndex.from_tuples([("*", "*")])[:0]
    return twolevel(
        obj, context=context, col_fill="*<br>*", squash_preheader=True,
    )
