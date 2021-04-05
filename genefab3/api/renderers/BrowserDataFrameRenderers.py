from pandas import DataFrame
from genefab3.common.exceptions import GeneFabConfigurationException
from collections.abc import Iterable
from pandas import isnull
from functools import lru_cache
from pathlib import Path
from genefab3.common.utils import map_replace
from flask import Response


def _assert_type(obj, nlevels):
    """Check validity of `obj` for converting as a multi-column-level dataframe"""
    passed_nlevels = getattr(getattr(obj, "columns", None), "nlevels", 0)
    if (not isinstance(obj, DataFrame)) or (passed_nlevels != nlevels):
        msg = "Data cannot be represented as an interactive dataframe"
        _kw = {"type": type(obj).__name__, "nlevels": passed_nlevels}
        raise GeneFabConfigurationException(msg, **_kw)


def _na_repr(x):
    """For format=browser, convert empty entries to 'NA'"""
    if isinstance(x, Iterable):
        return "NA" if (hasattr(x, "__len__") and (len(x) == 0)) else str(x)
    else:
        return "NA" if isnull(x) else str(x)


@lru_cache(maxsize=None)
def _get_browser_html():
    """Return text of HTML template"""
    return (Path(__file__).parent / "dataframe.html").read_text()


def build_url(context, target_view=None, drop=set()):
    """Rebuild URL from request, alter based on `replace` and `drop`"""
    parts = ["/" + (target_view or context.view) + "/?"]
    for arg, values in context.complete_kwargs.items():
        if arg not in drop:
            parts.extend([f"{arg}={v}&" if v else f"{arg}&" for v in values])
    return "".join(parts)


def get_browser_glds_formatter(context):
    """Get SlickGrid formatter for column 'accession'"""
    url = build_url(context, drop={"from"})
    _fr = f"""columns[0].formatter=function(r,c,v,d,x){{
        return "<a href='{url}from="+escape(v)+"'>"+v+"</a>";}};"""
    return f"columns[0].formatter={_fr}; columns[0].defaultFormatter={_fr};"


def get_browser_assay_formatter(context, shortnames):
    """Get SlickGrid formatter for column 'assay name'"""
    url, s = build_url(context, "samples", drop={"from"}), shortnames[0]
    _fr = f"""function(r,c,v,d,x){{
        return "<a href='{url}from="+data[r]["{s}"]+"."+escape(v)+"'>"+v+"</a>";
    }};"""
    return f"columns[1].formatter={_fr}; columns[1].defaultFormatter={_fr};"


def get_browser_meta_formatter(context, i, category, subkey, target):
    """Get SlickGrid formatter for meta column"""
    url = build_url(context)
    if context.view == "assays":
        _fr = f"""function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : ((v == "False")
            ? "<font style='color:#FAA'>"+v+"</font>"
            : "<a href='{url}"+escape("{category}.{subkey}.{target}")+
                "' style='color:green'>"+v+"</a>");
        }};"""
    else:
        _fr = f"""function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : "<a href='{url}"+escape("{category}.{subkey}.{target}")+
                "="+escape(v)+"'>"+v+"</a>";
        }};"""
    return f"columns[{i}].formatter={_fr}; columns[{i}].defaultFormatter={_fr};"


def iter_browser_formatters(obj, context, shortnames):
    """Get SlickGrid formatters for columns"""
    if obj.columns[0] == ("info", "accession"):
        yield get_browser_glds_formatter(context)
    if (len(obj.columns) > 1) and (obj.columns[1] == ("info", "assay")):
        yield get_browser_assay_formatter(context, shortnames)
    for i, (key, target) in enumerate(obj.columns):
        cat, *fields = key.split(".")
        if (len(fields) == 1) and (cat not in {"info", "file"}):
            yield get_browser_meta_formatter(context, i, cat, fields[0], target)


def twolevel(obj, context, indent=None, frozen=0):
    """Display dataframe with two-level columns using SlickGrid"""
    _assert_type(obj, nlevels=2)
    shortnames = []
    def _WITH_SIDE_EFFECT_generate_short_names(*args):
        s, j = "", len(shortnames) + 1
        while j > 0:
            s, j = chr(((j % 26) or 26) + 96) + s, (j - 1) // 26
        shortnames.append(s)
        return s
    rowdata = (
        obj.droplevel(0, axis=1)
        .rename(_WITH_SIDE_EFFECT_generate_short_names, axis=1)
        .applymap(_na_repr)
        .to_json(orient="records")
    )
    columndata = "\n".join(
        f"{{id:'{n}',field:'{n}',columnGroup:'{a}',name:'{b}'}},"
        for (a, b), n in zip(obj.columns, shortnames)
    )
    formatters = "\n".join(iter_browser_formatters(obj, context, shortnames))
    content = map_replace(
        _get_browser_html(), {
            "// FROZENCOLUMN": "undefined" if frozen is None else str(frozen),
            "// FORMATTERS": formatters,
            "HTMLINK": build_url(context, drop={"format"}) + "format=html",
            "CSVLINK": build_url(context, drop={"format"}) + "format=csv",
            "TSVLINK": build_url(context, drop={"format"}) + "format=tsv",
            "JSONLINK": build_url(context, drop={"format"}) + "format=json",
            "ASSAYSVIEW": build_url(context, "assays"),
            "SAMPLESVIEW": build_url(context, "samples"),
            "DATAVIEW": build_url(context, "data"),
            "// COLUMNDATA": columndata, "// ROWDATA": rowdata,
        }
    )
    return Response(content, mimetype="text/html")


def threelevel(obj, context, indent=None):
    """Squash two top levels of dataframe columns and display as two-level"""
    _assert_type(obj, nlevels=3)
    def renamer_generator(): # TODO can be refactored as inline
        for l0, l1, _ in obj.columns:
            yield f"{l0}\n{l1}"
    renamer = renamer_generator()
    def renamer_wrapper(*args):
        return next(renamer)
    return twolevel(
        obj.droplevel(0, axis=1).rename(renamer_wrapper, axis=1, level=0),
        context=context,
    )
