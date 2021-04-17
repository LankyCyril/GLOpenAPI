from pandas import DataFrame, isnull
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import wraps, lru_cache
from collections.abc import Iterable
from pathlib import Path
from genefab3.common.utils import map_replace
from flask import Response


def _assert_type(obj, nlevels):
    """Check validity of `obj` for converting as a multi-column-level dataframe"""
    passed_nlevels = getattr(getattr(obj, "columns", None), "nlevels", 0)
    if (not isinstance(obj, DataFrame)) or (passed_nlevels != nlevels):
        msg = "Data cannot be represented as an interactive dataframe"
        _kw = dict(type=type(obj).__name__, nlevels=passed_nlevels)
        raise GeneFabConfigurationException(msg, **_kw)


def _na_repr_permissive_cache(f):
    """Custom memoizer for `_na_repr`, bypasses hashing if not Hashable"""
    cache = {}
    @wraps(f)
    def wrapper(x):
        try:
            k = hash(x)
        except TypeError:
            return f(x)
        if (k not in cache) and (len(cache) >= 4096):
            del cache[next(iter(cache.keys()))]
        return cache.setdefault(k, f(x))
    return wrapper


@_na_repr_permissive_cache
def _na_repr(x):
    """For format=browser, convert empty entries to 'NaN'"""
    if isinstance(x, Iterable):
        return "NaN" if (hasattr(x, "__len__") and (len(x) == 0)) else str(x)
    else:
        return "NaN" if isnull(x) else str(x)


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


def get_browser_glds_formatter(context, i):
    """Get SlickGrid formatter for column 'accession'"""
    url = build_url(context, drop={"from"})
    _fr = f"""function(r,c,v,d,x){{return "<a class='filter' "+
        "href='{url}from="+escape(v)+"'>"+v+"</a>";}};"""
    return f"columns[{i}].formatter={_fr}; columns[{i}].defaultFormatter={_fr};"


def get_browser_assay_formatter(context, i):
    """Get SlickGrid formatter for column 'assay name'"""
    url = build_url(context, "samples", drop={"from"})
    _fr = f"""function(r,c,v,d,x){{return "<a class='filter' "+
        "href='{url}from="+data[r][0]+"."+escape(v)+"'>"+v+"</a>";}};"""
    return f"columns[{i}].formatter={_fr}; columns[{i}].defaultFormatter={_fr};"


def get_browser_file_formatter(context, i):
    """Get SlickGrid formatter for file column"""
    url = build_url(context, "data", drop={"format", "file.filename"})
    _fr = f"""function(r,c,v,d,x){{
        return (v == "NaN") ? "<i style='color:#BBB'>"+v+"</i>" :
        "<a class='file' href='{url}file.filename="+escape(v)+"&format=raw'>"+
        v+"</a>";}};"""
    return f"columns[{i}].formatter={_fr}; columns[{i}].defaultFormatter={_fr};"


def get_browser_meta_formatter(context, i, head, target):
    """Get SlickGrid formatter for meta column"""
    url = build_url(context)
    if context.view == "assays":
        _fr = f"""function(r,c,v,d,x){{
            return (v == "NaN")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : ((v == "False")
            ? "<font style='color:#FAA'>"+v+"</font>"
            : "<a href='{url}"+escape("{head}.{target}")+
                "' style='color:green' class='filter'>"+v+"</a>");
        }};"""
    else:
        _fr = f"""function(r,c,v,d,x){{
            return (v == "NaN")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : "<a href='{url}"+escape("{head}.{target}")+
                "="+escape(v)+"' class='filter'>"+v+"</a>";
        }};"""
    return f"columns[{i}].formatter={_fr}; columns[{i}].defaultFormatter={_fr};"


def iterate_formatters(obj, context):
    """Get SlickGrid formatters for columns"""
    if context.view in {"assays", "samples"}:
        for i, (key, target) in enumerate(obj.columns):
            if key == "info":
                if target == "accession":
                    yield get_browser_glds_formatter(context, i)
                elif target == "assay":
                    yield get_browser_assay_formatter(context, i)
            elif (key, target, context.view) == ("file", "filename", "samples"):
                yield get_browser_file_formatter(context, i)
            else:
                category, *fields = key.split(".")
                head = f"{category}.{fields[0]}" if fields else category
                yield get_browser_meta_formatter(context, i, head, target)


SQUASHED_PREHEADER_CSS = """
.slick-preheader-panel .slick-header-column {font-size: 9pt; line-height: .8}
.slick-preheader-panel .slick-column-name {position: relative; top: -1pt}
"""


def get_view_dependent_links(context):
    """Add CLS/GCT links to samples and data views, respectively"""
    if context.view == "samples":
        url = build_url(context, drop={"format"}) + "format=cls"
        return f", <a style='color:#D10' href='{url}'>cls</a>"
    elif context.view == "data":
        url = build_url(context, drop={"format"}) + "format=gct"
        return f", <a style='color:#D10' href='{url}'>gct</a>"
    else:
        return ""


def twolevel(obj, context, indent=None, frozen=0, use_formatters=True, squash_preheader=False):
    """Display dataframe with two-level columns using SlickGrid"""
    _assert_type(obj, nlevels=2)
    title_postfix = f"{context.view.capitalize()} {context.complete_kwargs}"
    values_json = obj.applymap(_na_repr).to_json(orient="split", index=False)
    formatters = iterate_formatters(obj, context)
    content = map_replace(_get_browser_html(), {
        "$APPNAME": f"{context.app_name}: {title_postfix}",
        "$SQUASH_PREHEADER": SQUASHED_PREHEADER_CSS if squash_preheader else "",
        "$CSVLINK": build_url(context, drop={"format"}) + "format=csv",
        "$TSVLINK": build_url(context, drop={"format"}) + "format=tsv",
        "$JSONLINK": build_url(context, drop={"format"}) + "format=json",
        "$VIEWDEPENDENTLINKS": get_view_dependent_links(context),
        "$ASSAYSVIEW": build_url(context, "assays"),
        "$SAMPLESVIEW": build_url(context, "samples"),
        "$DATAVIEW": build_url(context, "data"),
        "$VALUES": values_json,
        "$FORMATTERS": "\n".join(formatters) if use_formatters else "",
        "$FROZENCOLUMN": "undefined" if frozen is None else str(frozen),
    })
    return Response(content, mimetype="text/html")


def threelevel(obj, context, indent=None):
    """Squash two top levels of dataframe columns and display as two-level"""
    _assert_type(obj, nlevels=3)
    squashed_names = (f"{l0}<br>{l1}" for l0, l1, _ in obj.columns)
    renamer = lambda *a: next(squashed_names)
    return twolevel(
        obj.droplevel(0, axis=1).rename(renamer, axis=1, level=0),
        context=context, use_formatters=False, squash_preheader=True,
    )
