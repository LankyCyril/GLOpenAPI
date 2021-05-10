from genefab3.common.types import StreamedTable, StreamedAnnotationTable
from genefab3.common.exceptions import GeneFabConfigurationException
from pathlib import Path
from genefab3.common.logger import GeneFabLogger
from genefab3.api.renderers.PlaintextStreamedTableRenderers import _iter_json_chunks
from re import compile, escape


def _assert_type(obj, nlevels):
    """Check validity of `obj` for converting as a multi-column-level StreamedTable"""
    passed_nlevels = {len(column) for column in getattr(obj, "columns", [[]])}
    if (not isinstance(obj, StreamedTable)) or (passed_nlevels != {nlevels}):
        msg = "Data cannot be represented as an interactive table"
        _kw = dict(type=type(obj).__name__, nlevels=passed_nlevels)
        raise GeneFabConfigurationException(msg, **_kw)


def build_url(context, target_view=None, drop=set()):
    """Rebuild URL from request, alter based on `replace` and `drop`"""
    return "".join(sum((
        [f"{arg}={v}&" if v else f"{arg}&" for v in values]
        for arg, values in context.complete_kwargs.items() if arg not in drop),
        [context.url_root.rstrip("/")+"/", (target_view or context.view), "/?"],
    ))


def get_browser_glds_formatter(context, i):
    """Get SlickGrid formatter for column 'accession'"""
    url = build_url(context, drop={"id"})
    _fr = f"""function(r,c,v,d,x){{return "<a class='filter' "+
        "href='{url}id="+escape(v)+"'>"+v+"</a>";}}"""
    return f"columns[{i}].formatter = columns[{i}].defaultFormatter = {_fr};"


def get_browser_mixed_id_formatter(context, i, head):
    """Get SlickGrid formatter for column 'assay name'"""
    url = build_url(context, drop={"id"})
    _fr = f"""function(r,c,v,d,x){{return "<a class='filter' "+
        "href='{url}id="+{head}+"/"+escape(v)+"'>"+v+"</a>";}}"""
    return f"columns[{i}].formatter = columns[{i}].defaultFormatter = {_fr};"


def get_browser_file_formatter(context, i):
    """Get SlickGrid formatter for file column"""
    url = build_url(context, "data", drop={"format", "file.filename"})
    _fr = f"""function(r,c,v,d,x){{
        return (v === null) ? "<i style='color:#BBB'>"+v+"</i>" :
        "<a class='file' href='{url}file.filename="+escape(v)+"&format=raw'>"+
        v+"</a>";}}"""
    return f"columns[{i}].formatter = columns[{i}].defaultFormatter = {_fr};"


def get_browser_meta_formatter(context, i, head, target):
    """Get SlickGrid formatter for meta column"""
    _type = "assays" if (context.view == "assays") else "samples"
    _fr = f'function(r,c,v,d,x){{return fr_{_type}(v, "{head}.{target}")}}'
    return f"columns[{i}].formatter = columns[{i}].defaultFormatter = {_fr};"


def iterate_formatters(index_and_columns, context):
    """Get SlickGrid formatters for columns"""
    for i, (key, target) in enumerate(index_and_columns):
        if key == "id":
            if target == "accession":
                yield get_browser_glds_formatter(context, i)
            elif target == "assay name":
                head = "data[r][0]"
                yield get_browser_mixed_id_formatter(context, i, head)
            elif target == "sample name":
                head = 'data[r][0]+"/"+data[r][1]'
                yield get_browser_mixed_id_formatter(context, i, head)
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


def _iter_html_chunks(replacements):
    """Return list of lines of HTML template and subsitute variables with generated data"""
    pattern = compile(r'|'.join(map(escape, replacements.keys())))
    with open(Path(__file__).parent/"dataframe.html") as template:
        for line in template:
            match = pattern.search(line)
            if match:
                replacement = replacements[match.group()]
                if isinstance(replacement, str):
                    yield pattern.sub(replacement, line)
                else:
                    before, after = pattern.split(line, 1)
                    yield before
                    yield from replacement
                    yield after
            else:
                yield line


def twolevel(obj, context, indent=None, frozen=0, col_fill="*", squash_preheader=False):
    """Display StreamedTable with two-level columns using SlickGrid"""
    obj.move_index_boundary(to=0)
    _assert_type(obj, nlevels=2)
    title_postfix = f"{context.view} {context.complete_kwargs}"
    msg = "HTML: converting StreamedTable into interactive table"
    GeneFabLogger().info(msg)
    if isinstance(obj, StreamedAnnotationTable) and (context.view != "status"):
        formatters = iterate_formatters(obj.columns, context)
    else:
        formatters = []
    replacements = {
        "$APPNAME": f"{context.app_name}: {title_postfix}",
        "$SQUASH_PREHEADER": SQUASHED_PREHEADER_CSS if squash_preheader else "",
        "$CSVLINK": build_url(context, drop={"format"}) + "format=csv",
        "$TSVLINK": build_url(context, drop={"format"}) + "format=tsv",
        "$JSONLINK": build_url(context, drop={"format"}) + "format=json",
        "$VIEWDEPENDENTLINKS": get_view_dependent_links(obj, context),
        "$ASSAYSVIEW": build_url(context, "assays"),
        "$SAMPLESVIEW": build_url(context, "samples"),
        "$DATAVIEW": build_url(context, "data"),
        "$COLUMNDATA": _iter_json_chunks(d=obj.columns, n=obj.shape[1]),
        "$ROWDATA": _iter_json_chunks(d=obj.values, n=obj.shape[0]),
        "$CONTEXTURL": build_url(context),
        "$FORMATTERS": "\n".join(formatters),
        "$FROZENCOLUMN": "undefined" if frozen is None else str(frozen),
    }
    _iter_chained_formatted_chunks = lambda: _iter_html_chunks(replacements)
    return _iter_chained_formatted_chunks(), "text/html"


def threelevel(obj, context, indent=None):
    """Squash two top levels of StreamedTable columns and display as two-level"""
    # TODO
