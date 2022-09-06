from glopenapi.common.utils import space_quote, repr_quote
from re import compile, escape
from pathlib import Path
from glopenapi.common.exceptions import GLOpenAPILogger
from glopenapi.api.renderers.types import StreamedAnnotationTable
from glopenapi.api.renderers.PlaintextStreamedTableRenderers import _iter_json_chunks
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from glopenapi.common.exceptions import GLOpenAPIDisabledException


def build_url(context, target_view=None, drop=set()):
    """Rebuild URL from request, alter based on `replace` and `drop`"""
    path = context.url_root.rstrip("/") + "/" + (target_view or context.view)
    return path + "/?" + "".join(
        f"{space_quote(arg)}={space_quote(v)}&" if v else f"{space_quote(arg)}&"
        for arg, values in context.complete_kwargs.items() if arg not in drop
        for v in values
    )


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


def _iter_html_chunks(template_file, replacements):
    """Return list of lines of HTML template and subsitute variables with generated data"""
    # TODO: this is becoming generic and needs to be moved elsewhere and renamed
    pattern = compile(r'|'.join(map(escape, replacements.keys())))
    with open(template_file) as template:
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


SQUASHED_PREHEADER_CSS = """
    <link rel='stylesheet' href='$URL_ROOT/css/dataframe-squash.css'/>
"""


def twolevel(obj, context, squash_preheader=False, frozen=0, indent=None):
    """Display StreamedTable with two-level columns using SlickGrid"""
    GLOpenAPILogger.info("JS: converting StreamedTable to interactive table")
    obj.move_index_boundary(to=0)
    def content():
        is_annotation_table = isinstance(obj, StreamedAnnotationTable)
        if is_annotation_table and (context.view != "status"):
            formatters = iterate_formatters(obj.columns, context)
        else:
            formatters = []
        if squash_preheader:
            columns = ((f"{c[0]}<br>{c[1]}", c[2]) for c in obj.columns)
            preheader_css = SQUASHED_PREHEADER_CSS
        else:
            columns, preheader_css = obj.columns, ""
        replacements = {
            "$SQUASH_PREHEADER": preheader_css,
            "$VIEWDEPENDENTLINKS": get_view_dependent_links(obj, context),
            "$COLUMNDATA": _iter_json_chunks(data=columns, length=obj.shape[1]),
            "$ROWDATA": _iter_json_chunks(data=obj.values, length=obj.shape[0]),
            "$CONTEXTURL": build_url(context),
            "$FORMATTERS": "\n".join(formatters),
            "$FROZENCOLUMN": "undefined" if frozen is None else str(frozen),
        }
        template_file = Path(__file__).parent / "dataframe.js"
        yield from _iter_html_chunks(template_file, replacements)
    return content, "application/javascript"


def _get_passed_nlevels(obj):
    set_of_passed_nlevels = {len(c) for c in getattr(obj, "columns", [[]])}
    if not set_of_passed_nlevels:
        obj.move_index_boundary(to=0)
        set_of_passed_nlevels = {len(c) for c in getattr(obj, "columns", [[]])}
    if set_of_passed_nlevels == {2}:
        return 2
    elif set_of_passed_nlevels == {3}:
        return 3
    else:
        msg = "Data cannot be represented as an interactive table"
        _kw = dict(type=type(obj).__name__, nlevels=set_of_passed_nlevels)
        raise GLOpenAPIConfigurationException(msg, **_kw)


def javascript(obj, context, indent=None):
    """Force two-level columns in StreamedTable and provide objects to be used in SlickGrid"""
    # TODO: use AJAX or something like that
    return twolevel(
        obj, context, indent=indent,
        squash_preheader=(_get_passed_nlevels(obj)==3),
    )


def html(obj, context, indent=None):
    """Substitute output of `javascript` and render using SlickGrid"""
    # TODO: we're evaluating `obj` twice, once for `html` and once for `javascript`, there's no reason
    raise GLOpenAPIDisabledException(
        "Interactive format temporarily disabled", format=context.format,
    )
    def content():
        title_postfix = repr_quote(f"{context.view} {context.complete_kwargs}")
        replacements = {
            "$APPNAME": f"{context.app_name}: {title_postfix}",
            "$URL_ROOT": context.url_root,
            "$CSVLINK": build_url(context, drop={"format"}) + "format=csv",
            "$TSVLINK": build_url(context, drop={"format"}) + "format=tsv",
            "$JSONLINK": build_url(context, drop={"format"}) + "format=json",
            "$ASSAYSVIEW": build_url(context, "assays"),
            "$SAMPLESVIEW": build_url(context, "samples"),
            "$DATAVIEW": build_url(context, "data"),
            "$SQUASH_PREHEADER": (
                SQUASHED_PREHEADER_CSS if (_get_passed_nlevels(obj)==3) else ""
            ),
            "$JAVASCRIPT_PATH": (
                build_url(context, drop={"format"}) + "format=javascript",
            ),
        }
        template_file = Path(__file__).parent / "dataframe.html"
        yield from _iter_html_chunks(template_file, replacements)
    return content, "text/html"
