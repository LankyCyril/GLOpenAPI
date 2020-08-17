from os.path import join, split, abspath
from flask import Response
from pandas import DataFrame, isnull
from re import sub
from genefab3.utils import map_replace
from genefab3.config import ASSAY_METADATALIKES
from genefab3.flask.parser import parse_request, parse_meta_arguments
from genefab3.flask.parser import REMOVER


DF_KWS = dict(index=False, header=False, na_rep="NA")

DF_STATIC_CSS = """<style>
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

DF_STATIC_CSS_SHADING = "    td.col{} {{background: #E3E3E399 !important;}}"

DF_DYNAMIC_HTML_FILE = join(
    split(split(split(abspath(__file__))[0])[0])[0], "html/dynamic-df.html",
)
with open(DF_DYNAMIC_HTML_FILE) as html:
    DF_DYNAMIC_HTML = html.read()


def annotated_cols(dataframe, sep):
    """Format (multi)-columns of dataframe with '#' prepended"""
    return sub(
        r'^', "#", sub(
            r'\n(.)', r'\n#\1',
            dataframe.columns.to_frame().T.to_csv(sep=sep, **DF_KWS),
        ),
    )


def get_static_dataframe_css(columns):
    """Add shading of 'info' columns to default DF_STATIC_CSS"""
    shading_css_lines = []
    for i, col in enumerate(columns.get_level_values(0)):
        if col == "info":
            shading_css_lines.append(DF_STATIC_CSS_SHADING.format(i))
        else:
            break
    if shading_css_lines:
        return sub(
            r'    \/\* SHADEDCOLS \*\/', "\n".join(shading_css_lines),
            DF_STATIC_CSS,
        )
    else:
        return DF_STATIC_CSS


def static_color_bool(x):
    """Highlight True in green and False in pale red, NA in lightgray italics, keep everything else as-is"""
    if x == True:
        return "color: green"
    elif x == False:
        return "color: #FAA"
    elif isnull(x):
        return "color: #BBB; font-style: italic;"
    else:
        return ""


def build_url(context, target_view=None, drop=set()):
    """Rebuild URL from request, alter based on `replace` and `drop`"""
    if target_view is None:
        url_components = [context.view + "?"]
    else:
        url_components = [target_view + "?"]
    for arg in context.args:
        if arg not in drop:
            for value in context.args.getlist(arg):
                if value == "":
                    url_components.append(arg+"&")
                else:
                    url_components.append("{}={}&".format(arg, value))
    return "".join(url_components)


def get_dynamic_glds_formatter(context):
    """Get SlickGrid formatter for column 'accession'"""
    mask = "columns[0].formatter={}; columns[0].defaultFormatter={};"
    formatter_mask = """columns[0].formatter=function(r,c,v,d,x){{
        return "<a href='{}select="+escape(v)+"'>"+v+"</a>";
    }};"""
    formatter = formatter_mask.format(build_url(context, drop={"select"}))
    return mask.format(formatter, formatter)


def get_dynamic_assay_formatter(context, shortnames):
    """Get SlickGrid formatter for column 'assay name'"""
    mask = "columns[1].formatter={}; columns[1].defaultFormatter={};"
    formatter_mask = """function(r,c,v,d,x){{
        return "<a href='{}select="+data[r]["{}"]+":"+escape(v)+"'>"+v+"</a>";
    }};"""
    formatter = formatter_mask.format(
        build_url(context, "/samples/", drop={"select"}),
        shortnames[0],
    )
    return mask.format(formatter, formatter)


def get_dynamic_meta_formatter(context, i, meta, meta_name):
    """Get SlickGrid formatter for meta column"""
    mask = "columns[{}].formatter={}; columns[{}].defaultFormatter={};"
    if context.view == "/assays/":
        formatter_mask = """function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : ((v == "False")
            ? "<font style='color:#FAA'>"+v+"</font>"
            : "<a href='{}{}="+escape("{}")+"' style='color:green'>"+v+"</a>");
        }};"""
    else:
        formatter_mask = """function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : "<a href='{}{}:"+escape("{}")+"="+escape(v)+"'>"+v+"</a>";
        }};"""
    formatter = formatter_mask.format(build_url(context), meta, meta_name)
    return mask.format(i, formatter, i, formatter)


def get_dynamic_dataframe_formatters(df, context, shortnames):
    """Get SlickGrid formatters for columns"""
    formatters = []
    if df.columns[0] == ("info", "accession"):
        formatters.append(get_dynamic_glds_formatter(context))
    if df.columns[1] == ("info", "assay name"):
        formatters.append(get_dynamic_assay_formatter(context, shortnames))
    for i, (meta, meta_name) in enumerate(df.columns):
        if meta in ASSAY_METADATALIKES:
            formatters.append(
                get_dynamic_meta_formatter(context, i, meta, meta_name)
            )
    return "\n".join(formatters)


def get_dynamic_twolevel_dataframe_removers():
    """Get SlickGrid column removers"""
    return """var ci = 0;
    $(".slick-header-sortable").each(function () {
        var meta = columns[ci].columnGroup, name = columns[ci].name;
        if (meta !== "info") {
            $(this).append(
                "&nbsp;<a class='remover' href='"+
                window.location.href.replace(/#+$/g, "")+
                "&hide="+meta+":"+escape(name)+"'>&times;</a>"
            );
        };
        ci += 1;
    });"""


def get_select_query_explanation(cqs_list):
    """Generate human-friendly explanation of passed query '&select='"""
    select_mask = "<li><tt>&select={}</tt><br>list entries in {}</li>"
    aa_pairs = []
    explanations = []
    for cqs in cqs_list:
        accession, assay_name = cqs["accession"], cqs.get("assay name", None)
        if assay_name:
            aa_pairs.append("{}:{}".format(accession, assay_name))
            explanations.append(
                'assay "{}" from dataset "{}"'.format(assay_name, accession),
            )
        else:
            aa_pairs.append(accession)
            explanations.append('dataset "{}"'.format(accession))
    return select_mask.format("|".join(aa_pairs), ", or ".join(explanations))


def get_remover_query_explanation(key, value, meta, fields):
    """Generate human-friendly explanation of passed query '&hide='"""
    return '<li><tt>&{}={}</tt><br>remove {} column of "{}"</li>'.format(
        key, value, meta, next(iter(fields)),
    )


def get_meta_query_explanation(key, value, meta, query):
    """Generate human-friendly explanation of passed meta query"""
    if value == "":
        kv_pair = key
        explanation = "list all {} for all entries".format(key)
    else:
        kv_pair = "{}={}".format(key, value)
        head, tail = next(iter(query.items()))
        if head == "$or":
            mask = "list entries that have {}: {}"
            explanation = mask.format(
                meta, ", ".join(
                    '"{}"'.format(next(iter(v))) for v in tail
                )
            )
        elif "$in" in tail:
            mask = "list entries where {} of {} are one of: {}"
            explanation = mask.format(
                meta, head,
                ", or ".join('"{}"'.format(v) for v in tail["$in"])
            )
        else:
            explanation = "<i>unexplained</i>"
    return "<li><tt>&{}</tt><br>{}</li>".format(kv_pair, explanation)


def get_query_explanation(context):
    """Generate human-friendly explanation of passed query"""
    view_mask = "<li><tt>{}?</tt><br>view {}</li>"
    explanations = [
        view_mask.format(context.view, context.view.strip("/")),
        get_select_query_explanation(context.queries["select"].get("$or", [])),
    ]
    for key in sorted(context.args):
        for value in sorted(set(context.args.getlist(key))):
            for kind, meta, fields, query in parse_meta_arguments(key, {value}):
                if kind != REMOVER:
                    explanations.append(
                        get_meta_query_explanation(key, value, meta, query),
                    )
                elif kind == REMOVER:
                    explanations.append(
                        get_remover_query_explanation(key, value, meta, fields),
                    )
    return "<br>".join(explanations)


def get_dynamic_twolevel_dataframe_html(df, context, frozen=0):
    """Display dataframe with two-level columns using SlickGrid"""
    shortnames = []
    def generate_short_names(*args):
        s, j = "", len(shortnames) + 1
        while j > 0:
            s, j = chr(((j % 26) or 26) + 96) + s, (j - 1) // 26
        shortnames.append(s)
        return s
    rowdata = (
        df.droplevel(0, axis=1).rename(generate_short_names, axis=1)
        .applymap(lambda x: "NA" if isnull(x) else str(x))
        .to_json(orient="records")
    )
    cdm = "{{id:'{}',field:'{}',columnGroup:'{}',name:'{}'}},"
    columndata = "\n".join(
        cdm.format(n, n, a, b) for (a, b), n in zip(df.columns, shortnames)
    )
    formatters = get_dynamic_dataframe_formatters(df, context, shortnames)
    if context.view in {"/assays/", "/samples/"}:
        removers = get_dynamic_twolevel_dataframe_removers()
    else:
        removers = ""
    return map_replace(
        DF_DYNAMIC_HTML, {
            "// FROZENCOLUMN": str(frozen), "// FORMATTERS": formatters,
            "// REMOVERS": removers,
            "HTMLINK": build_url(context, drop={"fmt"}) + "fmt=html",
            "CSVLINK": build_url(context, drop={"fmt"}) + "fmt=csv",
            "TSVLINK": build_url(context, drop={"fmt"}) + "fmt=tsv",
            "ASSAYSVIEW": build_url(context, "/assays/"),
            "SAMPLESVIEW": build_url(context, "/samples/"),
            "DATAVIEW": build_url(context, "/data/"),
            "// COLUMNDATA": columndata, "// ROWDATA": rowdata,
            "<!--QUERYEXPLANATION-->": get_query_explanation(context),
        }
    )


def get_dynamic_threelevel_dataframe_html(df, context):
    """Squash two top levels of dataframe columns and display as two-level"""
    def renamer_generator():
        for l0, l1, _ in df.columns:
            yield "{}:{}".format(l0, l1)
    renamer = renamer_generator()
    def renamer_wrapper(*args):
        return next(renamer)
    return get_dynamic_twolevel_dataframe_html(
        df.droplevel(0, axis=1).rename(renamer_wrapper, axis=1, level=0),
        context, frozen="undefined",
    )


def get_dynamic_dataframe_html(df, context):
    """Display dataframe using SlickGrid"""
    if df.columns.nlevels == 2:
        return get_dynamic_twolevel_dataframe_html(df, context)
    elif df.columns.nlevels == 3:
        return get_dynamic_threelevel_dataframe_html(df, context)
    else:
        raise NotImplementedError("Dataframe with {} column levels".format(
            df.columns.nlevels,
        ))


def display_dataframe(df, context):
    """Display dataframe with specified format"""
    fmt = context.args.get("fmt", "tsv")
    if fmt == "tsv":
        content = annotated_cols(df, sep="\t") + df.to_csv(sep="\t", **DF_KWS)
        mimetype = "text/plain"
    elif fmt == "csv":
        content = annotated_cols(df, sep=",") + df.to_csv(sep=",", **DF_KWS)
        mimetype = "text/plain"
    elif fmt == "html":
        content = get_static_dataframe_css(df.columns) + (
            df.style.format(None, na_rep="NA").applymap(static_color_bool)
            .hide_index().render()
        )
        mimetype = "text/html"
    elif fmt in {"interactive", "browser"}:
        content = get_dynamic_dataframe_html(df, context)
        mimetype = "text/html"
    else:
        raise NotImplementedError("fmt='{}'".format(fmt))
    return Response(content, mimetype=mimetype)


def display(obj, context):
    """Dispatch object and trailing request arguments to display handler"""
    if isinstance(obj, DataFrame):
        return display_dataframe(obj, context)
    else:
        raise NotImplementedError(
            "Display of {}".format(str(type(obj).strip("<>")))
        )


def displayable(db, getter, kwargs, request):
    """Wrapper for data retrieval and display"""
    context = parse_request(request)
    return display(getter(db, **kwargs, context=context), context)
