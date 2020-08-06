from os.path import join, split, abspath
from flask import Response
from pandas import DataFrame, isnull
from re import sub


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

with open(join(split(split(abspath(__file__))[0])[0], "html/df.html")) as html:
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


def build_url(request, replace={}, drop=set()):
    """Rebuild URL from request, alter based on `replace` and `drop`"""
    base_url = request.base_url
    for old, new in replace.items():
        base_url = base_url.replace(old, new)
    url_components = [base_url + "?"]
    for arg in request.args:
        if arg not in drop:
            for value in request.args.getlist(arg):
                if value == "":
                    url_components.append(arg+"&")
                else:
                    url_components.append("{}={}&".format(arg, value))
    return "".join(url_components)


def get_dynamic_glds_formatter(request):
    """Get SlickGrid formatter for column 'accession'"""
    mask = "columns[0].formatter={}; columns[0].defaultFormatter={};"
    formatter_mask = """columns[0].formatter=function(r,c,v,d,x){{
        return "<a href='{}select="+v+"'>"+v+"</a>";
    }};"""
    formatter = formatter_mask.format(build_url(request, drop={"select"}))
    return mask.format(formatter, formatter)


def get_dynamic_assay_formatter(request, shortnames):
    """Get SlickGrid formatter for column 'assay name'"""
    mask = "columns[1].formatter={}; columns[1].defaultFormatter={};"
    formatter_mask = """function(r,c,v,d,x){{
        return "<a href='{}select="+data[r]["{}"]+":"+v+"'>"+v+"</a>";
    }};"""
    formatter = formatter_mask.format(
        build_url(request, replace={"/assays/": "/samples/"}, drop={"select"}),
        shortnames[0],
    )
    return mask.format(formatter, formatter)


def get_dynamic_twolevel_dataframe_html(df, request):
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
    if all(df.columns.get_level_values(1)[:2] == ["accession", "assay name"]):
        formatters = "\n".join([
            get_dynamic_glds_formatter(request),
            get_dynamic_assay_formatter(request, shortnames),
        ])
    else:
        formatters = ""
    htmlink = build_url(request, drop={"fmt"}) + "fmt=html"
    csvlink = build_url(request, drop={"fmt"}) + "fmt=csv"
    tsvlink = build_url(request, drop={"fmt"}) + "fmt=tsv"
    return (
        DF_DYNAMIC_HTML
        .replace("// FORMATTERS", formatters).replace("HTMLINK", htmlink)
        .replace("CSVLINK", csvlink).replace("TSVLINK", tsvlink)
        .replace("// COLUMNDATA", columndata).replace("// ROWDATA", rowdata)
    )


def get_dynamic_dataframe_html(df, request):
    """Display dataframe using SlickGrid"""
    if df.columns.nlevels == 2:
        return get_dynamic_twolevel_dataframe_html(df, request)
    else:
        raise NotImplementedError("Non-two-level interactive dataframe")


def display_dataframe(df, request):
    """Display dataframe with specified format"""
    fmt = request.args.get("fmt", "tsv")
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
    elif fmt == "interactive":
        content = get_dynamic_dataframe_html(df, request)
        mimetype = "text/html"
    else:
        raise NotImplementedError("fmt='{}'".format(fmt))
    return Response(content, mimetype=mimetype)


def display(obj, request):
    """Dispatch object and trailing request arguments to display handler"""
    if isinstance(obj, DataFrame):
        return display_dataframe(obj, request)
    else:
        raise NotImplementedError(
            "Display of {}".format(str(type(obj).strip("<>")))
        )
