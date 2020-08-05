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

DF_DYNAMIC_FORMATTER_GLDS_MASK = """columns[0].formatter=function(r,c,v,d,x){{
    return "<a href='{}&select="+v+"'>"+v+"</a>";
}};
columns[0].momentarilyFormattable = false;"""

DF_DYNAMIC_FORMATTER_ASSAY_MASK = """columns[1].formatter=function(r,c,v,d,x){{
    return "<a href='{}&select="+data[r]["{}"]+":"+v+"'>"+v+"</a>";
}};
columns[1].momentarilyFormattable = false;"""


def get_dataframe_css(columns):
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


def get_dynamic_dataframe_html(df, cur_url):
    """Display dataframe using SlickGrid"""
    shortnames = []
    def generate_short_names(*args):
        s, j = "", len(shortnames) + 1
        while j > 0:
            s, j = chr(((j % 26) or 26) + 96) + s, (j - 1) // 26
        shortnames.append(s)
        return s
    if df.columns.nlevels == 2:
        rowdata = (
            df.droplevel(0, axis=1).rename(generate_short_names, axis=1)
            .applymap(lambda x: "NA" if isnull(x) else str(x))
            .to_json(orient="records")
        )
        columndata = "\n".join((
            "{{id:'{}',field:'{}',columnGroup:'{}',name:'{}'}},".format(
                sn, sn, level0, level1,
            )
            for (level0, level1), sn in zip(df.columns, shortnames)
        ))
        glv1 = df.columns.get_level_values(1)
        if (glv1[:2] == ["accession", "assay name"]).all():
            formatters = "\n".join([
                DF_DYNAMIC_FORMATTER_GLDS_MASK.format(cur_url),
                DF_DYNAMIC_FORMATTER_ASSAY_MASK.format(
                    cur_url.replace("/assays/", "/samples/"),
                    shortnames[0]
                ),
            ])
        else:
            formatters = ""
    else:
        raise NotImplementedError("Laterz")
    return sub(
        r'\/\/ ROWDATA', rowdata, sub(
            r'\/\/ COLUMNDATA', columndata, sub(
                r'\/\/ FORMATTERS', formatters, DF_DYNAMIC_HTML,
            )
        )
    )


def display_dataframe(df, fmt, cur_url):
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
    elif fmt == "interactive":
        content = get_dynamic_dataframe_html(df, cur_url)
        mimetype = "text/html"
    else:
        raise NotImplementedError("fmt='{}'".format(fmt))
    return Response(content, mimetype=mimetype)


def display(obj, request):
    """Dispatch object and trailing request arguments to display handler"""
    if isinstance(obj, DataFrame):
        return display_dataframe(
            obj, request.args.get("fmt", "tsv"), request.url,
        )
    else:
        raise NotImplementedError(
            "Display of {}".format(str(type(obj).strip("<>")))
        )
