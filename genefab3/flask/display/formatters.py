from genefab3.config import ASSAY_METADATALIKES


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


def get_browser_glds_formatter(context):
    """Get SlickGrid formatter for column 'accession'"""
    mask = "columns[0].formatter={}; columns[0].defaultFormatter={};"
    formatter_mask = """columns[0].formatter=function(r,c,v,d,x){{
        return "<a href='{}select="+escape(v)+"'>"+v+"</a>";
    }};"""
    formatter = formatter_mask.format(build_url(context, drop={"select"}))
    return mask.format(formatter, formatter)


def get_browser_assay_formatter(context, shortnames):
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


def get_browser_meta_formatter(context, i, meta, meta_name):
    """Get SlickGrid formatter for meta column"""
    mask = "columns[{}].formatter={}; columns[{}].defaultFormatter={};"
    if context.view == "/assays/":
        formatter_mask = """function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : ((v == "False")
            ? "<a href='{0}{1}!="+escape("{2}")+"' style='color:#FAA'>"+v+"</a>"
            : "<a href='{0}{1}="+escape("{2}")+
                "' style='color:green'>"+v+"</a>");
        }};"""
    else:
        formatter_mask = """function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : "<a href='{}{}:"+escape("{}")+"="+escape(v)+"'>"+v+"</a>";
        }};"""
    formatter = formatter_mask.format(build_url(context), meta, meta_name)
    return mask.format(i, formatter, i, formatter)


def get_browser_formatters(df, context, shortnames):
    """Get SlickGrid formatters for columns"""
    formatters = []
    if df.columns[0] == ("info", "accession"):
        formatters.append(get_browser_glds_formatter(context))
    if df.columns[1] == ("info", "assay name"):
        formatters.append(get_browser_assay_formatter(context, shortnames))
    for i, (meta, meta_name) in enumerate(df.columns):
        if meta in ASSAY_METADATALIKES:
            formatters.append(
                get_browser_meta_formatter(context, i, meta, meta_name)
            )
    return "\n".join(formatters)
