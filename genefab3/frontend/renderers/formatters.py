def build_url(context, target_view=None, drop=set()):
    """Rebuild URL from request, alter based on `replace` and `drop`"""
    if target_view is None:
        url_components = [context.url_root.rstrip("/"), context.view + "?"]
    else:
        url_components = [context.url_root.rstrip("/"), target_view + "?"]
    for arg, values in context.complete_args.items():
        if arg not in drop:
            for value in values:
                if value == "":
                    url_components.append(arg+"&")
                else:
                    url_components.append("{}={}&".format(arg, value))
    return "".join(url_components)


def get_browser_glds_formatter(context):
    """Get SlickGrid formatter for column 'accession'"""
    mask = "columns[0].formatter={}; columns[0].defaultFormatter={};"
    formatter_mask = """columns[0].formatter=function(r,c,v,d,x){{
        return "<a href='{}from="+escape(v)+"'>"+v+"</a>";
    }};"""
    formatter = formatter_mask.format(build_url(context, drop={"from"}))
    return mask.format(formatter, formatter)


def get_browser_assay_formatter(context, shortnames):
    """Get SlickGrid formatter for column 'assay name'"""
    mask = "columns[1].formatter={}; columns[1].defaultFormatter={};"
    formatter_mask = """function(r,c,v,d,x){{
        return "<a href='{}from="+data[r]["{}"]+"."+escape(v)+"'>"+v+"</a>";
    }};"""
    formatter = formatter_mask.format(
        build_url(context, "/samples/", drop={"from"}),
        shortnames[0],
    )
    return mask.format(formatter, formatter)


def get_browser_meta_formatter(context, i, category, subkey, target):
    """Get SlickGrid formatter for meta column"""
    mask = "columns[{}].formatter={}; columns[{}].defaultFormatter={};"
    if context.view == "/assays/":
        formatter_mask = """function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : ((v == "False")
            ? "<font style='color:#FAA'>"+v+"</font>"
            : "<a href='{}"+escape("{}.{}.{}")+
                "' style='color:green'>"+v+"</a>");
        }};"""
    else:
        formatter_mask = """function(r,c,v,d,x){{
            return (v == "NA")
            ? "<i style='color:#BBB'>"+v+"</i>"
            : "<a href='{}"+escape("{}.{}.{}")+"="+escape(v)+"'>"+v+"</a>";
        }};"""
    formatter = formatter_mask.format(
        build_url(context), category, subkey, target,
    )
    return mask.format(i, formatter, i, formatter)


def get_browser_formatters(df, context, shortnames):
    """Get SlickGrid formatters for columns"""
    formatters = []
    if df.columns[0] == ("info", "accession"):
        formatters.append(get_browser_glds_formatter(context))
    if (len(df.columns) > 1) and (df.columns[1] == ("info", "assay")):
        formatters.append(get_browser_assay_formatter(context, shortnames))
    for i, (key, target) in enumerate(df.columns):
        category, *subkeys = key.split(".")
        if len(subkeys) == 1:
            if category != "info":
                formatters.append(
                    get_browser_meta_formatter(
                        context, i, category, subkeys[0], target,
                    ),
                )
    return "\n".join(formatters)
