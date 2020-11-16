from re import sub, escape
from argparse import Namespace
from genefab3.config import ANNOTATION_CATEGORIES
from genefab3.utils import UniversalSet
from collections import OrderedDict


def select_pair_to_query(key, value):
    """Interpret single key-value pair for dataset / assay constraint"""
    if value.count(".") == 0:
        yield {".accession": value}, None
    elif value.count(".") == 1:
        accession, assay_name = value.split(".")
        yield {".accession": accession, ".assay": assay_name}, None


def pair_to_query(isa_category, fields, value, constrain_to=UniversalSet(), dot_postfix=False):
    """Interpret single key-value pair if it gives rise to database query"""
    if fields[0] in constrain_to:
        if (len(fields) == 2) and (dot_postfix == "auto"):
            lookup_key = ".".join([isa_category] + fields) + "."
        else:
            lookup_key = ".".join([isa_category] + fields)
        if value:
            yield {lookup_key: {"$in": value.split("|")}}, lookup_key
        else:
            yield {lookup_key: {"$exists": True}}, lookup_key


def request_pairs_to_queries(rargs, key):
    """Interpret key-value pairs under same key if they give rise to database queries"""
    if key == "select":
        for value in rargs.getlist(key):
            if "$" not in value:
                yield from select_pair_to_query(key, value)
    elif "$" not in key:
        isa_category, *fields = key.split(".")
        if fields:
            for value in rargs.getlist(key):
                if "$" not in value:
                    if isa_category == "investigation":
                        yield from pair_to_query(
                            isa_category, fields, value,
                            constrain_to=UniversalSet(), dot_postfix=False,
                        )
                    elif isa_category in {"study", "assay"}:
                        yield from pair_to_query(
                            isa_category, fields, value,
                            constrain_to=ANNOTATION_CATEGORIES,
                            dot_postfix="auto",
                        )


def INPLACE_update_context_queries(context, rargs):
    """Interpret all key-value pairs that give rise to database queries"""
    show = set()
    for key in rargs:
        for query, lookup_key in request_pairs_to_queries(rargs, key):
            context.query["$and"].append(query)
            if lookup_key:
                show.add(lookup_key)
    return show


def INPLACE_update_context_projection(context, show):
    """Infer query projection using values in `show`"""
    ordered_show = OrderedDict((e, True) for e in sorted(show))
    for target, usable in ordered_show.items():
        if usable:
            if target[-1] == ".":
                context.projection[target + "."] = True
            else:
                context.projection[target] = True
            for potential_child in ordered_show:
                if potential_child.startswith(target):
                    ordered_show[potential_child] = False


def parse_request(request):
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        view="/"+sub(url_root, "", base_url).strip("/")+"/",
        args=request.args, query={"$and": []}, projection={},
    )
    show = INPLACE_update_context_queries(context, request.args)
    INPLACE_update_context_projection(context, show)
    return context
