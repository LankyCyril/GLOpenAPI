from re import sub, escape, search
from argparse import Namespace
from genefab3.config import ANNOTATION_CATEGORIES
from genefab3.utils import UniversalSet


def pair_to_query(isa_category, fields, value, constrain_to=UniversalSet(), dot_postfix=False):
    """Interpret single key-value pair if it gives rise to database query"""
    if fields[0] in constrain_to:
        for component in (isa_category, *fields, value):
            if search(r'\$', component):
                break
        else:
            if (len(fields) == 2) and (dot_postfix == "auto"):
                lookup_key = ".".join([isa_category] + fields) + "."
            else:
                lookup_key = ".".join([isa_category] + fields)
            if value:
                query = {lookup_key: {"$in": value.split("|")}}
            else:
                query = {lookup_key: {"$exists": True}}
            yield query, {lookup_key: True}


def request_pairs_to_queries(rargs, key):
    """Interpret key-value pairs if they give rise to database queries"""
    isa_category, *fields = key.split(".")
    if fields:
        for value in rargs.getlist(key):
            if isa_category == "investigation":
                yield from pair_to_query(
                    isa_category, fields, value,
                    constrain_to=UniversalSet(), dot_postfix=False,
                )
            elif isa_category in {"study", "assay"}:
                yield from pair_to_query(
                    isa_category, fields, value,
                    constrain_to=ANNOTATION_CATEGORIES, dot_postfix="auto",
                )


def parse_request(request):
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        view="/"+sub(url_root, "", base_url).strip("/")+"/",
        request=request, query={"$and": []}, projection={},
    )
    for key in request.args:
        for query, projection in request_pairs_to_queries(request.args, key):
            if query:
                context.query["$and"].append(query)
            if projection:
                context.projection.update(projection)
    for projection in request.args.getlist("hide"):
        context.projection[projection] = False
    return context
