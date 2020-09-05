from re import sub, escape
from argparse import Namespace
from genefab3.config import ANNOTATION_CATEGORIES
from genefab3.utils import UniversalSet


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
            projection = {lookup_key + ".": True}
        else:
            lookup_key = ".".join([isa_category] + fields)
            projection = {lookup_key: True}
        if value:
            query = {lookup_key: {"$in": value.split("|")}}
        else:
            query = {lookup_key: {"$exists": True}}
        yield query, projection


def request_pairs_to_queries(rargs, key):
    """Interpret key-value pairs if they give rise to database queries"""
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


def parse_request(request):
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        view="/"+sub(url_root, "", base_url).strip("/")+"/",
        args=request.args, query={"$and": []}, projection={},
        hide=set(),
    )
    for key in request.args:
        for query, projection in request_pairs_to_queries(request.args, key):
            if query:
                context.query["$and"].append(query)
            if projection:
                context.projection.update(projection)
    for field in request.args.getlist("hide"):
        if (field in context.projection) and (field != "_id"):
            del context.projection[field]
        else:
            context.projection.hide.add(field)
    return context
