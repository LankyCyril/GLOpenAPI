from re import search, sub, escape
from argparse import Namespace
from genefab3.config import ANNOTATION_CATEGORIES
from genefab3.utils import UniversalSet
from collections import OrderedDict
from werkzeug.datastructures import MultiDict


def assay_pair_to_query(key, value):
    """Interpret single key-value pair for dataset / assay constraint"""
    query = {"$or": []}
    accessions = set()
    for expr in value.split("|"):
        if expr.count(".") == 0:
            query["$or"].append({".accession": expr})
            accessions.add(expr)
        else:
            accession, assay_name = expr.split(".", 1)
            query["$or"].append({".accession": accession, ".assay": assay_name})
            accessions.add(accession)
    yield query, None, accessions


def pair_to_query(isa_category, fields, value, constrain_to=UniversalSet(), dot_postfix=False):
    """Interpret single key-value pair if it gives rise to database query"""
    if fields[0] in constrain_to:
        if (len(fields) == 2) and (dot_postfix == "auto"):
            lookup_key = ".".join([isa_category] + fields) + "."
        else:
            lookup_key = ".".join([isa_category] + fields)
        if value: # metadata field must equal value or one of values
            yield {lookup_key: {"$in": value.split("|")}}, {lookup_key}, None
        else: # metadata field or one of metadata fields must exist
            block_match = search(r'\.[^\.]+\.$', lookup_key)
            if (not block_match) or (block_match.group().count("|") == 0):
                # single field must exist (no OR condition):
                yield {lookup_key: {"$exists": True}}, {lookup_key}, None
            else: # either of the fields must exist (OR condition)
                head = lookup_key[:block_match.start()]
                targets = block_match.group().strip(".").split("|")
                lookup_keys = {f"{head}.{target}." for target in targets}
                query = {"$or": [
                    {key: {"$exists": True}} for key in lookup_keys
                ]}
                yield query, lookup_keys, None


def request_pairs_to_queries(rargs, key):
    """Interpret key-value pairs under same key if they give rise to database queries"""
    if key == "from":
        for value in rargs.getlist(key):
            if "$" not in value:
                yield from assay_pair_to_query(key, value)
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
    shown = set()
    for key in rargs:
        query_iterator = request_pairs_to_queries(rargs, key)
        for query, lookup_keys, accessions in query_iterator:
            context.query["$and"].append(query)
            if lookup_keys:
                shown.update(lookup_keys)
            if accessions:
                context.accessions.update(accessions)
            if key in context.kwargs:
                context.kwargs.pop(key)
    context.accessions = sorted(context.accessions)
    return shown


def INPLACE_update_context_projection(context, shown):
    """Infer query projection using values in `shown`"""
    ordered_shown = OrderedDict((e, True) for e in sorted(shown))
    for target, usable in ordered_shown.items():
        if usable:
            if target[-1] == ".":
                context.projection[target + "."] = True
            else:
                context.projection[target] = True
            for potential_child in ordered_shown:
                if potential_child.startswith(target):
                    ordered_shown[potential_child] = False


def INPLACE_update_context(context, rargs):
    """Update context using data in request arguments"""
    shown = INPLACE_update_context_queries(context, MultiDict(rargs))
    INPLACE_update_context_projection(context, shown)


def parse_request(request):
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        view="/"+sub(url_root, "", base_url).strip("/")+"/",
        complete_args=request.args,
        accessions=set(),
        query={"$and": []}, projection={},
        kwargs=MultiDict(request.args),
    )
    INPLACE_update_context(context, request.args)
    return context
