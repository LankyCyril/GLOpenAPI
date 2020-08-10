from genefab3.exceptions import GeneLabException
from re import sub, escape
from argparse import Namespace
from genefab3.config import ASSAY_METADATALIKES
from collections import defaultdict


QUERY, WILDCARD, REMOVER = 0, 1, 2


def parse_assay_selection(rargs_select_list):
    """Parse 'select' request argument"""
    if len(rargs_select_list) == 0:
        return None
    elif len(rargs_select_list) == 1:
        context_select = defaultdict(set)
        for query in rargs_select_list[0].split("|"):
            query_components = query.split(":", 1)
            if len(query_components) == 1:
                context_select[query_components[0]].add(None)
            else:
                context_select[query_components[0]].add(query_components[1])
        return context_select
    else:
        raise GeneLabException("'select' can be used no more than once")


def assay_selection_to_query(selection):
    """Convert 'select' dictionary into MongoDB query"""
    if selection:
        query = {"$or": []}
        for accession, assay_names in selection.items():
            for assay_name in assay_names:
                if assay_name:
                    query["$or"].append({
                        "accession": accession, "assay name": assay_name,
                    })
                else:
                    query["$or"].append({"accession": accession})
        return query
    else:
        return {}


def parse_meta_queries(key, expressions):
    """Process queries like e.g. 'factors=age' and 'factors:age=1|2'"""
    if key[-1] == "!":
        real_key, negation = key[:-1], True
    else:
        real_key, negation = key, False
    if ":" in real_key:
        meta, field = real_key.split(":") # e.g. "factors" and "age"
    else:
        meta, field = real_key, None # e.g. "factors"
    if meta in ASSAY_METADATALIKES:
        for expression in expressions:
            malformed_err = "Malformed argument: {}={}".format(key, expression)
            undefined_err = "Undefined behavior: {}={}".format(key, expression)
            values = expression.split("|")
            if negation and (len(values) > 1): # e.g. "f:a!=5|7" or "f!=a|b"
                raise GeneLabException(undefined_err)
            elif field: # e.g. "factors:age"
                if expression and negation: # e.g. "factors:age!=5"
                    yield QUERY, meta, {field}, {field: {"$ne": values[0]}}
                elif expression: # e.g. "factors:age=7"
                    yield QUERY, meta, {field}, {field: {"$in": values}}
                else: # e.g. "factors:age!=" without value
                    raise GeneLabException(malformed_err)
            else:
                if negation and expression: # e.g. "factors!=age"
                    yield REMOVER, meta, {values[0]}, None
                elif expression: # e.g. "factors=age"
                    yield QUERY, meta, set(values), {"$or": [
                        {value: {"$exists": True}} for value in values
                    ]}
                elif negation: # e.g. "factors!=" without value
                    raise GeneLabException(malformed_err)
                else: # e.g. "factors", i.e. all factors
                    yield WILDCARD, meta, None, None


def parse_request(request):
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        view="/"+sub(url_root, "", base_url).strip("/")+"/",
        select=parse_assay_selection(request.args.getlist("select")),
        args=request.args,
        queries=defaultdict(list),
        wildcards=set(),
        fields=defaultdict(set),
        removers=defaultdict(set),
    )
    context.queries["select"] = assay_selection_to_query(context.select)
    meta_has_removed_fields = defaultdict(bool)
    for key in request.args:
        parser = parse_meta_queries(key, set(request.args.getlist(key)))
        for kind, meta, fields, query in parser:
            if kind == QUERY:
                context.queries[meta].append(query)
                context.fields[meta] |= fields
            elif kind == REMOVER:
                context.fields[meta] -= fields
                context.removers[meta] |= fields
                meta_has_removed_fields[meta] = True
            elif kind == WILDCARD:
                context.wildcards.add(meta)
    for meta in ASSAY_METADATALIKES:
        if meta_has_removed_fields[meta] and (not context.fields[meta]):
            context.wildcards.add(meta)
    return context
