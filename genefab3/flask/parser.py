from genefab3.exceptions import GeneLabException
from re import sub, escape, search, split
from argparse import Namespace
from genefab3.config import ANNOTATION_CATEGORIES
from genefab3.utils import UniversalSet
from functools import partial
from collections import defaultdict


QUERY_ERROR = "Could not parse query component"


def is_convertible_to_query(key):
    """Determine if `key` is something that gives rise to database query"""
    return (
        (split(r'[.!]', key)[0] in ANNOTATION_CATEGORIES) or
        (split(r'\.', key)[0] == "assay") or
        (key == "select")
    )


def parse_request_wildcard_component(key, value):
    """Parse query wildcard, such as 'factor values', 'characteristics'"""
    query_component = {} # match everything, do not filter
    category = key # retrieve subkeys under `key`
    fields = UniversalSet() # use all subkeys
    return query_component, category, fields


def parse_request_select_component(key, value):
    """Parse query for accession / assay name selection, such as 'select=GLDS-242.RR9_LVR|GLDS-4'"""
    qchunks = []
    for chunk in value.split("|"):
        if search(r'^GLDS-[0-9]+\.', chunk):
            accession, assay_name = chunk.split(".", 1)
            qchunks.append({".accession": accession, ".assay": assay_name})
        else:
            qchunks.append({".accession": chunk})
    query_component = {"$or": qchunks} # match by any accession/assay combo
    category = None # accession / assay selection does not affect columns
    fields = None
    return query_component, category, fields


def parse_request_equality_component(key, value):
    """Parse query for existence / exact value, such as 'factor values=spaceflight', or 'characteristics.age=5|7"""
    if key.count(".") > 1:
        raise GeneLabException(QUERY_ERROR, key + "!=" + value)
    elif "." in key: # e.g. "characteristics.age", i.e. matching values of "age"
        category, field = key.lower().split(".")
        fields = {field}
        query_component = {f"{category}.{field}.": {"$in": value.split("|")}}
    else: # e.g. "characteristics=age", i.e. matching existence of "age"
        category = key
        fields = set(value.split("|"))
        query_component = {"$or": [
            {f"{category}.{field}": {"$exists": True}} for field in fields
        ]}
    return query_component, category, fields


def parse_request_negation_component(key, value):
    """Parse query for inequality, such as 'factor values.spaceflight!=Ground Control'"""
    if (key.count(".") == 1) and ("|" not in value):
        category, field = key.lower().rstrip("!").split(".")
        fields = {field}
        query_component = {"$and": [
            {f"{category}.{field}": {"$exists": True}},
            {f"{category}.{field}.": {"$ne": value}},
        ]}
        return query_component, category, fields
    else:
        raise GeneLabException(QUERY_ERROR, key + "!=" + value)


def parse_request(request):
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        view="/"+sub(url_root, "", base_url).strip("/")+"/", request=request,
        request_components=[], targets=defaultdict(set), query={"$and": []},
    )
    for key in request.args:
        if is_convertible_to_query(key):
            for value in request.args.getlist(key):
                request_component = key + "=" + value
                matches = partial(search, string=request_component)
                if matches(r'^[^!=$]+=$'):
                    parse_request_component = parse_request_wildcard_component
                elif matches(r'^select=[^!=$]+$'):
                    parse_request_component = parse_request_select_component
                elif matches(r'^[^!=$]+=[^!=$]+$'):
                    parse_request_component = parse_request_equality_component
                elif matches(r'^[^!=$]+!=[^!=$|]+$'):
                    parse_request_component = parse_request_negation_component
                else:
                    raise GeneLabException(QUERY_ERROR, request_component)
                query_component, t1k, t2ks = parse_request_component(key, value)
                if query_component:
                    context.query["$and"].append(query_component)
                if t1k:
                    context.targets[t1k] = context.targets[t1k] | t2ks
                context.request_components.append(request_component)
    return context
