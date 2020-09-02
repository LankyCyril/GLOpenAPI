from genefab3.exceptions import GeneLabException
from re import sub, escape, search, split, IGNORECASE
from argparse import Namespace
from genefab3.config import ANNOTATION_CATEGORIES
from functools import partial


QUERY_ERROR = "Could not parse query component"


def is_convertible_to_query(key):
    """Determine if `key` is something that gives rise to database query"""
    keystart = split(r'[.!]', key)[0]
    return (
        (keystart in ANNOTATION_CATEGORIES) or
        (keystart in {"", "assay", "select"})
    )


def parse_request_wildcard_component(key, value):
    """Parse query wildcard, such as 'factor values', 'characteristics'"""
    return {}, {key: None}


def parse_request_select_component(key, value):
    """Parse query for accession / assay name selection, such as 'select=GLDS-242.RR9_LVR|GLDS-4'"""
    qchunks = []
    for chunk in value.split("|"):
        if search(r'^GLDS-[0-9]+\.', chunk):
            accession, assay_name = chunk.split(".", 1)
            qchunks.append({".accession": accession, ".assay": assay_name})
        else:
            qchunks.append({".accession": chunk})
    return {"$or": qchunks}, {}


def parse_request_equality_component(key, value):
    """Parse query for existence / exact value, such as 'factor values=spaceflight', or 'characteristics.age=5|7"""
    if "." in key:
        query_component = {key.lower() + ".": {"$in": value.split("|")}}
        target = {key.lower().split(".")[0]: [key.lower().split(".", 1)[1]]}
    else:
        query_component = {"$or": [
            {key + "." + field: {"$exists": True}} for field in value.split("|")
        ]}
        target = {key: value.split("|")}
    return query_component, target


def parse_request_negation_component(key, value):
    """Parse query for inequality, such as 'factor values.spaceflight!=Ground Control'"""
    if ("." in key) and ("|" not in value):
        query_component = {"$and": [
            {key.lower().rstrip("!"): {"$exists": True}},
            {key.lower().rstrip("!") + ".": {"$ne": value}},
        ]}
        target = {
            key.lower().split(".")[0]: [
                key.lower().split(".", 1)[1].rstrip("!")
            ]
        }
        return query_component, target
    else:
        raise GeneLabException(QUERY_ERROR, key + "!=" + value)


def parse_request(request):
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        view="/"+sub(url_root, "", base_url).strip("/")+"/",
        request=request, request_components=[], targets=[], query={"$and": []},
    )
    for key in request.args:
        if is_convertible_to_query(key):
            for value in request.args.getlist(key):
                request_component = key + "=" + value
                parse_request_component = None
                matches = partial(
                    search, string=request_component, flags=IGNORECASE,
                )
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
                query_component, target = parse_request_component(key, value)
                if query_component:
                    context.query["$and"].append(query_component)
                if target:
                    context.targets.append(target)
                context.request_components.append(request_component)
    return context
