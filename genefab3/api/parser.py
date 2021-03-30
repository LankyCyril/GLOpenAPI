from collections import defaultdict, OrderedDict
from re import search, sub, escape
from argparse import Namespace
from genefab3.common.types import UniversalSet
from werkzeug.datastructures import MultiDict
from genefab3.common.exceptions import GeneFabParserException
from functools import lru_cache
from flask import request
from urllib.request import quote
from json import dumps


ANNOTATION_CATEGORIES = {"factor value", "parameter value", "characteristics"}
DEFAULT_FORMATS = defaultdict(lambda: "tsv", {})

from operator import eq, ne, gt, getitem, contains, length_hint
not_in = lambda v, s: v not in s
listlen = lambda d, k: len(d.getlist(k))
leaf_count = lambda d, h: sum(length_hint(v, h) for v in d.values())

DISALLOWED_CONTEXTS = [
    dict(_="at least one dataset or annotation category must be specified",
        view=(eq, "/status/", eq, False), # TODO FIXME allow for favicon
        projection=(length_hint, 0, eq, 0), # no projection
        accessions_and_assays=(length_hint, 0, eq, 0), # no datasets
    ),
    dict(_="metadata queries are not valid for /status/",
        view=(eq, "/status/", eq, True), query=(leaf_count, 0, gt, 0),
    ),
    dict(_="'format=cls' is only valid for /samples/",
        view=(eq, "/samples/", eq, False), kwargs=(getitem, "format", eq, "cls"),
    ),
    dict(_="/data/ requires a 'datatype=' argument",
        view=(eq, "/data/", eq, True), kwargs=(contains, "datatype", eq, False),
    ),
    dict(_="'format=gct' is only valid for /data/",
        view=(eq, "/data/", eq, False), kwargs=(getitem, "format", eq, "gct"),
    ),
    dict(_="'format=gct' is not valid for the requested datatype",
        kwargs=[
            (getitem, "format", eq, "gct"),
            (getitem, "datatype", not_in, {"unnormalized counts"}),
        ],
    ),
    dict(_="/file/ only accepts 'format=raw'",
        view=(eq, "/file/", eq, True), kwargs=(getitem, "format", ne, "raw"),
    ),
    dict(_="/file/ requires at most one 'filename=' argument",
        view=(eq, "/file/", eq, True), kwargs=(listlen, "filename", gt, 1),
    ),
    dict(_="/file/ requires a single dataset in the 'from=' argument",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(length_hint, 0, ne, 1), # no. of datasets != 1
    ),
    dict(_="/file/ metadata categories are only valid for lookups in assays",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(leaf_count, 0, eq, 0), # no. of assays == 0
        projection=(length_hint, 0, gt, 0), # projection present
    ),
    dict(_="/file/ accepts at most one metadata category for lookups in assays",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(leaf_count, 0, eq, 1), # no. of assays == 1
        projection=(length_hint, 0, gt, 1), # many fields to look in
    ),
    dict(_="/file/ requires at most one assay in the 'from=' argument",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(leaf_count, 0, gt, 1), # no. of assays > 1
    ),
]


def assay_pair_to_query(key, value):
    """Interpret single key-value pair for dataset / assay constraint"""
    query = {"$or": []}
    accessions_and_assays = defaultdict(set)
    for expr in value.split("|"):
        if expr.count(".") == 0:
            query["$or"].append({"info.accession": expr})
            accessions_and_assays[expr] = set()
        else:
            accession, assay_name = expr.split(".", 1)
            query["$or"].append({
                "info.accession": accession, "info.assay": assay_name,
            })
            accessions_and_assays[accession].add(assay_name)
    yield query, None, accessions_and_assays


def pair_to_query(isa_category, fields, value, constrain_to=UniversalSet(), dot_postfix=False):
    """Interpret single key-value pair if it gives rise to database query"""
    if fields[0] in constrain_to:
        if (len(fields) == 2) and (dot_postfix == "auto"):
            lookup_key = ".".join([isa_category] + fields) + "."
        else:
            lookup_key = ".".join([isa_category] + fields)
        if value: # metadata field must equal value or one of values
            yield {lookup_key: {"$in": value.split("|")}}, {lookup_key}, {}
        else: # metadata field or one of metadata fields must exist
            block_match = search(r'\.[^\.]+\.?$', lookup_key)
            if (not block_match) or (block_match.group().count("|") == 0):
                # single field must exist (no OR condition):
                yield {lookup_key: {"$exists": True}}, {lookup_key}, {}
            else: # either of the fields must exist (OR condition)
                postfix = "." if (block_match.group()[-1] == ".") else ""
                head = lookup_key[:block_match.start()]
                targets = block_match.group().strip(".").split("|")
                lookup_keys = {
                    f"{head}.{target}{postfix}" for target in targets
                }
                query = {"$or": [
                    {key: {"$exists": True}} for key in lookup_keys
                ]}
                yield query, lookup_keys, {}


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
        for query, lookup_keys, accessions_and_assays in query_iterator:
            context.query["$and"].append(query)
            if lookup_keys:
                shown.update(lookup_keys)
            for accession, assay_names in accessions_and_assays.items():
                context.accessions_and_assays[accession] = sorted(assay_names)
            if key in context.kwargs:
                context.kwargs.pop(key)
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


def INPLACE_fill_context_defaults(context):
    """Fill default arguments based on view and other arguments"""
    if "format" not in context.kwargs:
        context.kwargs["format"] = DEFAULT_FORMATS[context.view]
    if "debug" not in context.kwargs:
        context.kwargs["debug"] = "0"


def validate_context(context):
    """Check that no arguments conflict"""
    skip_underscore = lambda kv: kv[0] != "_"
    for scenario in DISALLOWED_CONTEXTS:
        scenario_matches = True
        for attribute, rules in filter(skip_underscore, scenario.items()):
            if isinstance(rules, list):
                rule_iter = rules
            else:
                rule_iter = [rules]
            for (f1, v1, f2, v2) in rule_iter:
                scenario_matches = (
                    scenario_matches and
                    f2(f1(getattr(context, attribute), v1), v2)
                )
        if scenario_matches:
            raise GeneFabParserException(scenario["_"])
    trailing_keys = set(context.kwargs) - {
        "datatype", "filename", "format", "debug",
    }
    if trailing_keys:
        raise GeneFabParserException(
            "Unrecognized arguments",
            **{k: context.kwargs[k] for k in trailing_keys}
        )


Context = lambda: _memoized_context(request)
Context.__doc__ = """Parse and memoize request components"""
@lru_cache(maxsize=None)
def _memoized_context(request):
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = Namespace(
        full_path=request.full_path,
        view="/"+sub(url_root, "", base_url).strip("/")+"/",
        complete_args=request.args.to_dict(flat=False),
        accessions_and_assays={},
        query={"$and": []}, projection={},
        kwargs=MultiDict(request.args),
    )
    INPLACE_update_context(context, request.args)
    INPLACE_fill_context_defaults(context)
    context.identity = quote(
        context.view + dumps(context.complete_args, sort_keys=True)
    )
    validate_context(context)
    return context
