from re import search, sub, escape
from argparse import Namespace
from genefab3.config import ANNOTATION_CATEGORIES, DEFAULT_FORMATS
from genefab3.common.types import UniversalSet
from collections import defaultdict, OrderedDict
from werkzeug.datastructures import MultiDict
from genefab3.config import DISALLOWED_CONTEXTS
from genefab3.common.exceptions import GeneFabParserException
from urllib.request import quote
from json import dumps


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
        context.kwargs["format"] = DEFAULT_FORMATS.get(context.view, "raw")
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


def parse_request(request):
    """Parse request components"""
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
