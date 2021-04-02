from collections import defaultdict, OrderedDict
from re import search, sub, escape
from types import SimpleNamespace
from genefab3.common.types import UniversalSet, StringKey, ElemMatchKey
from functools import partial
from werkzeug.datastructures import MultiDict
from genefab3.common.exceptions import GeneFabParserException
from functools import lru_cache
from flask import request
from urllib.request import quote
from json import dumps


leaf_count = lambda d: sum(len(v) for v in d.values())
DISALLOWED_CONTEXTS = {
    "at least one dataset or annotation category must be specified": lambda c:
        (not search(r'^(|status|debug.*|favicon\.\S*)$', c.view)) and
        (len(c.projection) == 0) and (len(c.accessions_and_assays) == 0),
    "metadata queries are not valid for /status/": lambda c:
        (c.view == "status") and (leaf_count(c.query) > 0),
    "'format=cls' is only valid for /samples/": lambda c:
        (c.view != "samples") and (c.kwargs.get("format") == "cls"),
    "'format=gct' is only valid for /data/": lambda c:
        (c.view != "data") and (c.kwargs.get("format") == "gct"),
    "'filename=' can only be specified once": lambda c:
        (len(c.complete_kwargs.get("filename", [])) > 1),
    "'datatype=' can only be specified once": lambda c:
        (len(c.complete_kwargs.get("datatype", [])) > 1),
    "/data/ requires a 'datatype=' argument": lambda c:
        (c.view == "data") and ("datatype" not in c.complete_kwargs),
    "'format=gct' is not valid for the requested datatype": lambda c:
        (c.kwargs.get("format") == "gct") and
        (c.complete_kwargs.get("datatype", []) != "unnormalized counts"),
    "/file/ only accepts 'format=raw'": lambda c:
        (c.view == "file") and (c.kwargs.get("format") != "raw"),
    # TODO: the following ones may not be needed with the new logic:
        "/file/ requires exactly one dataset in the 'from=' argument": lambda c:
            (c.view == "file") and (len(c.accessions_and_assays) != 1),
        "/file/ requires at most one assay in the 'from=' argument": lambda c:
            (c.view == "file") and (leaf_count(c.accessions_and_assays) > 1),
        "/file/ metadata categories are only valid for lookups in assays": lambda c:
            (c.view == "file") and
            (len(c.projection) > 0) and # projection present
            (leaf_count(c.accessions_and_assays) == 0), # but no assays specified
        "/file/ accepts at most one metadata category for lookups in assays": lambda c:
            (c.view == "file") and
            (leaf_count(c.accessions_and_assays) == 1) and # no. of assays == 1
            (len(c.projection) > 1), # more than one field to look in
}


def assay_pair_to_query(fields=None, value=""):
    """Interpret single key-value pair for dataset / assay constraint"""
    query, accessions_and_assays = {"$or": []}, defaultdict(set)
    for expr in value.split("|"):
        if expr.count(".") == 0:
            query["$or"].append({"info.accession": expr})
            accessions_and_assays[expr] = set()
        else:
            accession, assay_name = expr.split(".", 1)
            subquery = {"info.accession": accession, "info.assay": assay_name}
            query["$or"].append(subquery)
            accessions_and_assays[accession].add(assay_name)
    yield query, None, accessions_and_assays


def isa_pair_to_query(category, fields, value, constrain_to=UniversalSet(), dot_postfix="auto"):
    """Interpret single key-value pair if it gives rise to database query"""
    if fields and (fields[0] in constrain_to):
        if (len(fields) == 2) and (dot_postfix == "auto"):
            lookup_key = StringKey(".".join([category] + fields) + ".")
        else:
            lookup_key = StringKey(".".join([category] + fields))
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
                    StringKey(f"{head}.{t}{postfix}") for t in targets
                }
                query = {"$or": [{k: {"$exists": True}} for k in lookup_keys]}
                yield query, lookup_keys, {}


def datatype_pair_to_query(fields=None, value=""):
    query, lookup_keys = {"$or": []}, set()
    for expr in value.split("|"):
        query["$or"].append({"files.datatype": expr})
        lookup_keys.add(ElemMatchKey("files", datatype=expr))
    yield query, lookup_keys, {}


pass_as_kwarg = lambda *a, **k: []
KEY_PARSERS = {
    "from": assay_pair_to_query,
    "investigation": partial(
        isa_pair_to_query, category="investigation", dot_postfix=False,
    ),
    "study": partial(
        isa_pair_to_query, category="study",
        constrain_to={"factor value", "parameter value", "characteristics"},
    ),
    "assay": partial(
        isa_pair_to_query, category="assay",
        constrain_to={"factor value", "parameter value", "characteristics"},
    ),
    "datatype": datatype_pair_to_query,
    "debug": pass_as_kwarg,
    "filename": pass_as_kwarg,
    "format": pass_as_kwarg,
}


def INPLACE_update_context_queries(context, rargs):
    """Interpret all key-value pairs that give rise to database queries"""
    shown, processed = set(), set()
    for key in rargs:
        def query_iterator():
            if "$" not in key:
                category, *fields = key.split(".")
                if category in KEY_PARSERS:
                    parser = KEY_PARSERS[category]
                    for value in rargs.getlist(key):
                        if "$" not in value:
                            yield from parser(fields=fields, value=value)
                else:
                    msg = "Unrecognized argument"
                    raise GeneFabParserException(msg, **{key: rargs[key]})
        for query, lookup_keys, accessions_and_assays in query_iterator():
            context.query["$and"].append(query)
            if lookup_keys:
                shown.update(lookup_keys)
            for accession, assay_names in accessions_and_assays.items():
                context.accessions_and_assays[accession] = sorted(assay_names)
            processed.add(key)
    return shown, processed


def INPLACE_update_context_projection(context, shown):
    """Infer query projection using values in `shown`"""
    ordered_shown = OrderedDict((e, True) for e in sorted(shown))
    print(ordered_shown)
    for target, usable in ordered_shown.items():
        if usable:
            _target = target.format()
            if isinstance(target, StringKey):
                if _target[-1] == ".":
                    context.projection[_target + "."] = True
                else:
                    context.projection[_target] = True
                for potential_child in ordered_shown:
                    if potential_child.startswith(_target):
                        ordered_shown[potential_child] = False
            elif isinstance(target, ElemMatchKey):
                context.file_projection.update(_target)


def INPLACE_update_context(context, rargs):
    """Update context using data in request arguments"""
    shown, processed = INPLACE_update_context_queries(context, MultiDict(rargs))
    INPLACE_update_context_projection(context, shown)
    return processed


Context = lambda: _memoized_context(request)
Context.__doc__ = """Parse and memoize request components"""
@lru_cache(maxsize=None)
def _memoized_context(request):
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = SimpleNamespace(
        full_path=request.full_path,
        view=sub(url_root, "", base_url).strip("/"),
        complete_kwargs=request.args.to_dict(flat=False),
        query={"$and": []}, accessions_and_assays={},
        projection={}, file_projection={}, # TODO: rename `projection`; inject {"_id": False} here for both
    )
    processed = INPLACE_update_context(context, request.args)
    context.kwargs = MultiDict({
        k: v for k, v in request.args.lists() if k not in processed
    })
    context.kwargs["debug"] = context.kwargs.get("debug", "0")
    context.identity = quote("?".join([
        context.view, dumps(context.complete_kwargs, sort_keys=True),
    ]))
    for description, scenario in DISALLOWED_CONTEXTS.items():
        if scenario(context):
            raise GeneFabParserException(description)
    return context
