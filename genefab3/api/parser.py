from functools import lru_cache, partial
from genefab3.common.utils import empty_iterator, leaf_count, as_is
from genefab3.common.exceptions import GeneFabConfigurationException
from collections import defaultdict
from re import search, sub, escape
from types import SimpleNamespace
from genefab3.common.types import UniversalSet
from genefab3.db.mongo.utils import is_safe_token
from werkzeug.datastructures import MultiDict
from genefab3.common.exceptions import GeneFabParserException
from flask import request
from urllib.request import quote
from json import dumps


SPECIAL_ARGUMENT_PARSER_DISPATCHER = lru_cache(1)(lambda: {
    "file.filename": KeyValueParsers.kvp_filename,
    "debug": empty_iterator, # pass as kwarg
    "format": empty_iterator, # pass as kwarg
})

KEYVALUE_PARSER_DISPATCHER = lru_cache(1)(lambda: {
    "from": KeyValueParsers.kvp_assay,
    "investigation": partial(
        KeyValueParsers.kvp_generic, category="investigation", dot_postfix=None,
    ),
    "study": partial(
        KeyValueParsers.kvp_generic, category="study",
        constrain_to={"factor value", "parameter value", "characteristics"},
    ),
    "assay": partial(
        KeyValueParsers.kvp_generic, category="assay",
        constrain_to={"factor value", "parameter value", "characteristics"},
    ),
    "file": partial(
        KeyValueParsers.kvp_generic, category="file", constrain_to={"datatype"},
    ),
})

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
    "'file.filename=' can only be specified once": lambda c:
        (len(c.complete_kwargs.get("file.filename", [])) > 1),
    "only one of 'file.filename=', 'file.datatype=' can be specified": lambda c:
        (len(c.complete_kwargs.get("file.filename", [])) > 1) and
        (len(c.complete_kwargs.get("file.datatype", [])) > 1),
    "/data/ requires a 'file.datatype=' argument": lambda c:
        (c.view == "data") and ("file.datatype" not in c.complete_kwargs),
    "'format=gct' is not valid for the requested datatype": lambda c:
        (c.kwargs.get("format") == "gct") and
        (c.complete_kwargs.get("file.datatype", []) != ["unnormalized counts"]),
    "/file/ only accepts 'format=raw' or 'format=json'": lambda c:
        (c.view == "file") and
        (c.kwargs.get("format", "raw") not in {"raw", "json"}),
}


def INPLACE_unwind_target_filenames(dataframe, filename):
    """Remove unneeded entries and flatten target entry, as projection {"file.filename..": True} returns too much"""
    def _unwind(value):
        if isinstance(value, list):
            _it = value
        elif isinstance(value, dict):
            _it = [value]
        else:
            return value
        for entry in _it:
            if isinstance(entry, dict) and (entry.get("") == filename):
                if len(entry) == 1: # only the target field present
                    return entry[""]
                else: # other keys present
                    return [entry] # formatted for internal URL resolution
        else:
            msg = "Non-unwindable value returned"
            raise GeneFabConfigurationException(msg, value=value)
    raw_group = dataframe[[("file.filename", "")]]
    dataframe[("file.filename", filename)] = raw_group.applymap(_unwind)
    dataframe.drop(columns=[("file.filename", "")], inplace=True)


class KeyValueParsers():
 
    def kvp_filename(fields=None, value=""):
        """Interpret single key-value pair for filename constraint"""
        if (len(fields) != 2) or (fields[0] != "filename") or value:
            msg = "Unrecognized argument"
            raise GeneFabParserException(msg, **{f"file.{fields[0]}": value})
        else:
            filename = fields[1]
            query = {"file.filename": {"$elemMatch": {"": filename}}}
            projection_keys = {"file.filename.."}
            pp_fn = partial(INPLACE_unwind_target_filenames, filename=filename)
        yield query, projection_keys, {}, pp_fn
 
    def kvp_assay(fields=None, value=""):
        """Interpret single key-value pair for dataset / assay constraint"""
        if (fields) or (not value):
            msg = "Unrecognized argument"
            raise GeneFabParserException(msg, arg=f"from.{fields[0]}")
        else:
            query, accessions_and_assays = {"$or": []}, defaultdict(set)
        for expr in value.split("|"):
            if expr.count(".") == 0:
                query["$or"].append({"info.accession": expr})
                accessions_and_assays[expr] = set()
            else:
                accession, assay_name = expr.split(".", 1)
                subqry = {"info.accession": accession, "info.assay": assay_name}
                query["$or"].append(subqry)
                accessions_and_assays[accession].add(assay_name)
        yield query, (), accessions_and_assays, as_is
 
    def kvp_generic(category, fields, value, constrain_to=UniversalSet(), dot_postfix="auto"):
        """Interpret single key-value pair if it gives rise to database query"""
        if (not fields) or (fields[0] not in constrain_to):
            msg = "Unrecognized argument"
            raise GeneFabParserException(msg, arg=".".join([category, *fields]))
        elif (len(fields) == 2) and (dot_postfix == "auto"):
            projection_key = ".".join([category] + fields)
            lookup_key = projection_key + "."
        else:
            projection_key = lookup_key = ".".join([category] + fields)
        if value: # metadata field must equal value or one of values
            query = {lookup_key: {"$in": value.split("|")}}
            projection_keys = {projection_key}
        else: # metadata field or one of metadata fields must exist
            block_match = search(r'\.[^\.]+\.*$', lookup_key)
            if (not block_match) or (block_match.group().count("|") == 0):
                # single field must exist (no OR condition):
                query = {lookup_key: {"$exists": True}}
                projection_keys = {projection_key}
            else: # either of the fields must exist (OR condition)
                head = lookup_key[:block_match.start()]
                targets = block_match.group().strip(".").split("|")
                projection_keys = {f"{head}.{target}" for target in targets}
                if block_match.group()[-1] == ".":
                    lookup_keys = {k+"." for k in projection_keys}
                else:
                    lookup_keys = projection_keys
                query = {"$or": [{k: {"$exists": True}} for k in lookup_keys]}
        yield query, projection_keys, {}, as_is


def INPLACE_update_context(context, rargs):
    """Interpret all key-value pairs that give rise to database queries"""
    for arg in rargs:
        def _it():
            category, *fields = arg.split(".")
            if not is_safe_token(arg):
                raise GeneFabParserException("Forbidden argument", arg=arg)
            elif arg in SPECIAL_ARGUMENT_PARSER_DISPATCHER():
                parser = SPECIAL_ARGUMENT_PARSER_DISPATCHER()[arg]
            elif category in KEYVALUE_PARSER_DISPATCHER():
                parser = KEYVALUE_PARSER_DISPATCHER()[category]
            else:
                msg = "Unrecognized argument"
                raise GeneFabParserException(msg, **{arg: rargs[arg]})
            for value in rargs.getlist(arg):
                if not is_safe_token(value):
                    raise GeneFabParserException("Forbidden value", arg=value)
                if (value) and (len(fields) == 1):
                    # assay.characteristics=age -> assay.characteristics.age
                    yield from parser(fields=[*fields, value], value="")
                else:
                    # assay.characteristics.age
                    # assay.characteristics.age=5
                    # from=GLDS-1.assay
                    yield from parser(fields=fields, value=value)
        for query, projection_keys, accessions_and_assays, postproc_fn in _it():
            context.query["$and"].append(query)
            context.projection.update({k: True for k in projection_keys})
            for accession, assay_names in accessions_and_assays.items():
                context.accessions_and_assays[accession] = sorted(assay_names)
            context.INPLACE_postprocess_functions.append(postproc_fn)
            yield arg


def Context():
    """Parse request components"""
    url_root = escape(request.url_root.strip("/"))
    base_url = request.base_url.strip("/")
    context = SimpleNamespace(
        full_path=request.full_path,
        view=sub(url_root, "", base_url).strip("/"),
        complete_kwargs=request.args.to_dict(flat=False),
        query={"$and": []}, projection={}, accessions_and_assays={},
        INPLACE_postprocess_functions=[],
    )
    processed_args = set(INPLACE_update_context(context, request.args))
    context.kwargs = MultiDict({
        k: v for k, v in request.args.lists() if k not in processed_args
    })
    context.kwargs["debug"] = context.kwargs.get("debug", "0")
    context.identity = quote("?".join([
        context.view, dumps(context.complete_kwargs, sort_keys=True),
    ]))
    for description, scenario in DISALLOWED_CONTEXTS.items():
        if scenario(context):
            raise GeneFabParserException(description)
    return context
