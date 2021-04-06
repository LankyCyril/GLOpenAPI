from functools import lru_cache, partial
from genefab3.common.utils import empty_iterator, leaf_count
from collections import defaultdict
from re import search, sub, escape
from genefab3.common.types import UniversalSet
from genefab3.db.mongo.utils import is_safe_token
from genefab3.common.exceptions import GeneFabConfigurationException
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
        (c.view != "samples") and (c.format == "cls"),
    "'format=gct' is only valid for /data/": lambda c:
        (c.view != "data") and (c.format == "gct"),
    "'file.filename=' can only be specified once": lambda c:
        (len(c.complete_kwargs.get("file.filename", [])) > 1),
    "only one of 'file.filename=', 'file.datatype=' can be specified": lambda c:
        (len(c.complete_kwargs.get("file.filename", [])) > 1) and
        (len(c.complete_kwargs.get("file.datatype", [])) > 1),
    "'format=gct' is not valid for the requested datatype": lambda c:
        (c.format == "gct") and
        (c.complete_kwargs.get("file.datatype", []) != ["unnormalized counts"]),
    "/file/ only accepts 'format=raw' or 'format=json'": lambda c:
        (c.view == "file") and
        ((c.format or "raw") not in {"raw", "json"}),
}


class Context():
    """Stores request components parsed into MongoDB queries, projections, pipelines"""
 
    SPECIAL_ARGUMENT_PARSER_DISPATCHER = lru_cache(1)(lambda: {
        "file.filename": KeyValueParsers.kvp_filename,
        "debug": empty_iterator, # pass as kwarg
        "format": empty_iterator, # pass as kwarg
    })
 
    def __init__(self):
        """Parse request components"""
        url_root = escape(request.url_root.strip("/"))
        base_url = request.base_url.strip("/")
        self.full_path = request.full_path
        self.view = sub(url_root, "", base_url).strip("/")
        self.complete_kwargs = request.args.to_dict(flat=False)
        self.pipeline = []
        self.query = {"$and": []}
        self.projection = {}
        self.accessions_and_assays = {}
        self.parser_errors = []
        processed_args = set(
            arg for arg, values in request.args.lists()
            if self.update(arg, values)
        )
        self.identity = quote("?".join([
            self.view, dumps(self.complete_kwargs, sort_keys=True),
        ]))
        for k, v in request.args.items():
            if k not in processed_args:
                if not hasattr(self, k):
                    setattr(self, k, v)
                else:
                    msg = "Cannot set context"
                    raise GeneFabConfigurationException(msg, **{k: v})
        self.format = getattr(self, "format", None)
        self.debug = getattr(self, "debug", "0")
        for description, scenario in DISALLOWED_CONTEXTS.items():
            if scenario(self):
                if self.debug == "0":
                    raise GeneFabParserException(description)
                else:
                    self.parser_errors.append(description)
 
    def update(self, arg, values=("",)):
        """Interpret key-value pair; return False/None if not interpretable, else return True and update queries, projections, pipelines"""
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
                raise GeneFabParserException(msg, **{arg: values})
            for value in values:
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
        is_converted_to_query = None
        for pipestep, query, projection_keys, accessions_and_assays in _it():
            if pipestep:
                self.pipeline.append(pipestep)
            if query:
                self.query["$and"].append(query)
            self.projection.update({k: True for k in projection_keys})
            for accession, assay_names in accessions_and_assays.items():
                self.accessions_and_assays[accession] = sorted(assay_names)
            is_converted_to_query = True
        return is_converted_to_query


class KeyValueParsers():
 
    def kvp_filename(fields=(), value=""):
        """Interpret single key-value pair for filename constraint"""
        if (len(fields) == 2) and (fields[0] == "filename") and (not value):
            query = {"file.filename.": fields[1]} # passed as 'file.filename.??'
        elif (len(fields) == 1) and (fields[0] == "filename") and value:
            query = {"file.filename.": value} # passed as 'file.filename=??'
        elif (len(fields) == 1) and (fields[0] == "filename") and (not value):
            query = None # passed as 'file.filename', i.e. catch-all
        else:
            msg, arg = "Unrecognized argument", ".".join("file", *fields)
            raise GeneFabParserException(msg, **{arg: value})
        pipestep = {"$unwind": "$file.filename"}
        projection_keys = {"file.filename"}
        accessions_and_assays = {}
        yield pipestep, query, projection_keys, accessions_and_assays
 
    def kvp_assay(fields=None, value=""):
        """Interpret single key-value pair for dataset / assay constraint"""
        if (fields) or (not value):
            msg = "Unrecognized argument"
            raise GeneFabParserException(msg, arg=f"from.{fields[0]}")
        else:
            pipestep, query = None, {"$or": []}
            projection_keys, accessions_and_assays = (), defaultdict(set)
        for expr in value.split("|"):
            if expr.count(".") == 0:
                query["$or"].append({"info.accession": expr})
                accessions_and_assays[expr] = set()
            else:
                accession, assay_name = expr.split(".", 1)
                subqry = {"info.accession": accession, "info.assay": assay_name}
                query["$or"].append(subqry)
                accessions_and_assays[accession].add(assay_name)
        yield pipestep, query, projection_keys, accessions_and_assays
 
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
        pipestep, accessions_and_assays = None, {}
        yield pipestep, query, projection_keys, accessions_and_assays
