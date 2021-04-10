from functools import lru_cache, partial
from genefab3.common.utils import empty_iterator, leaf_count
from re import search, sub, escape
from flask import request
from urllib.request import quote
from json import dumps
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.exceptions import GeneFabParserException
from genefab3.db.mongo.utils import is_safe_token, reduce_projection, is_regex
from collections import defaultdict


CONTEXT_ARGUMENTS = {"debug", "format"}

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
        KeyValueParsers.kvp_generic, category="file", shift=True,
        constrain_to={"datatype", "filename"},
    ),
})

DISALLOWED_CONTEXTS = {
    "at least one dataset or annotation category must be specified": lambda c:
        (not search(r'^(|status|debug.*|favicon\.\S*)$', c.view)) and
        (len(c.projection) == 0) and (len(c.accessions_and_assays) == 0),
    "metadata queries are not valid for /status/": lambda c:
        (c.view == "status") and (leaf_count(c.query) > 0),
    "'file.filename=' can only be specified once": lambda c:
        (len(c.complete_kwargs.get("file.filename", [])) > 1),
    "only one of 'file.filename=', 'file.datatype=' can be specified": lambda c:
        (len(c.complete_kwargs.get("file.filename", [])) > 1) and
        (len(c.complete_kwargs.get("file.datatype", [])) > 1),
    "'format=cls' is only valid for /samples/": lambda c:
        (c.view != "samples") and (c.format == "cls"),
    "'format=gct' is only valid for /data/": lambda c:
        (c.view != "data") and (c.format == "gct"),
}


class Context():
    """Stores request components parsed into MongoDB queries, projections"""
 
    def __init__(self):
        """Parse request components"""
        url_root = escape(request.url_root.strip("/"))
        base_url = request.base_url.strip("/")
        self.url_root = request.url_root
        self.full_path = request.full_path
        self.view = sub(url_root, "", base_url).strip("/")
        self.complete_kwargs = request.args.to_dict(flat=False)
        self.query, self.projection = {"$and": []}, {}
        self.accessions_and_assays = {}
        self.parser_errors = []
        processed_args = set(
            arg for arg, values in request.args.lists()
            if self.update(arg, values, auto_reduce=False)
        )
        if set(self.projection) & {"file.datatype", "file.filename"}:
            self.projection.update({"file.filename": 1, "file.datatype": 1})
        self.projection = reduce_projection(self.projection)
        if not self.query["$and"]:
            self.query = {}
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
                self.parser_errors.append(description)
 
    def update(self, arg, values=("",), auto_reduce=True):
        """Interpret key-value pair; return False/None if not interpretable, else return True and update queries, projections"""
        def _it():
            category, *fields = arg.split(".")
            if not is_safe_token(arg):
                raise GeneFabParserException("Forbidden argument", arg=arg)
            elif arg in CONTEXT_ARGUMENTS:
                parser = empty_iterator
            elif category in KEYVALUE_PARSER_DISPATCHER():
                parser = KEYVALUE_PARSER_DISPATCHER()[category]
            else:
                msg = "Unrecognized argument"
                raise GeneFabParserException(msg, **{arg: values})
            for value in values:
                if not is_safe_token(value, allow_regex=(arg=="file.filename")):
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
        for query, projection_keys, accessions_and_assays in _it():
            if query:
                self.query["$and"].append(query)
            self.projection.update({k: True for k in projection_keys})
            for accession, assay_names in accessions_and_assays.items():
                self.accessions_and_assays[accession] = sorted(assay_names)
            is_converted_to_query = True
        if auto_reduce:
            self.projection = reduce_projection(self.projection)
        return is_converted_to_query


class KeyValueParsers():
 
    def kvp_assay(fields=None, value=""):
        """Interpret single key-value pair for dataset / assay constraint"""
        if (fields) or (not value):
            msg = "Unrecognized argument"
            raise GeneFabParserException(msg, arg=f"from.{fields[0]}")
        else:
            query = {"$or": []}
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
        yield query, projection_keys, accessions_and_assays
 
    def kvp_generic(category, fields, value, constrain_to=None, dot_postfix="auto", shift=False):
        """Interpret single key-value pair if it gives rise to database query"""
        if (not fields) or (constrain_to and (fields[0] not in constrain_to)):
            msg = "Unrecognized argument"
            raise GeneFabParserException(msg, arg=".".join([category, *fields]))
        elif shift and (len(fields) > 1):
            _fields, _value = fields[:-1], fields[-1]
        else:
            _fields, _value = fields, value
        if (len(_fields) == 2) and (dot_postfix == "auto"):
            projection_key = ".".join([category] + _fields)
            lookup_key = projection_key + "."
        else:
            projection_key = lookup_key = ".".join([category] + _fields)
        if _value: # metadata field must equal value or one of values
            if is_regex(_value):
                query = {lookup_key: {"$regex": _value[1:-1]}}
            else:
                query = {lookup_key: {"$in": _value.split("|")}}
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
        accessions_and_assays = {}
        yield query, projection_keys, accessions_and_assays
