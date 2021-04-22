from functools import lru_cache, partial
from genefab3.common.utils import EmptyIterator, BranchTracer
from re import search, sub, escape
from flask import request
from urllib.request import quote
from json import dumps
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.exceptions import GeneFabParserException
from genefab3.db.mongo.utils import is_safe_token, is_regex


CONTEXT_ARGUMENTS = {"debug", "format"}

KEYVALUE_PARSER_DISPATCHER = lru_cache(maxsize=1)(lambda: {
    "id": partial(KeyValueParsers.kvp_assay,
        category="id", fields_depth=1,
        constrain_to={"accession", "assay", "sample name", "study", None},
    ),
    "investigation": partial(KeyValueParsers.kvp_generic,
        category="investigation", fields_depth=2, dot_postfix=None,
        constrain_to={"investigation", "study", "study assays"},
    ),
    "study": partial(KeyValueParsers.kvp_generic,
        category="study", fields_depth=2,
        constrain_to={"characteristics", "factor value", "parameter value"},
    ),
    "assay": partial(KeyValueParsers.kvp_generic,
        category="assay", fields_depth=2,
        constrain_to={"characteristics", "factor value", "parameter value"},
    ),
    "file": partial(KeyValueParsers.kvp_generic,
        category="file", fields_depth=1, constrain_to={"datatype", "filename"},
    ),
})


class Context():
    """Stores request components parsed into MongoDB queries, projections"""
 
    def __init__(self, flask_app):
        """Parse request components"""
        self.app_name = flask_app.name
        url_root = escape(request.url_root.strip("/")) # TODO why escape this
        base_url = request.base_url.strip("/") # but not this?
        self.url_root = request.url_root
        self.full_path = request.full_path
        self.view = sub(url_root, "", base_url).strip("/")
        self.complete_kwargs = request.args.to_dict(flat=False)
        self.query, self.unwind = {"$and": []}, set()
        self.projection = {"id.accession": True, "id.assay": True}
        self.sort_by = ["id.accession", "id.assay"]
        self.processed_args = {
            arg for arg, values in request.args.lists()
            if self.update(arg, values, auto_reduce=False)
        }
        self.update_special_fields()
        self.reduce_projection()
        self.update_attributes()
        if not self.query["$and"]:
            self.query = {}
        self.identity = quote(dumps(sort_keys=True, separators=(",", ":"), obj={
            "?": self.view, "query": self.query, "sort_by": self.sort_by,
            "unwind": sorted(self.unwind), "projection": self.projection,
            "format": self.format, "debug": self.debug,
        }))
 
    def update(self, arg, values=("",), auto_reduce=True):
        """Interpret key-value pair; return False/None if not interpretable, else return True and update queries, projections"""
        (category, *fields), keyvalue = arg.split("."), {arg: values}
        if not is_safe_token(arg):
            raise GeneFabParserException("Forbidden argument", arg=arg)
        elif arg in CONTEXT_ARGUMENTS:
            parser = EmptyIterator
        elif category in KEYVALUE_PARSER_DISPATCHER():
            parser = KEYVALUE_PARSER_DISPATCHER()[category]
        else:
            raise GeneFabParserException("Unrecognized argument", **keyvalue)
        def _it():
            for value in values:
                if not is_safe_token(value, allow_regex=(arg=="file.filename")):
                    raise GeneFabParserException("Forbidden value", arg=value)
                else:
                    yield from parser(arg=arg, fields=fields, value=value)
        n_iterations = None
        for n_iterations, (query, projection_keys) in enumerate(_it(), 1):
            if query:
                self.query["$and"].append(query)
            self.projection.update({k: True for k in projection_keys})
        if auto_reduce:
            self.reduce_projection()
        return n_iterations
 
    def update_special_fields(self):
        """Automatically adjust projection and unwind pipeline for special fields ('file')"""
        if set(self.projection) & {"file", "file.datatype", "file.filename"}:
            special_projection = {"file.filename": True, "file.datatype": True}
            self.projection.update(special_projection)
            self.unwind.add("file")
 
    def reduce_projection(self):
        """Drop longer paths that are extensions of existing shorter paths"""
        tracer = BranchTracer(sep=r'\.')
        for key in sorted(self.projection, reverse=True):
            tracer.descend(key).make_terminal(truthy=True)
        self.projection = {
            key: v for key, v in self.projection.items() if tracer.descend(key)
        }
 
    def update_attributes(self):
        """Push remaining request arguments into self as attributes, set defaults"""
        for k, v in request.args.items():
            if k not in self.processed_args:
                if not hasattr(self, k):
                    setattr(self, k, v)
                else:
                    msg = "Cannot set context"
                    raise GeneFabConfigurationException(msg, **{k: v})
        self.format = getattr(self, "format", None)
        self.debug = getattr(self, "debug", "0")


class KeyValueParsers():
 
    def kvp_assay(arg, category, fields, value, fields_depth=1, constrain_to=None, mix_separator="."):
        """Interpret single key-value pair for dataset / assay constraint"""
        if (not fields) and value: # mixed syntax: 'id=GLDS-1|GLDS-2.assay-B'
            query, projection_keys = {"$or": []}, ()
            for expr in value.split("|"):
                if expr.count(mix_separator) == 0:
                    query["$or"].append({f"{category}.accession": expr})
                else:
                    accession, assay_name = expr.split(mix_separator, 1)
                    query["$or"].append({
                        f"{category}.accession": accession,
                        f"{category}.assay": assay_name,
                    })
            yield query, projection_keys
        else: # standard syntax: 'id', 'id.accession', 'id.assay=assayname', ...
            yield from KeyValueParsers.kvp_generic(
                arg=arg, fields_depth=fields_depth, constrain_to=constrain_to,
                category=category, fields=fields, value=value, dot_postfix=None,
            )
 
    def _infer_postfix(dot_postfix, fields):
        _auto_postfix_true = (dot_postfix == "auto") and (len(fields) == 2)
        return "." if ((dot_postfix is True) or _auto_postfix_true) else ""
 
    def kvp_generic(arg, category, fields, value, fields_depth=2, constrain_to=None, dot_postfix="auto"):
        """Interpret single key-value pair if it gives rise to database query"""
        if not fields:
            msg = "Category requires a subfield to be specified"
            raise GeneFabParserException(msg, **{arg: value}, category=category)
        elif constrain_to and (fields[0] not in constrain_to):
            msg = "Unrecognized field in argument"
            raise GeneFabParserException(msg, **{arg: value}, field=fields[0])
        elif len(fields) > fields_depth:
            msg = "Too many nested fields in argument"
            _kw = {"max_dots": fields_depth}
            raise GeneFabParserException(msg, **{arg: value}, **_kw)
        else:
            _dot_postfix = KeyValueParsers._infer_postfix(dot_postfix, fields)
            lookup_key, projection_key = arg + _dot_postfix, arg
        if value: # metadata field must equal value or one of values
            if is_regex(value):
                query = {lookup_key: {"$regex": value[1:-1]}}
            else:
                query = {lookup_key: {"$in": value.split("|")}}
            projection_keys = {projection_key}
        else: # metadata field or one of metadata fields must exist
            block_match = search(r'\.[^\.]+\.*$', lookup_key)
            if (not block_match) or (block_match.group().count("|") == 0):
                query = {lookup_key: {"$exists": True}} # single field exists
                projection_keys = {projection_key}
            else: # either of the fields exists (OR condition)
                head = lookup_key[:block_match.start()]
                targets = block_match.group().strip(".").split("|")
                projection_keys = {f"{head}.{target}" for target in targets}
                _pfx = "." if (block_match.group()[-1] == ".") else ""
                lookup_keys = {k+_pfx for k in projection_keys}
                query = {"$or": [{k: {"$exists": True}} for k in lookup_keys]}
        yield query, projection_keys
