from functools import lru_cache, partial
from flask import request
from urllib.request import quote, unquote
from json import dumps
from genefab3.common.utils import EmptyIterator, BranchTracer
from genefab3.common.exceptions import is_debug, GeneFabParserException
from genefab3.common.utils import make_safe_token, space_quote, is_regex
from genefab3.common.exceptions import GeneFabConfigurationException
from re import search


CONTEXT_ARGUMENTS = {"debug": "0", "format": None, "schema": "0"}

KEYVALUE_PARSER_DISPATCHER = lru_cache(maxsize=1)(lambda: {
    "id": partial(KeyValueParsers.kvp_assay,
        category="id", fields_depth=1,
        constrain_to=["accession", "assay name", "sample name", "study name"],
    ),
    "investigation": partial(KeyValueParsers.kvp_generic,
        category="investigation", fields_depth=3, dot_postfix=None,
        constrain_to=["investigation", "study", "study assays"],
    ),
    "study": partial(KeyValueParsers.kvp_generic,
        category="study", fields_depth=2,
        constrain_to=["characteristics", "factor value", "parameter value"],
    ),
    "assay": partial(KeyValueParsers.kvp_generic,
        category="assay", fields_depth=2,
        constrain_to=["characteristics", "factor value", "parameter value"],
    ),
    "file": partial(KeyValueParsers.kvp_generic,
        category="file", fields_depth=1, constrain_to=["datatype", "filename"],
    ),
    "column": partial(KeyValueParsers.kvp_column, category="column"),
    "c": partial(KeyValueParsers.kvp_column, category="column"),
})


class Context():
    """Stores request components parsed into MongoDB queries, projections"""
 
    def __init__(self, flask_app):
        """Parse request components"""
        self.app_name = flask_app.name
        self.complete_kwargs = request.args.to_dict(flat=False)
        self.url_root = request.url_root.rstrip("/")
        self.view, self.full_path = request.path.strip("/"), request.full_path
        self.query, self.unwind = {"$and": []}, set()
        self.projection = {"id.accession": True, "id.assay name": True}
        self.sort_by = ["id.accession", "id.assay name"]
        self.data_columns, self.data_comparisons = [], []
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
            "data_columns": self.data_columns,
            "data_comparisons": self.data_comparisons,
            "format": self.format, "schema": self.schema, "debug": self.debug,
        }))
        if self.debug != "0" and (not is_debug()):
            raise GeneFabParserException("Setting 'debug' is not allowed")
 
    def update(self, arg, values=("",), auto_reduce=True):
        """Interpret key-value pair; return False/None if not interpretable, else return True and update queries, projections"""
        category, *fields = map(make_safe_token, arg.split("."))
        if arg in CONTEXT_ARGUMENTS:
            parser = EmptyIterator
        elif category in KEYVALUE_PARSER_DISPATCHER():
            parser = KEYVALUE_PARSER_DISPATCHER()[category]
        else:
            _kw = {arg: values}
            raise GeneFabParserException("Unrecognized argument", **_kw)
        def _it():
            allow_regex = (arg=="file.filename")
            _make_safe_token = partial(make_safe_token, allow_regex=allow_regex)
            for value in map(_make_safe_token, values):
                yield from parser(arg=arg, fields=fields, value=value)
        n_iter, _en_it = None, enumerate(_it(), 1)
        for n_iter, (query, projection_keys, columns, comparisons) in _en_it:
            self.projection.update({k: True for k in projection_keys})
            if query:
                if "$and" not in self.query:
                    self.query["$and"] = []
                self.query["$and"].append(query)
            if columns or comparisons:
                if self.view == "data":
                    _already_present = set(self.data_columns)
                    for column in columns:
                        if column not in _already_present:
                            self.data_columns.extend(columns)
                    self.data_comparisons.extend(comparisons)
                else:
                    msg = "Column queries are only valid for /data/"
                    raise GeneFabParserException(msg)
        if auto_reduce:
            self.reduce_projection()
        return n_iter
 
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
            safe_v = make_safe_token(v)
            if k not in self.processed_args:
                if not hasattr(self, k):
                    setattr(self, k, safe_v)
                else:
                    msg = "Cannot set context"
                    raise GeneFabConfigurationException(msg, **{k: safe_v})
        for k, v in CONTEXT_ARGUMENTS.items():
            setattr(self, k, getattr(self, k, v))


class KeyValueParsers():
 
    def kvp_assay(arg, category, fields, value, fields_depth=1, constrain_to=None, mix_separator="/"):
        """Interpret single key-value pair for dataset / assay constraint"""
        if (not fields) and value: # mixed syntax: 'id=GLDS-1|GLDS-2/assay-B'
            query, projection_keys = {"$or": []}, ()
            for expr in unquote(value).split("|"):
                query["$or"].append({
                    f"{category}.{field}": part for field, part in zip(
                        ["accession", "assay name", "sample name"],
                        expr.split(mix_separator, 2),
                    )
                })
            yield query, projection_keys, None, None
        else: # standard syntax: 'id', 'id.accession', 'id.assay name=name', ...
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
                query = {lookup_key: {"$regex": unquote(value)[1:-1]}}
            else:
                query = {lookup_key: {"$in": unquote(value).split("|")}}
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
        yield query, projection_keys, None, None
 
    def kvp_column(arg, category, fields, value):
        """Interpret data table constraint"""
        if not fields:
            msg, v = "Constraint requires a subfield to be specified", value
            raise GeneFabParserException(msg, **{arg: v}, constraint=category)
        else:
            expr = f"{'.'.join(fields)}={value}" if value else ".".join(fields)
            unq_expr = unquote(expr)
        if not ({"<", "=", ">"} & set(unq_expr)):
            yield None, (), [expr], ()
        else:
            match = search(r'^([^<>=]+)(<|<=|=|==|>=|>)([^<>=]*)$', unq_expr)
            if match:
                name, op, value = match.groups()
                try:
                    float(value)
                except ValueError:
                    msg = "Only comparisons to numbers are currently supported"
                    raise GeneFabParserException(msg, **{arg: value})
                else:
                    yield None, (), (), [f"`{space_quote(name)}` {op} {value}"]
            else:
                msg = "Unparseable expression"
                raise GeneFabParserException(msg, expression=expr)
