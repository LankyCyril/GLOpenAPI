from functools import lru_cache, partial, reduce
from re import search, sub, compile
from glopenapi.common.exceptions import GLOpenAPIParserException, is_debug
from operator import getitem
from collections import defaultdict
from flask import request
from urllib.request import quote, unquote
from json import dumps
from glopenapi.common.utils import space_quote
from glopenapi.common.hacks import ForceShowAllDataColumns, HackyBlackHoleList
from glopenapi.common.exceptions import GLOpenAPIConfigurationException


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


EmptyIterator = lambda *a, **k: []
is_regex = lambda v: search(r'^\/.*\/$', v)
NonNaN = type("NonNaN", (object,), {})


def make_safe_mongo_token(token, allow_regex=False):
    """Quote special characters, ensure not a $-command."""
    if token is NonNaN:
        return token
    else:
        q_token = space_quote(token)
        if allow_regex and ("$" not in sub(r'\$\/$', "", q_token)):
            return q_token
        elif "$" not in q_token:
            return q_token
        else:
            raise GLOpenAPIParserException("Forbidden argument", field=q_token)


def make_safe_sql_name(name, arg="arg"):
    """Quote special characters; note: SQL injection prevention not necessary, as `name` must match existing column name"""
    return space_quote(name)


def make_safe_sql_value(value, arg="arg"):
    """Quote special characters; currently, only allow numeric values""" # TODO: allow other values, while preventing injections
    try:
        float(value)
    except ValueError:
        msg = "Only comparisons to numbers are currently supported"
        raise GLOpenAPIParserException(msg, **{arg: value})
    else:
        return value


BranchTracer = lambda sep: BranchTracerLevel(partial(BranchTracer, sep), sep)
BranchTracer.__doc__ = """Infinitely nestable and descendable defaultdict"""
class BranchTracerLevel(defaultdict):
    """Level of BranchTracer; creates nested levels by walking paths with sep"""
    def __init__(self, factory, sep):
        super().__init__(factory)
        self.split = compile(sep).split
    def descend(self, path, reduce=reduce, getitem=getitem):
        """Move one level down for each key in `path`; return terminal level"""
        return reduce(getitem, self.split(path), self)
    def make_terminal(self, truthy=True):
        """Prune descendants of current level, optionally marking self truthy"""
        self.clear()
        if truthy:
            self[True] = True # create a non-descendable element


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
        if self.full_path in {"/", "/?"}:
            self.identity = "root"
        else:
            self.identity = quote(dumps(sort_keys=True, separators=(",", ":"),
                obj={
                    "?": self.view, "query": self.query,
                    "sort_by": self.sort_by, "unwind": sorted(self.unwind),
                    "projection": self.projection,
                    "data_columns": self.data_columns,
                    "data_comparisons": self.data_comparisons,
                    "format": self.format, "schema": self.schema,
                    "debug": self.debug,
                }
            ))
        if (self.debug != "0") and (not is_debug()):
            raise GLOpenAPIParserException("Setting 'debug' is not allowed")
 
    def _choose_parser(self, arg, category, fields, values):
        """Choose appropriate parser from KEYVALUE_PARSER_DISPATCHER based on `category`, or bypass if `arg` is one of simple CONTEXT_ARGUMENTS"""
        if arg in CONTEXT_ARGUMENTS:
            return EmptyIterator
        elif category in KEYVALUE_PARSER_DISPATCHER():
            return KEYVALUE_PARSER_DISPATCHER()[category]
        else:
            _kw = {arg: values}
            raise GLOpenAPIParserException("Unrecognized argument", **_kw)
 
    def update(self, arg, values=("",), auto_reduce=True):
        """Interpret key-value pair; return False/None if not interpretable, else return True and update queries, projections"""
        category, *fields = map(make_safe_mongo_token, arg.split("."))
        if category == "": # "=a.b.c&=d.e.f" => a.b.c is NonNaN; d.e.f is NonNaN
            return sum( # sum of `n_iter` for each: a.b.c, d.e.f, ...
                self.update(shifted_arg, (NonNaN,), auto_reduce=auto_reduce)
                for shifted_arg in values
            )
        else:
            parser = self._choose_parser(arg, category, fields, values)
            def _it():
                _kw = dict(arg=arg, fields=fields)
                _make_safe_mongo_token = partial(
                    make_safe_mongo_token, allow_regex=(arg=="file.filename"),
                )
                for value in map(_make_safe_mongo_token, values):
                    yield from parser(value=value, **_kw)
            n_iter, _en_it = None, enumerate(_it(), 1)
            for n_iter, (query, projection_keys, cols, comparisons) in _en_it:
                self.projection.update({k: True for k in projection_keys})
                if query:
                    self.query.setdefault("$and", list()).append(query)
                if cols or comparisons:
                    if self.view == "data":
                        _already_present = set(self.data_columns)
                        for col in cols:
                            if col is ForceShowAllDataColumns:
                                self.data_columns = HackyBlackHoleList()
                            elif col not in _already_present:
                                # TODO: recall, why not .append(col) here?
                                self.data_columns.extend(cols)
                        self.data_comparisons.extend(comparisons)
                    else:
                        msg = "Column queries are only valid for /data/"
                        raise GLOpenAPIParserException(msg)
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
            safe_v = make_safe_mongo_token(v)
            if k not in self.processed_args:
                if not hasattr(self, k):
                    setattr(self, k, safe_v)
                else:
                    msg = "Cannot set context"
                    raise GLOpenAPIConfigurationException(msg, **{k: safe_v})
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
            m = "Category requires a subfield to be specified"
            raise GLOpenAPIParserException(m, **{arg: value}, category=category)
        elif constrain_to and (fields[0] not in constrain_to):
            m = "Unrecognized field in argument"
            raise GLOpenAPIParserException(m, **{arg: value}, field=fields[0])
        elif len(fields) > fields_depth:
            m = "Too many nested fields in argument"
            _kw = {"max_dots": fields_depth}
            raise GLOpenAPIParserException(m, **{arg: value}, **_kw)
        else:
            _dot_postfix = KeyValueParsers._infer_postfix(dot_postfix, fields)
            lookup_key, projection_key = arg + _dot_postfix, arg
            block_match = search(r'\.[^\.]+\.*$', lookup_key)
        if (not block_match) or (block_match.group().count("|") == 0):
            is_multikey, projection_keys = False, {projection_key}
        else:
            is_multikey, head = True, lookup_key[:block_match.start()]
            targets = block_match.group().strip(".").split("|")
            projection_keys = {f"{head}.{target}" for target in targets}
        if value: # metadata field must equal value or one of values
            if value is NonNaN: # not being NaN is same as existing
                if is_multikey: # allow checking $exists for multiple fields
                    _real_dot = "." if (block_match.group()[-1] == ".") else ""
                    query = {"$or": [{k+_real_dot: {"$exists": True}}
                        for k in projection_keys]}
                else:
                    query = {lookup_key : {"$exists": True}}
            elif is_regex(value):
                query = {lookup_key: {"$regex": unquote(value)[1:-1]}}
            else:
                query = {lookup_key: {"$in": unquote(value).split("|")}}
        else: # metadata field(s) should be included in output regardless
            query = None
        yield query, projection_keys, None, None
 
    def kvp_column(arg, category, fields, value):
        """Interpret data table constraint"""
        if not fields:
            msg, v = "Constraint requires a subfield to be specified", value
            raise GLOpenAPIParserException(msg, **{arg: v}, constraint=category)
        else:
            expr = f"{'.'.join(fields)}={value}" if value else ".".join(fields)
            unq_expr = unquote(expr)
        if unq_expr == "*":
            yield None, (), [ForceShowAllDataColumns], ()
        elif not ({"<", "=", ">"} & set(unq_expr)):
            yield None, (), [make_safe_sql_name(expr)], ()
        else:
            match = search(r'^([^<>=]+)(<|<=|=|==|>=|>)([^<>=]*)$', unq_expr)
            if match:
                name = make_safe_sql_name(match.group(1), arg=arg)
                op = match.group(2)
                value = make_safe_sql_value(match.group(3), arg=arg)
                yield None, (), [name], [f"`{name}` {op} {value}"]
            else:
                msg = "Unparseable expression"
                raise GLOpenAPIParserException(msg, expression=expr)
