from pandas import isnull, DataFrame
from re import sub, search
from functools import partial, reduce, wraps
from bson.errors import InvalidDocument as InvalidDocumentError
from collections.abc import ValuesView
from genefab3.common.logger import GeneFabLogger
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.types import NestedDefaultDict
from operator import getitem as gi_
from collections import OrderedDict
from marshal import dumps as marshals
from pymongo import ASCENDING


def isempty(v):
    """Check if terminal leaf value is a null value or an empty string"""
    return isnull(v) or (v == "")


def is_safe_token(v, allow_regex=False):
    """Check if value is safe for PyMongo queries"""
    return "$" not in (sub(r'\$\/$', "", v) if allow_regex else v)


def is_regex(v):
    """Check if value is a regex"""
    return search(r'^\/.*\/$', v)


def is_unit_formattable(e, unit_key):
    """Check if entry `e` contains keys "" and unit_key and that `e[unit_key]` is not empty"""
    return ("" in e) and (unit_key in e) and (not isempty(e[unit_key]))


def format_units(e, unit_key, units_formatter):
    """Replace `e[""]` with value with formatted `e[unit_key]`, discard `e[unit_key]`"""
    return {
        k: units_formatter(value=v, unit=e[unit_key]) if k == "" else v
        for k, v in e.items() if k != unit_key
    }


def harmonize_document(query, units_formatter=None, lowercase=True, dropna=True, depth_tracker=0, *, max_depth=32):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.'), normalize case, format units, drop terminal NaNs"""
    unit_key = "unit" if lowercase else "Unit"
    harmonizer_function = partial(
        harmonize_document, lowercase=lowercase, dropna=dropna,
        units_formatter=units_formatter, depth_tracker=depth_tracker+1,
    )
    if depth_tracker >= max_depth:
        raise InvalidDocumentError("Document exceeds maximum depth", max_depth)
    elif isinstance(query, dict):
        harmonized = {}
        for key, branch in query.items():
            harmonized_key = key.replace("$", "_").replace(".", "_")
            if lowercase:
                harmonized_key = harmonized_key.lower()
            if harmonized_key == "_id":
                harmonized_key = "__id"
            if harmonized_key not in harmonized:
                harmonized_branch = harmonizer_function(branch)
                if harmonized_branch:
                    harmonized[harmonized_key] = harmonized_branch
            else:
                raise InvalidDocumentError("Harmonized keys conflict")
        if units_formatter and is_unit_formattable(harmonized, unit_key):
            return format_units(harmonized, unit_key, units_formatter)
        else:
            return harmonized
    elif isinstance(query, (list, ValuesView)):
        return [hq for hq in (harmonizer_function(q) for q in query) if hq]
    elif (not dropna) or (not isempty(query)):
        return query
    else:
        return {}


def run_mongo_transaction(action, collection, *, query=None, data=None, documents=None):
    """Shortcut to replace/delete/insert all matching instances in one transaction"""
    error_message, unused_arguments = None, None
    with collection.database.client.start_session() as session:
        with session.start_transaction():
            if action == "replace":
                if (query is not None) and (data is not None):
                    collection.delete_many(query)
                    collection.insert_one({**query, **data})
                    if documents is not None:
                        unused_arguments = "`documents`"
                else:
                    error_message = "no `query` and/or `data` specified"
            elif action == "delete_many":
                if query is not None:
                    collection.delete_many(query)
                    if (data is not None) or (documents is not None):
                        unused_arguments = "`data`, `documents`"
                else:
                    error_message = "no `query` specified"
            elif action == "insert_many":
                if documents is not None:
                    collection.insert_many(documents)
                    if (query is not None) or (data is not None):
                        unused_arguments = "`query`, `data`"
                else:
                    error_message = "no `documents` specified"
            else:
                error_message = "unsupported action"
    if unused_arguments:
        message = "run_mongo_transaction('%s'): %s unused in this action"
        GeneFabLogger().warning(message, action, unused_arguments)
    if error_message:
        raise GeneFabDatabaseException(
            error_message, action=action, collection=collection,
            query=query, data=data, documents=documents,
        )


def reduce_projection(fp, longest=False):
    """Drop longer OR shorter paths if they conflict with longer paths"""
    d = NestedDefaultDict()
    [reduce(gi_, k.split("."), d) for k in fp]
    if longest:
        return {k: v for k, v in fp.items() if not reduce(gi_, k.split("."), d)}
    else:
        for k in sorted(fp, reverse=True):
            v = reduce(gi_, k.split("."), d)
            v[True] = [v.clear() if v else None]
        return {k: v for k, v in fp.items() if reduce(gi_, k.split("."), d)}


def _iter_blackjack_items_cache(f):
    """Custom lru_cache-like memoizer for `iter_blackjack_items` with hashing of simple dictionaries"""
    cache = OrderedDict()
    @wraps(f)
    def wrapper(e, head=()):
        k = marshals(e, 4), head
        if k not in cache:
            if len(cache) > 4096:
                cache.popitem(last=False)
            cache[k] = list(f(e, head))
        return cache[k]
    return wrapper


@_iter_blackjack_items_cache
def iter_blackjack_items(e, head=()):
    """Quickly iterate flattened dictionary key-value pairs in pure Python"""
    if isinstance(e, dict):
        for k, v in e.items():
            yield from iter_blackjack_items(v, head=head+(k,))
    else:
        yield ".".join(head), e


def blackjack_normalize(cursor):
    """Quickly flatten iterable of dictionaries in pure Python"""
    return DataFrame(dict(iter_blackjack_items(e)) for e in cursor)


def retrieve_by_context(collection, query, full_projection, sortby, locale):
    """Run .find() or .aggregate() based on query, projection"""
    return blackjack_normalize(collection.aggregate(
        pipeline=[
            {"$sort": {f: ASCENDING for f in sortby}},
            {"$unwind": "$file"}, {"$match": query},
            {"$project": {**full_projection, "_id": False}},
        ],
        collation={"locale": locale, "numericOrdering": True},
    ))
