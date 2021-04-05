from pymongo import ASCENDING
from pandas import json_normalize, MultiIndex, isnull
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException
from re import findall
from genefab3.api.renderers import Placeholders
from genefab3.common.utils import set_attributes


def get_raw_metadata_dataframe(mongo_collections, *, locale, context, include):
    """Get target metadata as a single-level dataframe, numerically sorted by info fields"""
    sortby = ["info.accession", "info.assay", *include]
    full_projection = {
        "info.accession": True, "info.assay": True,
        **context.projection, **{field: True for field in include},
    }
    if context.pipeline:
        cursor = mongo_collections.metadata.aggregate(
            pipeline=[
                {"$sort": {f: ASCENDING for f in sortby}},
                *context.pipeline, {"$match": context.query},
                {"$project": {**full_projection, "_id": False}},
            ],
            collation={"locale": locale, "numericOrdering": True},
        )
    else:
        cursor = mongo_collections.metadata.find(
            context.query, {**full_projection, "_id": False},
            sort=[(f, ASCENDING) for f in sortby],
            collation={"locale": locale, "numericOrdering": True},
        )
    try:
        return json_normalize(list(cursor)), full_projection
    except MongoOperationError as e:
        errmsg = getattr(e, "details", {}).get("errmsg", "").lower()
        has_index = ("info" in mongo_collections.metadata.index_information())
        index_reason = ("index" in errmsg)
        if index_reason and (not has_index):
            msg = "Metadata is not indexed yet; this is a temporary error"
        else:
            msg = "Could not retrieve sorted metadata"
        raise GeneFabDatabaseException(msg, locale=locale, reason=str(e))


def INPLACE_drop_trailing_fields(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested in projection"""
    is_trailing = lambda c:(
        (c not in full_projection) and
        (len(findall(r'\..', c)) + c.startswith("file.filename") >= 3)
    )
    dataframe.drop(inplace=True, columns=filter(is_trailing, dataframe.columns))


def iisaf_sort_dataframe(dataframe):
    """Sort single-level dataframe in order info-investigation-study-assay-file"""
    prefix_order = ["info", "investigation", "study", "assay", "file", ""]
    column_order = {p: set() for p in prefix_order}
    for column in dataframe.columns:
        for prefix in prefix_order:
            if column.startswith(prefix):
                column_order[prefix].add(column)
                break
    return dataframe[sum((sorted(column_order[p]) for p in prefix_order), [])]


def get(mongo_collections, *, locale, context, include=(), drop_trailing_fields=True, aggregate=False):
    """Select assays/samples based on annotation filters"""
    dataframe, full_projection = get_raw_metadata_dataframe( # single-level
        mongo_collections, locale=locale, context=context, include=include,
    )
    if drop_trailing_fields:
        INPLACE_drop_trailing_fields(dataframe, full_projection)
    if dataframe.empty:
        _kw = dict(include=include, genefab_type="annotation")
        return Placeholders.metadata_dataframe(**_kw)
    else:
        dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
        dataframe = iisaf_sort_dataframe(dataframe)
        dataframe.columns = MultiIndex.from_tuples(
            (fields[0], ".".join(fields[1:])) if fields[0] == "info"
            else (".".join(fields[:2]), ".".join(fields[2:]))
            for fields in map(lambda s: s.split("."), dataframe.columns)
        )
        if aggregate: # coerce to boolean "existence" if requested
            info_cols = list(dataframe[["info"]].columns)
            if len(info_cols) == dataframe.shape[1]: # only 'info' cols present
                return dataframe.drop_duplicates()
            else: # metadata cols present and can be collapsed into booleans
                gby = dataframe.groupby(info_cols, as_index=False, sort=False)
                return gby.agg(lambda a: ~isnull(a).all())
        else:
            set_attributes(dataframe, genefab_type="annotation")
            return dataframe
