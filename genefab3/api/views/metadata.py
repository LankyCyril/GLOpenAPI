from pymongo import ASCENDING
from pandas import json_normalize, MultiIndex, isnull
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException
from re import findall
from genefab3.api.renderers import Placeholders
from genefab3.common.utils import set_attributes


def get_raw_metadata_dataframe(mongo_collections, *, locale, query, projection, include):
    """Get target metadata as a single-level dataframe, numerically sorted by info fields"""
    sort = [(f, ASCENDING) for f in ["info.accession", "info.assay", *include]]
    entries = mongo_collections.metadata.find(
        query, projection, sort=sort,
        collation={"locale": locale, "numericOrdering": True},
    )
    try:
        return json_normalize(list(entries))
    except MongoOperationError as e:
        has_index = ("info" in mongo_collections.metadata.index_information())
        index_reason = (
            "index" in getattr(e, "details", {}).get("errmsg", "").lower()
        )
        if index_reason and (not has_index):
            msg = "Metadata is not indexed yet; this is a temporary error"
        else:
            msg = "Could not retrieve sorted metadata"
        raise GeneFabDatabaseException(msg, locale=locale, reason=str(e))


def INPLACE_drop_non_projected_columns(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested in projection"""
    dataframe.drop(inplace=True, columns={
        c for c in list(dataframe.columns)
        if (len(findall(r'\..', c)) >= 3) and (c not in full_projection)
    })


def iisaf_sort_dataframe(dataframe):
    """Sort single-level dataframe in order info-investigation-study-assay-file"""
    prefix_order = ["info", "investigation", "study", "assay", "file", "other"]
    column_order = {prefix: set() for prefix in prefix_order}
    for column in dataframe.columns:
        for prefix in column_order:
            if column.startswith(prefix):
                column_order[prefix].add(column)
                break
        else:
            column_order["other"].add(column)
    return dataframe[
        sum((sorted(column_order[prefix]) for prefix in prefix_order), [])
    ]


def get(mongo_collections, *, locale, context, include=(), aggregate=False):
    """Select assays/samples based on annotation filters"""
    full_projection = {
        "info.accession": True, "info.assay": True,
        **context.projection, **{field: True for field in include},
    }
    dataframe = get_raw_metadata_dataframe( # single-level
        mongo_collections, locale=locale, query=context.query,
        projection={**full_projection, "_id": False}, include=include,
    )
    # drop trailing qualifier fields not requested in projection:
    INPLACE_drop_non_projected_columns(dataframe, full_projection)
    if not (dataframe.shape[0] * dataframe.shape[1]): # empty
        _kw = dict(include=include, genefab_type="annotation")
        return Placeholders.metadata_dataframe(**_kw)
    else:
        # remove trailing dots and hide columns that are explicitly hidden:
        dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
        # sort (ISA-aware) and convert to two-level dataframe:
        dataframe = iisaf_sort_dataframe(dataframe)
        dataframe.columns = MultiIndex.from_tuples(
            (fields[0], ".".join(fields[1:])) if fields[0] == "info"
            else (".".join(fields[:2]), ".".join(fields[2:]))
            for fields in map(lambda s: s.split("."), dataframe.columns)
        )
        for INPLACE_postprocess_fn in context.INPLACE_postprocess_functions:
            INPLACE_postprocess_fn(dataframe)
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
