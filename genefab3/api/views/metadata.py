from genefab3.db.mongo.utils import blackjack_normalize, retrieve_by_context
from pandas import MultiIndex, isnull
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException
from re import findall
from genefab3.api.renderers import Placeholders
from genefab3.common.utils import set_attributes


def get_raw_metadata_dataframe(mongo_collections, *, locale, context, include):
    """Get target metadata as a single-level dataframe, numerically sorted by info fields"""
    cursor, full_projection = retrieve_by_context(
        mongo_collections.metadata, locale=locale, context=context,
        include=include,
    )
    try:
        dataframe = blackjack_normalize(cursor)
    except MongoOperationError as e:
        errmsg = getattr(e, "details", {}).get("errmsg", "").lower()
        has_index = ("info" in mongo_collections.metadata.index_information())
        index_reason = ("index" in errmsg)
        if index_reason and (not has_index):
            msg = "Metadata is not indexed yet; this is a temporary error"
        else:
            msg = "Could not retrieve sorted metadata"
        raise GeneFabDatabaseException(msg, locale=locale, reason=str(e))
    else:
        return dataframe, full_projection


def INPLACE_drop_trailing_fields(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested in projection"""
    is_trailing = lambda c: (
        (c not in full_projection) and (len(findall(r'\..', c)) >= 3)
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
        _kw = dict(include=include, object_type="annotation")
        return Placeholders.metadata_dataframe(**_kw)
    else:
        dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
        dataframe = iisaf_sort_dataframe(dataframe)
        dataframe.columns = MultiIndex.from_tuples(
            (fields[0], ".".join(fields[1:])) if fields[0] in {"info", "file"}
            else (".".join(fields[:2]), ".".join(fields[2:]) or "*")
            for fields in map(lambda s: s.split("."), dataframe.columns)
        )
        if aggregate: # coerce to boolean "existence" if requested
            info_cols = list(dataframe[["info"]].columns)
            if len(info_cols) == dataframe.shape[1]: # only 'info' cols present
                dataframe = dataframe.drop_duplicates()
            else: # metadata cols present and can be collapsed into booleans
                gby = dataframe.groupby(info_cols, as_index=False, sort=False)
                dataframe = gby.agg(lambda a: ~isnull(a).all())
        set_attributes(
            dataframe, object_type="annotation", accessions=set(
                dataframe[("info", "accession")].drop_duplicates(),
            ),
        )
        return dataframe
