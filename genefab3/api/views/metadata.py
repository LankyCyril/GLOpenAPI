from genefab3.db.mongo.utils import aggregate_entries_by_context
from genefab3.common.utils import blackjack_normalize
from pandas import MultiIndex, isnull
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.api.renderers import Placeholders
from genefab3.common.types import AnnotationDataFrame


def get_raw_metadata_dataframe(mongo_collections, *, locale, context, id_fields):
    """Get target metadata as a single-level dataframe, numerically sorted by id fields"""
    cursor, full_projection = aggregate_entries_by_context(
        mongo_collections.metadata, locale=locale, context=context,
        id_fields=id_fields,
    )
    try:
        dataframe = blackjack_normalize(cursor, max_level=2)
    except MongoOperationError as e:
        errmsg = getattr(e, "details", {}).get("errmsg", "").lower()
        has_index = ("id" in mongo_collections.metadata.index_information())
        index_reason = ("index" in errmsg)
        if index_reason and (not has_index):
            msg = "Metadata is not indexed yet; this is a temporary error"
        else:
            msg = "Could not retrieve sorted metadata"
        raise GeneFabDatabaseException(msg, locale=locale, reason=str(e))
    else:
        return dataframe, full_projection


def iisaf_sort_dataframe(dataframe):
    """Sort single-level dataframe in order id-investigation-study-assay-file"""
    prefix_order = ["id", "investigation", "study", "assay", "file", ""]
    column_order = {p: set() for p in prefix_order}
    for column in dataframe.columns:
        for prefix in prefix_order:
            if column.startswith(prefix):
                column_order[prefix].add(column)
                break
    return dataframe[sum((sorted(column_order[p]) for p in prefix_order), [])]


def INPLACE_set_id_as_index(dataframe):
    """Move all columns with first level value of "id" into MultiIndex"""
    if "id" in dataframe.columns.get_level_values(0):
        dataframe.index = MultiIndex.from_frame(
            dataframe["id"], names=dataframe[["id"]].columns,
        )
        dataframe.drop(columns="id", inplace=True)


def get(*, mongo_collections, locale, context, id_fields, aggregate=False):
    """Select assays/samples based on annotation filters"""
    dataframe, full_projection = get_raw_metadata_dataframe( # single-level
        mongo_collections, locale=locale, context=context, id_fields=id_fields,
    )
    if dataframe.empty:
        return Placeholders.EmptyAnnotationDataFrame(id_fields=id_fields)
    else:
        dataframe = iisaf_sort_dataframe(dataframe)
        dataframe.columns = MultiIndex.from_tuples(
            (fields[0], ".".join(fields[1:])) if fields[0] in {"id", "file"}
            else (".".join(fields[:2]), ".".join(fields[2:]) or "*")
            for fields in map(lambda s: s.split("."), dataframe.columns)
        )
        if aggregate: # coerce to boolean "existence" if requested
            info_cols = list(dataframe[["id"]].columns)
            if len(info_cols) == dataframe.shape[1]: # only 'id' cols present
                dataframe = dataframe.drop_duplicates()
            else: # metadata cols present and can be collapsed into booleans
                gby = dataframe.groupby(info_cols, as_index=False, sort=False)
                dataframe = gby.agg(lambda a: ~isnull(a).all())
        INPLACE_set_id_as_index(dataframe)
        return AnnotationDataFrame(dataframe)
