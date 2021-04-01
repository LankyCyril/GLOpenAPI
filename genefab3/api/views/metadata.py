from pymongo import ASCENDING
from pandas import json_normalize, MultiIndex, isnull
from genefab3.common.exceptions import GeneFabDatabaseException
from re import findall
from genefab3.api.renderers import Placeholders
from genefab3.common.utils import set_attributes


def get_raw_metadata_dataframe(mongo_collections, *, locale, query, projection, include):
    """Get target metadata as a single-level dataframe, numerically sorted by info fields"""
    #if False:
    ## TODO see genefab3/legacy/backend/mongo/readers/metadata.py
    #    for _ in range(0, METADATA_INDEX_WAIT_DELAY, METADATA_INDEX_WAIT_STEP):
    #        if "info" in metadata_collection.index_information():
    #            break
    #        else:
    #            sleep(METADATA_INDEX_WAIT_STEP)
    #    else:
    #        raise GeneFabDatabaseException(
    #            "Could not retrieve sorted metadata (no index found)",
    #        )
    #sort = [(f, ASCENDING) for f in ["info.accession", "info.assay", *include]]
    entries = mongo_collections.metadata.find(
        query, projection, #sort=sort,
        #collation={"locale": locale, "numericOrdering": True},
    )
    try:
        return json_normalize(list(entries))
    except Exception as e:
        msg = "Could not retrieve sorted metadata"
        raise GeneFabDatabaseException(msg, locale=locale, reason=str(e))


def keep_projection(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested"""
    return dataframe.drop(columns={
        c for c in dataframe.columns
        if (len(findall(r'\..', c)) >= 3) and (c not in full_projection)
    })


def isa_sort_dataframe(dataframe):
    """Sort single-level dataframe in order info-investigation-study-assay"""
    column_order = dict(
        info=set(), investigation=set(), study=set(), assay=set(), other=set(),
    )
    for column in dataframe.columns:
        if column.startswith("info"):
            column_order["info"].add(column)
        elif column.startswith("investigation"):
            column_order["investigation"].add(column)
        elif column.startswith("study"):
            column_order["study"].add(column)
        elif column.startswith("assay"):
            column_order["assay"].add(column)
        else:
            column_order["other"].add(column)
    return dataframe[(
        sorted(column_order["info"]) + sorted(column_order["investigation"]) +
        sorted(column_order["study"]) + sorted(column_order["assay"]) +
        sorted(column_order["other"])
    )]


def get(mongo_collections, *, locale, context, include=(), modify=keep_projection, aggregate=False):
    """Select assays/samples based on annotation filters"""
    full_projection = {
        "info.accession": True, "info.assay": True, **context.projection,
        **{field: True for field in include},
    }
    try:
        dataframe = get_raw_metadata_dataframe( # single-level
            mongo_collections, locale=locale, query=context.query,
            projection={**full_projection, "_id": False}, include=include,
        )
        # modify with injected function:
        dataframe = modify(dataframe, full_projection)
        # remove trailing dots and hide columns that are explicitly hidden:
        dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
        # sort (ISA-aware) and convert to two-level dataframe:
        dataframe = isa_sort_dataframe(dataframe)
        dataframe.columns = MultiIndex.from_tuples(
            (fields[0], ".".join(fields[1:])) if fields[0] == "info"
            else (".".join(fields[:2]), ".".join(fields[2:]))
            for fields in map(lambda s: s.split("."), dataframe.columns)
        )
    except TypeError: # no data retrieved; TODO: handle more gracefully
        return Placeholders.metadata_dataframe(
            include=include, genefab_type="annotation",
        )
    else:
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
