from genefab3.config import METADATA_INDEX_WAIT_DELAY, METADATA_INDEX_WAIT_STEP
from time import sleep
from genefab3.config import MONGO_DB_LOCALE, RAW_FILE_REGEX, COLLECTION_NAMES
from pymongo import ASCENDING
from pandas import json_normalize, MultiIndex, isnull, concat, merge
from genefab3.common.exceptions import GeneLabDatabaseException
from types import SimpleNamespace
from re import findall, search, IGNORECASE, escape, split
from genefab3.frontend.renderer import Placeholders
from numpy import nan


def get_raw_meta_dataframe(mongo_db, query, projection, include, cname=COLLECTION_NAMES.METADATA):
    """Get target metadata as a single-level dataframe, numerically sorted by info fields"""
    metadata_collection = getattr(mongo_db, cname)
    for _ in range(0, METADATA_INDEX_WAIT_DELAY, METADATA_INDEX_WAIT_STEP):
        if "info" in metadata_collection.index_information():
            break
        else:
            sleep(METADATA_INDEX_WAIT_STEP)
    else:
        raise GeneLabDatabaseException(
            "Could not retrieve sorted metadata (no index found)",
        )
    entries = metadata_collection.find(
        query, projection,
        sort=[
            (field, ASCENDING)
            for field in ["info.accession", "info.assay", *include]
        ],
        collation={
            "locale": MONGO_DB_LOCALE, "numericOrdering": True,
        },
    )
    try:
        return json_normalize(list(entries))
    except Exception as e:
        raise GeneLabDatabaseException(
            "Could not retrieve sorted metadata",
            locale=MONGO_DB_LOCALE, reason=str(e),
        )


def keep_projection(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested"""
    subkeys_to_drop = {
        c for c in dataframe.columns
        if (len(findall(r'\..', c)) >= 3) and (c not in full_projection)
    }
    return dataframe.drop(columns=subkeys_to_drop)


def unwind_file_entry(cell):
    """Unwind lists of dicts returned for some complex unfiltered fields from database"""
    if isinstance(cell, list):
        try:
            return cell[0][""]
        except (KeyError, IndexError):
            return str(cell)
    else:
        return cell


def keep_files(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested OR contain filenames"""
    subkeys_to_drop = {
        c for c in dataframe.columns if (
            (c not in full_projection) and
            (not search(RAW_FILE_REGEX, c, flags=IGNORECASE))
        )
    }
    return dataframe.drop(columns=subkeys_to_drop).applymap(unwind_file_entry)


def isa_sort_dataframe(dataframe):
    """Sort single-level dataframe in order info-investigation-study-assay"""
    column_order = SimpleNamespace(
        info=set(), investigation=set(), study=set(), assay=set(), other=set(),
    )
    for column in dataframe.columns:
        if column.startswith("info"):
            column_order.info.add(column)
        elif column.startswith("investigation"):
            column_order.investigation.add(column)
        elif column.startswith("study"):
            column_order.study.add(column)
        elif column.startswith("assay"):
            column_order.assay.add(column)
        else:
            column_order.other.add(column)
    return dataframe[(
        sorted(column_order.info) + sorted(column_order.investigation) +
        sorted(column_order.study) + sorted(column_order.assay) +
        sorted(column_order.other)
    )]


def get_annotation_by_metas(mongo_db, context, include=(), search_with_projection=True, modify=keep_projection, aggregate=False):
    """Select assays/samples based on annotation filters"""
    full_projection = {
        "info.accession": True, "info.assay": True, **context.projection,
        **{field: True for field in include},
    }
    try:
        if search_with_projection:
            proj = {"_id": False, **full_projection}
        else:
            proj = {"_id": False}
        dataframe = get_raw_meta_dataframe( # single-level
            mongo_db, context.query, proj, include,
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
        return Placeholders.metadata_dataframe(include)
    else:
        if aggregate: # coerce to boolean "existence" if requested
            info_cols = list(dataframe[["info"]].columns)
            if len(info_cols) == dataframe.shape[1]: # only 'info' cols present
                return dataframe.drop_duplicates()
            else: # metadata cols present and can be collapsed into booleans
                gby = dataframe.groupby(info_cols, as_index=False, sort=False)
                return gby.agg(lambda a: ~isnull(a).all())
        else:
            return dataframe


def filter_filenames(dataframe, mask, startloc):
    """Constrain dataframe cells only to cells passing `mask` filter"""
    if mask is None:
        return dataframe
    else:
        if (mask[0] == "/") and (mask[-1] == "/"): # regular expression passed
            expression = mask[1:-1]
        else: # simple filename passed, match full
            expression = r'^' + escape(mask) + r'$'
        def mapper(cell):
            if isinstance(cell, str):
                return ", ".join({
                    filename for filename in split(r'\s*,\s*', cell)
                    if search(expression, filename)
                }) or nan
            else:
                return nan
        return concat(
            objs=[
                dataframe.iloc[:,:startloc],
                dataframe.iloc[:,startloc:].applymap(mapper).dropna(
                    how="all", axis=1,
                ),
            ],
            axis=1,
        )


def get_assays_by_metas(mongo_db, context):
    """Select assays based on annotation filters"""
    return get_annotation_by_metas(mongo_db, context, aggregate=True)


def get_samples_by_metas(mongo_db, context):
    """Select samples based on annotation filters"""
    return get_annotation_by_metas(
        mongo_db, context, include={"info.sample name"},
    )


def get_files_by_metas(mongo_db, context):
    """Select files based on annotation filters"""
    return merge(
        get_annotation_by_metas(
            mongo_db, context, include={"info.sample name"},
            search_with_projection=True, modify=keep_projection,
        ),
        filter_filenames(
            get_annotation_by_metas(
                mongo_db, context, include={"info.sample name"},
                search_with_projection=False, modify=keep_files,
            ),
            context.kwargs.get("filename"), startloc=3,
        ),
    )
