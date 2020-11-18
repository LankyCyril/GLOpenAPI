from argparse import Namespace
from pandas import json_normalize, MultiIndex, isnull, merge
from re import findall, search, IGNORECASE
from genefab3.config import RAW_FILE_REGEX
from pymongo.errors import OperationFailure


def isa_sort_dataframe(dataframe):
    """Sort single-level dataframe in order info-investigation-study-assay"""
    column_order = Namespace(
        info=set(), investigation=set(), study=set(), assay=set(), other=set(),
    )
    for column in dataframe.columns:
        if column.startswith("."):
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


def get_annotation_by_metas(db, context, include=(), search_with_projection=True, modify=keep_projection, aggregate=False):
    """Select assays/samples based on annotation filters"""
    full_projection = {
        ".accession": True, ".assay": True, **context.projection,
        **{field: True for field in include},
    }
    try: # get target metadata as single-level dataframe
        if search_with_projection:
            dataframe = json_normalize(list(db.metadata.find(
                context.query, {"_id": False, **full_projection},
            )))
        else:
            dataframe = json_normalize(list(db.metadata.find(
                context.query, {"_id": False},
            )))
    except OperationFailure:
        return None
    # modify with injected function:
    dataframe = modify(dataframe, full_projection)
    # remove trailing dots and hide columns that are explicitly hidden:
    dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
    # sort (ISA-aware) and convert to two-level dataframe:
    dataframe = isa_sort_dataframe(dataframe)
    try:
        dataframe.columns = MultiIndex.from_tuples(
            ("info", ".".join(fields[1:])) if fields[0] == ""
            else (".".join(fields[:2]), ".".join(fields[2:]))
            for fields in map(lambda s: s.split("."), dataframe.columns)
        )
    except TypeError:
        return None
    if aggregate: # coerce to boolean "existence" if requested
        grouper = dataframe.groupby(
            list(dataframe[["info"]].columns), as_index=False,
        )
        return grouper.agg(lambda a: ~isnull(a).all())
    else:
        return dataframe


def get_assays_by_metas(db, context):
    """Select assays based on annotation filters"""
    return get_annotation_by_metas(db, context, aggregate=True)


def get_samples_by_metas(db, context):
    """Select samples based on annotation filters"""
    return get_annotation_by_metas(db, context, include={".sample name"})


def get_files_by_metas(db, context):
    """Select files based on annotation filters"""
    return merge(
        get_annotation_by_metas(
            db, context, include={".sample name"},
            search_with_projection=True, modify=keep_projection,
        ),
        get_annotation_by_metas(
            db, context, include={".sample name"},
            search_with_projection=False, modify=keep_files,
        ),
    )
