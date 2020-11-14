from argparse import Namespace
from pandas import json_normalize, MultiIndex, isnull
from re import findall


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


def get_annotation_by_metas(db, context, include=(), aggregate=False):
    """Select assays/samples based on annotation filters"""
    full_projection = {
        ".accession": True, ".assay": True, **context.projection,
        **{field: True for field in include},
    }
    # get target metadata as single-level dataframe:
    dataframe = json_normalize(list(db.metadata.find(
        context.query, {"_id": False, **full_projection},
    )))
    # drop qualifier fields unless explicitly requested:
    subkeys_to_drop = {
        c for c in dataframe.columns
        if (len(findall(r'\..', c)) >= 3) and (c not in full_projection)
    }
    dataframe.drop(columns=subkeys_to_drop, inplace=True)
    # remove trailing dots and hide columns that are explicitly hidden:
    dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
    dataframe.drop(columns=context.hide, inplace=True)
    # sort (ISA-aware) and convert to two-level dataframe:
    dataframe = isa_sort_dataframe(dataframe)
    dataframe.columns = MultiIndex.from_tuples(
        ("info", ".".join(fields[1:])) if fields[0] == ""
        else (".".join(fields[:2]), ".".join(fields[2:]))
        for fields in map(lambda s: s.split("."), dataframe.columns)
    )
    # coerce to boolean "existence" if requested:
    if aggregate:
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
