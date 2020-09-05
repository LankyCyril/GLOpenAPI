from pandas import json_normalize, MultiIndex, isnull
from re import findall


def get_annotation_by_metas(db, context, include=(), aggregate=False):
    """Select assays/samples based on annotation filters"""
    full_projection = {
        ".accession": True, ".assay": True, **context.projection,
        **{field: True for field in include},
    }
    dataframe = json_normalize(db.metadata.find(
        context.query, {"_id": False, **full_projection},
    ))
    subkeys_to_drop = {
        c for c in dataframe.columns
        if (len(findall(r'\..', c)) == 3) and (c not in full_projection)
    }
    dataframe.drop(columns=subkeys_to_drop, inplace=True)
    dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
    dataframe.drop(columns=context.hide, inplace=True)
    dataframe.columns = MultiIndex.from_tuples(
        ("info", ".".join(fields[1:])) if fields[0] == ""
        else (".".join(fields[:2]), ".".join(fields[2:]))
        for fields in map(lambda s: s.split("."), dataframe.columns)
    )
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
