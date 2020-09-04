from pandas import json_normalize, MultiIndex


def get_annotation_by_metas(db, context, include=()):
    """Select assays/samples based on annotation filters"""
    dataframe = json_normalize(db.metadata.find(
        context.query, {
            ".accession": True, ".assay": True,
            **{field: True for field in include},
            **context.projection, "_id": False,
        }
    ))
    dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
    dataframe.drop(columns=context.hide, inplace=True)
    dataframe.columns = MultiIndex.from_tuples(
        ("info", ".".join(fields[1:])) if fields[0] == ""
        else (".".join(fields[:2]), ".".join(fields[2:]))
        for fields in map(lambda s: s.split("."), dataframe.columns)
    )
    return dataframe


def get_assays_by_metas(db, context):
    """Select assays based on annotation filters"""
    return get_annotation_by_metas(db, context)


def get_samples_by_metas(db, context):
    """Select samples based on annotation filters"""
    return get_annotation_by_metas(db, context, include={".sample name"})
