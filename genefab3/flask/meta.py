from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from genefab3.mongo.utils import get_collection_fields
from pandas import DataFrame
from natsort import natsorted
from genefab3.utils import UniversalSet, natsorted_dataframe
from genefab3.mongo.utils import get_collection_fields_as_dataframe
from pandas import concat, merge


def get_meta_names(db, meta, context):
    """List names of particular meta"""
    if meta not in ASSAY_METADATALIKES:
        raise GeneLabException("Unknown request: '{}'".format(meta))
    else:
        return DataFrame(
            data=natsorted(
                get_collection_fields(
                    collection=getattr(db, meta),
                    skip={"accession", "assay name", "sample name"},
                )
            ),
            columns=[meta],
        )


def get_info_cols(sample_level=True):
    if sample_level:
        info_cols = ["accession", "assay name", "sample name"]
    else:
        info_cols = ["accession", "assay name"]
    info_multicols = [("info", col) for col in info_cols]
    return info_cols, info_multicols


def get_annotation_by_one_meta(db, meta, or_expression, query={}, sample_level=True):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    if or_expression == "": # wildcard, get all info
        constrain_fields = UniversalSet()
    else:
        constrain_fields = set(or_expression.split("|"))
    if sample_level:
        store_value = True
        skip = set()
        info_cols, _ = get_info_cols(sample_level=True)
    else:
        store_value = False
        skip = {"sample name"}
        info_cols, _ = get_info_cols(sample_level=False)
    annotation_by_one_meta = get_collection_fields_as_dataframe(
        collection=getattr(db, meta), constrain_fields=constrain_fields,
        targets=info_cols, skip=skip, store_value=store_value, query=query,
    )
    # prepend column level, see: https://stackoverflow.com/a/42094658/590676
    return concat({
        "info": annotation_by_one_meta[info_cols],
        meta: annotation_by_one_meta.iloc[:,len(info_cols):],
    }, axis=1)


def safe_merge_with_all(constrained_df, unconstrained_df):
    """Merge constrained annotation with single-meta-unconstrained annotation"""
    cols_to_ignore = [
        (l0, l1) for l0, l1 in constrained_df.columns
        if (l0 != "info") and (l0, l1) in unconstrained_df
    ]
    return merge(constrained_df, unconstrained_df.drop(columns=cols_to_ignore))


def get_annotation_by_metas(db, context, sample_level=True):
    """Select samples based on annotation filters"""
    annotation_by_metas = None
    for meta in ASSAY_METADATALIKES:
        for expression, query in getattr(context.queries, meta, []):
            annotation_by_one_meta = get_annotation_by_one_meta(
                db, meta, expression, {**query, **context.queries.select},
                sample_level=sample_level,
            )
            if annotation_by_metas is None: # populate with first result
                annotation_by_metas = annotation_by_one_meta
            elif expression != "": # perform inner join (AND)
                annotation_by_metas = merge(
                    annotation_by_metas, annotation_by_one_meta,
                )
            else:
                annotation_by_metas = safe_merge_with_all(
                    annotation_by_metas, annotation_by_one_meta,
                )
    # reduce and sort presentation:
    _, info_multicols = get_info_cols(sample_level=sample_level)
    return natsorted_dataframe(
        annotation_by_metas.loc[:,~annotation_by_metas.columns.duplicated()],
        by=info_multicols, sort_trailing_columns=True,
    )


def get_assays_by_metas(db, context):
    return get_annotation_by_metas(db, context, sample_level=False)


def get_samples_by_metas(db, context):
    return get_annotation_by_metas(db, context, sample_level=True)
