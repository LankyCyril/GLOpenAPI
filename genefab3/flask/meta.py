from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from genefab3.mongo.utils import get_collection_fields
from pandas import DataFrame
from natsort import natsorted
from genefab3.utils import natsorted_dataframe, empty_df
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
        drop_cols = ["_id"]
        info_cols = ["accession", "assay name", "sample name"]
    else:
        drop_cols = ["_id", "sample name"]
        info_cols = ["accession", "assay name"]
    info_multicols = [("info", col) for col in info_cols]
    return drop_cols, info_cols, info_multicols


def get_annotation_by_one_meta(db, meta, context, drop_cols, info_cols, sample_level=True):
    """Generate dataframe of assays matching (AND) multiple `meta` queries"""
    select = context.queries["select"]
    collection, by_one_meta = getattr(db, meta), None
    query2df = lambda query: (
        DataFrame(collection.find(query))
        .drop(columns=drop_cols, errors="ignore")
        .dropna(how="all", axis=1)
        .applymap(
            lambda vv: "|".join(sorted(map(str, vv))) if isinstance(vv, list)
            else vv
        )
    )
    if context.queries[meta]:
        by_one_meta = query2df({"$and": context.queries[meta] + [select]})
    if meta in context.wildcards:
        by_one_wildcard = query2df(select)
        if by_one_meta is None:
            by_one_meta = by_one_wildcard
        elif len(by_one_meta) == 0:
            return None
        else:
            by_one_meta = merge(by_one_meta, by_one_wildcard)
    if by_one_meta is not None:
        by_one_meta = ( # drop empty columns and simplify representation:
            by_one_meta # FIXME: what if a meta name is "accession"?
            .drop(columns=context.removers[meta])
            .dropna(how="all", axis=1)
        )
        return concat({ # make two-level:
            "info": by_one_meta[info_cols],
            meta: by_one_meta.drop(columns=info_cols) if sample_level
                else ~by_one_meta.drop(columns=info_cols).isnull()
        }, axis=1).drop_duplicates()
    else:
        return None


def get_annotation_by_metas(db, context, sample_level=True):
    """Select samples based on annotation filters"""
    drop_cols, info_cols, info_multicols = get_info_cols(sample_level)
    annotation_by_metas = None
    for meta in ASSAY_METADATALIKES:
        annotation_by_one_meta = get_annotation_by_one_meta(
            db, meta, context, drop_cols, info_cols, sample_level=sample_level,
        )
        if annotation_by_metas is None:
            annotation_by_metas = annotation_by_one_meta
        elif annotation_by_one_meta is not None:
            annotation_by_metas = merge(
                annotation_by_metas, annotation_by_one_meta,
            )
    # reduce and sort presentation:
    if (annotation_by_metas is None) or (len(annotation_by_metas) == 0):
        return empty_df(columns=info_multicols)
    else:
        return natsorted_dataframe(
            annotation_by_metas, by=info_multicols, sort_trailing_columns=True,
        )


def get_assays_by_metas(db, context):
    return get_annotation_by_metas(db, context, sample_level=False)


def get_samples_by_metas(db, context):
    return get_annotation_by_metas(db, context, sample_level=True)
