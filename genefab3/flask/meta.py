from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from genefab3.mongo.utils import get_collection_fields
from genefab3.mongo.meta import parse_assay_selection
from pandas import DataFrame
from natsort import natsorted
from genefab3.utils import UniversalSet, natsorted_dataframe
from genefab3.mongo.utils import get_collection_fields_as_dataframe
from pandas import concat, merge
from re import search


def get_meta_names(db, meta, rargs=None):
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


def duplicate_aware_merge(df1, df2, on, how="inner"):
    """Merge dataframes on specific columns, also check duplicates among other columns"""
    merged_df = merge(df1, df2, on=on, how=how, suffixes=("[DUPLICATE]", ""))
    for level0, level1 in merged_df.columns:
        match = search(r'(.+)\[DUPLICATE\]', level0)
        if match:
            indexer = match.group(1), level1
            if (merged_df[indexer] == merged_df[(level0, level1)]).all():
                merged_df.drop(columns=[(level0, level1)], inplace=True)
            else:
                raise GeneLabException("Failed to merge columns")
    return merged_df


def get_annotation_by_metas(db, sample_level=True, rargs={}):
    """Select samples based on annotation filters"""
    annotation_by_metas = None
    assay_query = parse_assay_selection(rargs.getlist("select"), as_query=True)
    _, info_multicols = get_info_cols(sample_level=sample_level)
    for meta_query in rargs:
        # process queries like e.g. "factors=age" and "factors:age=1|2":
        query_cc = meta_query.split(":")
        if (len(query_cc) == 2) and (query_cc[0] in ASSAY_METADATALIKES):
            meta, queried_field = query_cc # e.g. "factors" and "age"
        else:
            meta, queried_field = meta_query, None # e.g. "factors"
        if meta in ASSAY_METADATALIKES:
            for expr in rargs.getlist(meta_query):
                if queried_field: # e.g. {"age": {"$in": [1, 2]}}
                    query = {
                        queried_field: {"$in": expr.split("|")}, **assay_query,
                    }
                    expr = queried_field
                else: # lookup just by meta name:
                    query = assay_query
                if annotation_by_metas is None: # populate with first result
                    annotation_by_metas = get_annotation_by_one_meta(
                        db, meta, expr, query, sample_level=sample_level,
                    )
                else: # perform AND
                    annotation_by_metas = duplicate_aware_merge(
                        annotation_by_metas,
                        get_annotation_by_one_meta(
                            db, meta, expr, query, sample_level=sample_level,
                        ),
                        on=info_multicols, how="inner",
                    )
    # sort presentation:
    return natsorted_dataframe(
        annotation_by_metas, by=info_multicols,
        sort_trailing_columns=True,
    )


def get_assays_by_metas(db, rargs={}):
    return get_annotation_by_metas(db, sample_level=False, rargs=rargs)


def get_samples_by_metas(db, rargs={}):
    return get_annotation_by_metas(db, sample_level=True, rargs=rargs)
