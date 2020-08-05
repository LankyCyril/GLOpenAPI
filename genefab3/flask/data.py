from genefab3.config import ASSAY_METADATALIKES
from genefab3.utils import UniversalSet, natsorted_dataframe
from genefab3.mongo.utils import get_collection_fields_as_dataframe
from genefab3.mongo.meta import parse_assay_selection
from genefab3.mongo.data import query_data
from pandas import concat, merge


SAMPLE_META_INFO_COLS = ["accession", "assay name", "sample name"]
SAMPLE_META_MULTIINDEX = [("info", col) for col in SAMPLE_META_INFO_COLS]


def get_samples_by_one_meta(db, meta, or_expression, query={}):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    if or_expression == "": # wildcard, get all info
        constrain_fields = UniversalSet()
    else:
        constrain_fields = set(or_expression.split("|"))
    samples_by_one_meta = get_collection_fields_as_dataframe(
        collection=getattr(db, meta), constrain_fields=constrain_fields,
        targets=SAMPLE_META_INFO_COLS, store_value=True, query=query,
    )
    # prepend column level, see: https://stackoverflow.com/a/42094658/590676
    return concat({
        "info": samples_by_one_meta[SAMPLE_META_INFO_COLS],
        meta: samples_by_one_meta.iloc[:,3:],
    }, axis=1)


def get_samples_by_metas(db, rargs={}):
    """Select samples based on annotation filters"""
    samples_by_metas = None
    assay_query = parse_assay_selection(rargs.getlist("select"), as_query=True)
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
                if samples_by_metas is None: # populate with first result
                    samples_by_metas = get_samples_by_one_meta(
                        db, meta, expr, query,
                    )
                else: # perform AND
                    samples_by_metas = merge(
                        samples_by_metas,
                        get_samples_by_one_meta(db, meta, expr, query),
                        on=SAMPLE_META_MULTIINDEX, how="inner",
                    )
    # sort presentation:
    return natsorted_dataframe(
        samples_by_metas, by=SAMPLE_META_MULTIINDEX, sort_trailing_columns=True,
    )


def get_data_by_metas(db, rargs={}):
    """Select data based on annotation filters"""
    samples_by_metas = get_samples_by_metas(db, rargs)
    sample_columns = samples_by_metas[SAMPLE_META_MULTIINDEX].set_index(
        SAMPLE_META_MULTIINDEX
    ).index
    return query_data(sample_columns)
