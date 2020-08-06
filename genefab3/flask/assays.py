from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from genefab3.utils import UniversalSet, natsorted_dataframe
from genefab3.mongo.utils import get_collection_fields_as_dataframe
from genefab3.mongo.meta import parse_assay_selection
from werkzeug.datastructures import MultiDict
from pandas import concat, merge


ASSAY_META_INFO_COLS = ["accession", "assay name"]
ASSAY_META_MULTIINDEX = [("info", col) for col in ASSAY_META_INFO_COLS]


def get_assays_by_one_meta(db, meta, or_expression, assay_query={}):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    if or_expression == "": # wildcard, get all info
        constrain_fields = UniversalSet()
    else:
        constrain_fields = set(or_expression.split("|"))
    assays_by_one_meta = get_collection_fields_as_dataframe(
        collection=getattr(db, meta), constrain_fields=constrain_fields,
        targets=ASSAY_META_INFO_COLS, skip={"sample name"},
        query=assay_query, store_value=False,
    )
    # prepend column level, see: https://stackoverflow.com/a/42094658/590676
    return concat({
        "info": assays_by_one_meta[ASSAY_META_INFO_COLS],
        meta: assays_by_one_meta.iloc[:,2:],
    }, axis=1)


def get_assays_by_metas(db, rargs={}):
    """Select assays based on annotation (`meta`) filters"""
    assays_by_metas = None
    assay_query = parse_assay_selection(rargs.getlist("select"), as_query=True)
    for meta in rargs:
        if meta in ASSAY_METADATALIKES:
            for expr in rargs.getlist(meta):
                if assays_by_metas is None: # populate with first result
                    assays_by_metas = get_assays_by_one_meta(
                        db, meta, expr, assay_query,
                    )
                else: # perform AND
                    assays_by_metas = merge(
                        assays_by_metas,
                        get_assays_by_one_meta(db, meta, expr, assay_query),
                        on=ASSAY_META_MULTIINDEX, how="inner",
                    )
    # sort presentation:
    return natsorted_dataframe(
        assays_by_metas, by=ASSAY_META_MULTIINDEX, sort_trailing_columns=True,
    )
