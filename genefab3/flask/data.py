from genefab3.config import ASSAY_METADATALIKES
from genefab3.utils import UniversalSet
from genefab3.mongo.utils import get_collection_fields_as_dataframe
from pandas import concat, merge
from werkzeug.datastructures import ImmutableMultiDict


SAMPLE_META_INFO_COLS = [
    ("info", "accession"), ("info", "assay name"), ("info", "sample name"),
]


def get_samples_by_one_meta(db, meta, or_expression):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    if or_expression == "": # wildcard, get all info
        constrain_fields = UniversalSet()
    else:
        constrain_fields = set(or_expression.split("|"))
    samples_by_one_meta = get_collection_fields_as_dataframe(
        collection=getattr(db, meta), constrain_fields=constrain_fields,
        targets=["accession", "assay name", "sample name"],
        store_value=True,
    )
    # prepend column level, see: https://stackoverflow.com/a/42094658/590676
    return concat({
        "info": samples_by_one_meta[["accession", "assay name", "sample name"]],
        meta: samples_by_one_meta.iloc[:,3:],
    }, axis=1)


def get_data_by_metas(db, rargs={}):
    """Select data based on annotation filters"""
    samples_by_metas, trailing_rargs = None, {}
    for meta in rargs:
        if meta in ASSAY_METADATALIKES:
            for expr in rargs.getlist(meta):
                if samples_by_metas is None: # populate with first result
                    samples_by_metas = get_samples_by_one_meta(db, meta, expr)
                else: # perform AND
                    samples_by_metas = merge(
                        samples_by_metas,
                        get_samples_by_one_meta(db, meta, expr),
                        on=SAMPLE_META_INFO_COLS, how="inner",
                    )
        else:
            trailing_rargs[meta] = rargs.getlist(meta)
    return samples_by_metas, ImmutableMultiDict(trailing_rargs)
