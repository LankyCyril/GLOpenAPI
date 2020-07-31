from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from genefab3.utils import UniversalSet, natsorted_dataframe
from genefab3.mongo.utils import get_collection_keys_as_dataframe
from werkzeug.datastructures import ImmutableMultiDict, MultiDict
from pandas import concat, merge


def get_assays_by_one_meta(db, meta, or_expression):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    if or_expression == "": # wildcard, get all info
        constrain_fields = UniversalSet()
    else:
        constrain_fields = set(or_expression.split("|"))
    assays_by_one_meta = get_collection_keys_as_dataframe(
        collection=getattr(db, meta), constrain_fields=constrain_fields,
        targets=["accession", "assay name"], skip={"sample name"},
    )
    # prepend column level, see: https://stackoverflow.com/a/42094658/590676
    return concat({
        "info": assays_by_one_meta[["accession", "assay name"]],
        meta: assays_by_one_meta.iloc[:,2:],
    }, axis=1)


def get_assays_by_metas(db, meta=None, rargs={}):
    """Select assays based on annotation (`meta`) filters"""
    if meta:
        if meta in rargs:
            error_mask = "Malformed request: '{}' redefinition"
            raise GeneLabException(error_mask.format(meta))
        else:
            rargs = MultiDict(rargs)
            rargs[meta] = ""
    # perform intersections of unions:
    assays_by_metas, trailing_rargs = None, {}
    for meta in rargs:
        if meta in ASSAY_METADATALIKES:
            for expr in rargs.getlist(meta):
                if assays_by_metas is None: # populate with first result
                    assays_by_metas = get_assays_by_one_meta(db, meta, expr)
                else: # perform AND
                    assays_by_metas = merge(
                        assays_by_metas, get_assays_by_one_meta(db, meta, expr),
                        on=[("info", "accession"), ("info", "assay name")],
                        how="inner",
                    )
        else:
            trailing_rargs[meta] = rargs.getlist(meta)
    # sort presentation:
    natsorted_assays_by_metas = natsorted_dataframe(
        assays_by_metas, by=[("info", "accession"), ("info", "assay name")],
    )
    return natsorted_assays_by_metas, ImmutableMultiDict(trailing_rargs)
