from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from werkzeug.datastructures import ImmutableMultiDict, MultiDict
from pandas import DataFrame, merge
from natsort import natsorted
from collections import defaultdict


class UniversalSet(set):
    def __and__(self, x): return x
    def __rand__(self, x): return x
    def __contains__(self, x): return True


def get_collection_keys_dataframe(collection, targets, skip=set(), constrain_fields=UniversalSet()):
    skip_downstream = set(skip) | {"_id"} | set(targets)
    unique_descriptors = defaultdict(dict)
    for entry in collection.find():
        for key in set(entry.keys()) - skip_downstream:
            if key in constrain_fields:
                unique_descriptors[tuple(entry[t] for t in targets)][key] = True
    dataframe_by_metas = DataFrame(unique_descriptors).T
    dataframe_by_metas.index = dataframe_by_metas.index.rename(targets)
    return dataframe_by_metas.fillna(False).reset_index()


def get_assays_by_one_meta(db, meta, or_expression):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    if or_expression == "": # wildcard, get all info
        constrain_fields = UniversalSet()
    else:
        constrain_fields = set(or_expression.split("|"))
    return get_collection_keys_dataframe(
        collection=getattr(db, meta),
        targets=["accession", "assay name"], skip={"sample name"},
        constrain_fields=constrain_fields,
    )


def sorted_human(assays_by_metas):
    """See: https://stackoverflow.com/a/29582718/590676"""
    reindexed = assays_by_metas[
        ["accession", "assay name"] + sorted(assays_by_metas.columns[2:])
    ]
    reindexed["accession"] = reindexed["accession"].astype("category")
    reindexed["accession"].cat.reorder_categories(
        natsorted(set(reindexed["accession"])), inplace=True, ordered=True,
    )
    return reindexed.sort_values(by=["accession", "assay name"])


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
                        on=["accession", "assay name"], how="inner",
                    )
        else:
            trailing_rargs[meta] = rargs.getlist(meta)
    return sorted_human(assays_by_metas), ImmutableMultiDict(trailing_rargs)
