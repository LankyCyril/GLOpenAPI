from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from werkzeug.datastructures import ImmutableMultiDict, MultiDict
from pandas import DataFrame, concat, merge
from natsort import natsorted
from collections import defaultdict


class UniversalSet(set):
    def __and__(self, x): return x
    def __rand__(self, x): return x
    def __contains__(self, x): return True


def lookup_meta_keys(collection, targets, skip=set(), target_fields=UniversalSet()):
    skip_downstream = set(skip) | {"_id"} | set(targets)
    unique_descriptors = defaultdict(dict)
    for entry in collection.find():
        for key in set(entry.keys()) - skip_downstream:
            if key in target_fields:
                unique_descriptors[tuple(entry[t] for t in targets)][key] = True
    dataframe_by_metas = DataFrame(unique_descriptors).T
    dataframe_by_metas.index = dataframe_by_metas.index.rename(targets)
    return dataframe_by_metas.fillna(False).reset_index()


def get_assays_by_one_meta(db, meta, rargs, ignore={"Unknown"}):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    collection = getattr(db, meta)
    set_of_or_expression_values = set(rargs.getlist(meta))
    if set_of_or_expression_values == {""}: # wildcard, get all info
        dataframe_by_metas = lookup_meta_keys(
            collection, targets=["accession", "assay name"],
            skip={"sample name"},
        )
        return dataframe_by_metas
    else: # perform ANDs on ORs
        return concat([
            lookup_meta_keys(
                collection, targets=["accession", "assay name"],
                skip={"sample name"},
                target_fields=set(or_expression_values.split("|")),
            )
            for or_expression_values in set_of_or_expression_values
        ], axis=1)


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
            if assays_by_metas is None:
                assays_by_metas = get_assays_by_one_meta(db, meta, rargs)
            else:
                assays_by_metas = merge(
                    assays_by_metas, get_assays_by_one_meta(db, meta, rargs),
                    on=["accession", "assay name"], how="inner",
                )
        else:
            trailing_rargs[meta] = rargs.getlist(meta)
    return sorted_human(assays_by_metas), ImmutableMultiDict(trailing_rargs)
