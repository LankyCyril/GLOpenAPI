from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from werkzeug.datastructures import ImmutableMultiDict, MultiDict
from pandas import DataFrame, concat, merge
from natsort import natsorted


def lookup_meta(db, keys, value, matcher):
    """Match-group-aggregate in MongoDB and represent result as DataFrame"""
    aggregator = db.assay_meta.aggregate([
        {"$match": matcher},
        {"$group": {"_id": {kv: "$"+kv for kv in keys+[value]}}},
    ])
    keys_to_values_lookup = {
        tuple(entry[k] for k in keys): entry[value]
        for entry in map(lambda e: e["_id"], aggregator)
    }
    keys_to_values_dataframe = DataFrame(keys_to_values_lookup, index=[value]).T
    keys_to_values_dataframe.index = keys_to_values_dataframe.index.rename(keys)
    return keys_to_values_dataframe.reset_index()


def pivot_by(dataframe, by, drop=None, groupby=None, ignore=set()):
    """Pseudo-pivot dataframe by values of single column"""
    if drop:
        pivoted_dataframe = dataframe.drop(columns=[by, drop]).copy()
    else:
        pivoted_dataframe = dataframe.drop(columns=by).copy()
    for value in set(dataframe[by].drop_duplicates()) - set(ignore):
        pivoted_dataframe[value] = (dataframe[by] == value)
    if groupby:
        return pivoted_dataframe.groupby(groupby, as_index=False).max()
    else:
        return pivoted_dataframe


def get_assays_by_one_meta_any(db, meta, meta_any):
    """Generate dataframe of assays matching ANY of the `meta` values (e.g., "factors" in {"spaceflight" OR "microgravity"})"""
    dataframe_by_meta = lookup_meta(
        db, keys=["accession", "assay_name"], value="field",
        matcher={"meta": meta, "field": {"$in": meta_any.split("|")}},
    )
    return pivot_by(dataframe_by_meta, "field")


def get_assays_by_one_meta(db, meta, rargs, ignore={"Unknown"}):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    set_of_meta_anys = set(rargs.getlist(meta))
    if set_of_meta_anys == {""}: # wildcard, get all info
        dataframe_by_metas = lookup_meta(
            db, keys=["accession", "assay_name", "field"], value="meta",
            matcher={"meta": meta},
        )
        return pivot_by(
            dataframe_by_metas, drop="meta", ignore=ignore,
            by="field", groupby=["accession", "assay_name"],
        )
    else: # perform ANDs on ORs
        return concat([
            get_assays_by_one_meta_any(db, meta, meta_any)
            for meta_any in set_of_meta_anys
        ], axis=1)


def sorted_human(assays_by_metas):
    reindexed = assays_by_metas[
        ["accession", "assay_name"] + sorted(assays_by_metas.columns[2:])
    ]
    reindexed["accession"] = reindexed["accession"].astype("category")
    reindexed["accession"].cat.reorder_categories(
        natsorted(set(reindexed["accession"])), inplace=True, ordered=True,
    )
    return reindexed.sort_values(by=["accession", "assay_name"])


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
                    on=["accession", "assay_name"], how="inner",
                )
        else:
            trailing_rargs[meta] = rargs.getlist(meta)
    return sorted_human(assays_by_metas), ImmutableMultiDict(trailing_rargs)
