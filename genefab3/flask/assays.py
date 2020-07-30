from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from werkzeug.datastructures import ImmutableMultiDict
from pandas import DataFrame, concat


ALL = None


def lookup_meta(db, keys, values, matcher):
    """Match-group-aggregate in MongoDB and represent result as DataFrame"""
    aggregator = db.assay_meta.aggregate([
        {"$match": matcher},
        {"$group": {"_id": {kv: "$"+kv for kv in keys+values}}},
    ])
    keys_to_values_lookup = {
        tuple(entry[k] for k in keys): [entry[v] for v in values]
        for entry in map(lambda e: e["_id"], aggregator)
    }
    return DataFrame(keys_to_values_lookup, index=values).T


def pivot_by(dataframe, by):
    """Pseudo-pivot dataframe by values of single column"""
    pivoted_dataframe = dataframe.drop(columns=by).copy()
    for value in dataframe[by].drop_duplicates():
        pivoted_dataframe[value] = (dataframe[by] == value)
    return pivoted_dataframe


def get_assays_by_one_meta_any(db, meta, meta_any):
    """Generate dataframe of assays matching ANY of the `meta` values (e.g., "factors" in {"spaceflight" OR "microgravity"})"""
    dataframe_by_meta = lookup_meta(
        db, keys=["accession", "assay_name"], values=["field"],
        matcher={"meta": meta, "field": {"$in": meta_any.split("|")}},
    )
    return pivot_by(dataframe_by_meta, "field")


def get_assays_by_one_meta(db, meta, rargs):
    """Generate dataframe of assays matching (AND) multiple `meta` lookups (OR)"""
    set_of_meta_anys = set(rargs.getlist(meta))
    if set_of_meta_anys == {""}:
        return ALL
    else:
        return concat([
            get_assays_by_one_meta_any(db, meta, meta_any)
            for meta_any in set_of_meta_anys
        ], axis=1)


def get_all_assays_metas(db, metas, ignore="Unknown"):
    dataframe_by_metas = lookup_meta(
        db, ["accession", "assay_name"], ["meta", "field"],
        {"meta": {"$in": metas}},
    )
    for meta in metas:
        dataframe_by_one_meta = pivot_by(
            dataframe_by_metas[dataframe_by_metas["meta"]==meta].drop(
                columns="meta",
            ),
            "field",
        )
        return dataframe_by_one_meta
    # TODO


def get_assays_by_metas(db, meta=None, rargs={}):
    """Select assays based on annotation (`meta`) filters"""
    if meta and rargs: # impossible request
        error_mask = "Malformed request: '{}' with extra arguments"
        raise GeneLabException(error_mask.format(meta))
    elif meta: # convert subpage to a meta wildcard
        rargs = ImmutableMultiDict({meta: ""})
    # perform intersections of unions:
    assays_by_metas = ALL
    for meta in rargs:
        if meta not in ASSAY_METADATALIKES:
            raise GeneLabException("Unrecognized meta: '{}'".format(meta))
        else:
            assays_by_one_meta = get_assays_by_one_meta(db, meta, rargs)
            if assays_by_metas is ALL:
                assays_by_metas = assays_by_one_meta
            elif assays_by_one_meta is not ALL:
                assays_by_metas = concat(
                    [assays_by_metas, assays_by_one_meta], axis=1
                ).dropna()
    if assays_by_metas is ALL:
        assays_by_metas = get_all_assays_metas(db, list(rargs))
    return assays_by_metas.to_html()
