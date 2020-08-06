from genefab3.exceptions import GeneLabException
from argparse import Namespace
from genefab3.config import ASSAY_METADATALIKES


def parse_assay_selection(rargs_select_list):
    """Parse request argument 'select'"""
    if len(rargs_select_list) == 0:
        return None
    elif len(rargs_select_list) == 1:
        selection = {}
        for query in rargs_select_list[0].split("|"):
            query_components = query.split(":", 1)
            if len(query_components) == 1:
                selection[query] = None
            else:
                selection[query_components[0]] = query_components[1]
        return selection
    else:
        raise GeneLabException("'select' can be used no more than once")


def parse_meta_selectors(meta_key, meta_selectors):
    """Process queries like e.g. 'factors=age' and 'factors:age=1|2'"""
    query_cc = meta_key.split(":")
    if (len(query_cc) == 2) and (query_cc[0] in ASSAY_METADATALIKES):
        meta, queried_field = query_cc # e.g. "factors" and "age"
    else:
        meta, queried_field = meta_key, None # e.g. "factors"
    if meta in ASSAY_METADATALIKES:
        subqueries = []
        for expression in meta_selectors:
            if queried_field: # e.g. {"age": {"$in": [1, 2]}}
                if expression == "":
                    raise GeneLabException(meta_key + " must have a value")
                else:
                    subqueries.append({
                        queried_field: {"$in": expression.split("|")}
                    })
            elif expression != "": # look up by specific meta name(s)
                subqueries.append({"$or": [
                    {key: {"$exists": True}}
                    for key in expression.split("|")
                ]})
            else: # lookup just by meta name:
                pass
        if subqueries:
            return meta, {"$and": subqueries}
        else:
            return meta, {}
    else:
        return None, None


def parse_request_args(rargs):
    parsed_rargs = Namespace(
        select=parse_assay_selection(rargs.getlist("select")),
    )
    for key in rargs:
        meta, meta_query = parse_meta_selectors(key, rargs.getlist(key))
        if meta is not None:
            setattr(parsed_rargs, meta, meta_query)
            print(meta, meta_query)
    return parsed_rargs
