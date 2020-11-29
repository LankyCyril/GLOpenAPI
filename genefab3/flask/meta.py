from argparse import Namespace
from pandas import json_normalize, MultiIndex, isnull, concat, merge
from re import findall, search, IGNORECASE, escape, split
from genefab3.config import RAW_FILE_REGEX
from genefab3.flask.display import Placeholders
from pymongo.errors import OperationFailure
from numpy import nan


def isa_sort_dataframe(dataframe):
    """Sort single-level dataframe in order info-investigation-study-assay"""
    column_order = Namespace(
        info=set(), investigation=set(), study=set(), assay=set(), other=set(),
    )
    for column in dataframe.columns:
        if column.startswith("."):
            column_order.info.add(column)
        elif column.startswith("investigation"):
            column_order.investigation.add(column)
        elif column.startswith("study"):
            column_order.study.add(column)
        elif column.startswith("assay"):
            column_order.assay.add(column)
        else:
            column_order.other.add(column)
    return dataframe[(
        sorted(column_order.info) + sorted(column_order.investigation) +
        sorted(column_order.study) + sorted(column_order.assay) +
        sorted(column_order.other)
    )]


def keep_projection(dataframe, display_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested"""
    subkeys_to_drop = {
        c for c in dataframe.columns
        if (len(findall(r'\..', c)) >= 3) and (c not in display_projection)
    }
    return dataframe.drop(columns=subkeys_to_drop)


def unwind_file_entry(cell):
    """Unwind lists of dicts returned for some complex unfiltered fields from database"""
    if isinstance(cell, list):
        try:
            return cell[0][""]
        except (KeyError, IndexError):
            return str(cell)
    else:
        return cell


def keep_files(dataframe, display_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested OR contain filenames"""
    subkeys_to_drop = {
        c for c in dataframe.columns if (
            (c not in display_projection) and
            (not search(RAW_FILE_REGEX, c, flags=IGNORECASE))
        )
    }
    return dataframe.drop(columns=subkeys_to_drop).applymap(unwind_file_entry)


def get_projections(projection, include=(), units=False, search_with_projection=True):
    """Generate display projection (context projection without units), search projection (with units if units=True), normalization level"""
    display_projection = {
        ".accession": True, ".assay": True, **projection,
        **{field: True for field in include},
    }
    if not search_with_projection:
        search_projection = {"_id": False}
        normlevel = None # will make json_normalize unwind all cells
    elif units:
        search_projection = {
            "_id": False, **display_projection, **{
                k[:-1]+"unit": True # will request .unit qualifiers if exist
                for k in projection if k[-2:] == ".."
            },
        }
        normlevel = 2 # will make json_normalize keep {'': value, 'unit': unit}
    else:
        search_projection = display_projection
        normlevel = None # will make json_normalize unwind all cells
    return search_projection, display_projection, normlevel


def format_units(cell):
    """Convert mini-dictionaries of form {'': value, 'unit': unit} to strings"""
    if isinstance(cell, dict):
        if "unit" in cell:
            return "{} [{}]".format(cell[""], cell["unit"])
        else:
            return cell[""]
    else:
        return cell


def get_annotation_by_metas(db, context, include=(), search_with_projection=True, modify=keep_projection, units=False, aggregate=False):
    """Select assays/samples based on annotation filters"""
    search_projection, display_projection, normlevel = get_projections(
        projection=context.projection, include=include, units=units,
        search_with_projection=search_with_projection,
    )
    try:
        dataframe = json_normalize( # get metadata as single-level dataframe:
            list(db.metadata.find(context.query, search_projection)),
            max_level=normlevel, # None unwinds fully, 2 keeps value-unit dicts
        )
        # modify with injected function:
        dataframe = modify(dataframe, display_projection)
        # remove trailing dots:
        dataframe.columns = dataframe.columns.map(lambda c: c.rstrip("."))
        if units: # format value-unit dicts in cells:
            dataframe = dataframe.applymap(format_units)
        # sort (ISA-aware) and convert to two-level dataframe:
        dataframe = isa_sort_dataframe(dataframe)
        dataframe.columns = MultiIndex.from_tuples(
            ("info", ".".join(fields[1:])) if fields[0] == ""
            else (".".join(fields[:2]), ".".join(fields[2:]))
            for fields in map(lambda s: s.split("."), dataframe.columns)
        )
    except (OperationFailure, TypeError): # no data retrieved/retrievable
        return Placeholders.dataframe(
            info=["accession", "assay", *(c.strip(".") for c in include)],
        )
    else:
        if aggregate: # coerce to boolean "existence" if requested
            info_cols = list(dataframe[["info"]].columns)
            if len(info_cols) == dataframe.shape[1]: # only 'info' cols present
                return dataframe.drop_duplicates()
            else: # metadata cols present and can be collapsed into booleans
                grouper = dataframe.groupby(info_cols, as_index=False)
                return grouper.agg(lambda a: ~isnull(a).all())
        else:
            return dataframe


def filter_filenames(dataframe, mask, startloc):
    """Constrain dataframe cells only to cells passing `mask` filter"""
    if mask is None:
        return dataframe
    else:
        if (mask[0] == "/") and (mask[-1] == "/"): # regular expression passed
            expression = mask[1:-1]
        else: # simple filename passed, match full
            expression = r'^' + escape(mask) + r'$'
        def mapper(cell):
            if isinstance(cell, str):
                return ", ".join({
                    filename for filename in split(r'\s*,\s*', cell)
                    if search(expression, filename)
                }) or nan
            else:
                return nan
        return concat(
            objs=[
                dataframe.iloc[:,:startloc],
                dataframe.iloc[:,startloc:].applymap(mapper).dropna(
                    how="all", axis=1,
                ),
            ],
            axis=1,
        )


def get_assays_by_metas(db, context):
    """Select assays based on annotation filters"""
    return get_annotation_by_metas(db, context, aggregate=True)


def get_samples_by_metas(db, context):
    """Select samples based on annotation filters"""
    return get_annotation_by_metas(
        db, context, include={".sample name"}, units=True,
    )


def get_files_by_metas(db, context):
    """Select files based on annotation filters"""
    return merge(
        get_annotation_by_metas(
            db, context, include={".sample name"},
            search_with_projection=True, modify=keep_projection,
        ),
        filter_filenames(
            get_annotation_by_metas(
                db, context, include={".sample name"},
                search_with_projection=False, modify=keep_files,
            ),
            context.kwargs.get("filename"), startloc=3,
        ),
    )
