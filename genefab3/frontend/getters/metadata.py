from genefab3.config import RAW_FILE_REGEX
from pandas import concat, merge
from re import findall, search, IGNORECASE, escape, split
from genefab3.frontend.renderer import Placeholders
from numpy import nan
from genefab3.backend.mongo.readers.metadata import get_annotation_by_metadata


def keep_projection(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested"""
    subkeys_to_drop = {
        c for c in dataframe.columns
        if (len(findall(r'\..', c)) >= 3) and (c not in full_projection)
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


def keep_files(dataframe, full_projection):
    """Drop qualifier fields from single-level dataframe, unless explicitly requested OR contain filenames"""
    subkeys_to_drop = {
        c for c in dataframe.columns if (
            (c not in full_projection) and
            (not search(RAW_FILE_REGEX, c, flags=IGNORECASE))
        )
    }
    return dataframe.drop(columns=subkeys_to_drop).applymap(unwind_file_entry)


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


def get_assays(mongo_db, context):
    """Select assays based on annotation filters"""
    dataframe = get_annotation_by_metadata(
        mongo_db, context, modify=keep_projection, aggregate=True,
    )
    if dataframe is None:
        return Placeholders.metadata_dataframe()
    else:
        return dataframe


def get_samples(mongo_db, context):
    """Select samples based on annotation filters"""
    dataframe = get_annotation_by_metadata(
        mongo_db, context, include={"info.sample name"}, modify=keep_projection,
    )
    if dataframe is None:
        return Placeholders.metadata_dataframe(include={"info.sample name"})
    else:
        return dataframe


def get_files(mongo_db, context):
    """Select files based on annotation filters"""
    annotation_dataframe = get_annotation_by_metadata(
        mongo_db, context, include={"info.sample name"},
        search_with_projection=True, modify=keep_projection,
    ),
    if annotation_dataframe is None:
        return Placeholders.metadata_dataframe(include={"info.sample name"})
    else:
        files_dataframe = filter_filenames(
            get_annotation_by_metadata(
                mongo_db, context, include={"info.sample name"},
                search_with_projection=False, modify=keep_files,
            ),
            context.kwargs.get("filename"), startloc=3,
        )
        if files_dataframe is None:
            return Placeholders.metadata_dataframe(include={"info.sample name"})
        else:
            return merge(annotation_dataframe, files_dataframe)
