from genefab3.common.utils import pick_reachable_url
from flask import redirect, Response
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDataManagerException
from natsort import natsorted
from genefab3.db.mongo.utils import match_sample_names_to_file_descriptor
from functools import partial, lru_cache, reduce
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.db.sql.pandas import OndemandSQLiteDataFrame
from pandas import MultiIndex
from genefab3.common.exceptions import GeneFabFormatException
from genefab3.db.sql.files import CachedTableFile, CachedBinaryFile
from genefab3.common.types import PhoenixIterator
from genefab3.db.mongo.utils import aggregate_file_descriptors_by_context
from urllib.error import HTTPError


def fail_if_files_not_joinable(getset):
    """Check for ability to join data from requested files"""
    if len(getset("file", "datatype")) > 1:
        msg = "Cannot combine data of multiple datatypes"
        _kw = dict(datatypes=getset("file", "datatype"))
    elif len(getset("technology type")) > 1:
        msg = "Cannot combine data for multiple technology types"
        _kw = dict(technology_types=getset("technology type"))
    elif getset("file", "joinable") != {True}:
        msg = "Cannot combine multiple files of this datatype"
        _kw = dict(
            datatype=getset("file", "datatype").pop(),
            filenames=getset("file", "filename"),
        )
    elif getset("file", "type") != {"table"}:
        msg = "Cannot combine non-table files"
        _kw = dict(types=getset("file", "type"))
    elif len(getset("file", "index_name")) > 1:
        msg = "Cannot combine tables with conflicting index names"
        _kw = dict(index_names=getset("file", "index_name"))
    else:
        msg, _kw = None, {}
    if msg:
        raise GeneFabFileException(msg, **_kw)


def file_redirect(descriptor):
    """Redirect to file at original URL, without caching or interpreting"""
    name = descriptor["file"]["filename"]
    urls = descriptor["file"].get("urls", ())
    with pick_reachable_url(urls, name=name) as url:
        return redirect(url, code=303, Response=Response)


def harmonize_columns(dataframe, descriptor, sample_names, best_sample_name_matches):
    """Match sample names to columns, infer correct order of original columns based on order of sample_names"""
    harmonized_column_order, harmonized_positions = [], []
    include_nomatch = (descriptor["file"].get("column_subset") != "sample name")
    for i, c in enumerate(dataframe.columns):
        hcs, ps = best_sample_name_matches(
            c, sample_names, return_positions=True,
        )
        if len(hcs) == 0:
            if include_nomatch:
                harmonized_column_order.append(c)
            else:
                harmonized_column_order.append(None)
        elif len(hcs) == 1:
            harmonized_column_order.append(hcs[0])
            harmonized_positions.append((ps[0], i))
        else:
            msg = "Column name matches multiple sample names"
            filename = descriptor["file"].get("filename")
            _kws = dict(filename=filename, column=c, sample_names=hcs)
            raise GeneFabDataManagerException(msg, **_kws)
    harmonized_unordered = harmonized_column_order[:]
    original_unordered = list(dataframe.columns)
    column_order = original_unordered[:]
    current_positions = [i for p, i in harmonized_positions]
    target_positions = [i for p, i in sorted(harmonized_positions)]
    for cp, tp in zip(current_positions, target_positions):
        harmonized_column_order[tp] = harmonized_unordered[cp]
        column_order[tp] = original_unordered[cp]
    return (
        [c for c in column_order if c is not None],
        [c for c in harmonized_column_order if c is not None],
    )


def INPLACE_process_dataframe(dataframe, *, mongo_collections, descriptor, best_sample_name_matches):
    """Harmonize index name, column names, reorder sample columns to match all associated sample names in database"""
    if not dataframe.index.name:
        dataframe.index.name = descriptor["file"].get("index_name", "index")
    all_sample_names = natsorted(match_sample_names_to_file_descriptor(
        mongo_collections.metadata, descriptor,
    ))
    if descriptor["file"].get("index_subset") == "sample name":
        if descriptor["file"].get("column_subset") == "sample name":
            msg = "Processing data with both index_subset and column_subset"
            raise NotImplementedError(msg)
        else:
            index_order, harmonized_index_order = harmonize_columns(
                dataframe.T, descriptor, all_sample_names,
                best_sample_name_matches,
            )
            if not (dataframe.index == index_order).all():
                for ix in index_order: # reorder rows in-place:
                    row = dataframe.loc[ix]
                    dataframe.drop(index=ix, inplace=True)
                    dataframe.loc[ix] = row
            dataframe.index = harmonized_index_order
    else:
        column_order, harmonized_column_order = harmonize_columns(
            dataframe, descriptor, all_sample_names, best_sample_name_matches,
        )
        if not (dataframe.columns == column_order).all():
            for column in column_order: # reorder columns in-place
                dataframe[column] = dataframe.pop(column)
        dataframe.columns = harmonized_column_order


def get_formatted_data(descriptor, mongo_collections, sqlite_db, CachedFile, adapter, identifier_prefix, _kws):
    """Instantiate and initialize CachedFile object; post-process its data; select only the columns in passed annotation"""
    try:
        accession = descriptor["accession"]
        assay_name = descriptor["assay name"]
        filename = descriptor["file"]["filename"]
    except (KeyError, TypeError, IndexError):
        msg = "File descriptor missing 'accession', 'assay name', or 'filename'"
        raise GeneFabDatabaseException(msg, descriptor=descriptor)
    else:
        prefix = identifier_prefix
        identifier = f"{prefix}:{accession}/File/{assay_name}/{filename}"
    if "INPLACE_process" in _kws:
        _kws = {**_kws, "INPLACE_process": partial(
            INPLACE_process_dataframe, descriptor=descriptor,
            best_sample_name_matches=adapter.best_sample_name_matches,
            mongo_collections=mongo_collections,
        )}
    file = CachedFile(
        name=filename, identifier=identifier,
        urls=descriptor["file"].get("urls", ()),
        timestamp=descriptor["file"].get("timestamp", -1),
        sqlite_db=sqlite_db, **_kws,
    )
    data = file.data
    if isinstance(data, OndemandSQLiteDataFrame):
        _, harmonized_column_order = harmonize_columns(
            data, descriptor, natsorted(descriptor["sample name"]),
            adapter.best_sample_name_matches,
        )
        data.columns = MultiIndex.from_tuples((
            (accession, assay_name, column)
            for column in harmonized_column_order
        ))
    return data


def combine_objects(objects, context, limit=None):
    """Combine objects and post-process"""
    if len(objects) == 0:
        return None
    elif len(objects) == 1:
        combined = objects[0]
    elif all(isinstance(obj, OndemandSQLiteDataFrame) for obj in objects):
        combined = OndemandSQLiteDataFrame.concat(objects, axis=1)
    else:
        raise NotImplementedError("Merging non-table data objects")
    if isinstance(combined, OndemandSQLiteDataFrame):
        if context.data_columns and (context.format == "gct"):
            msg = "GCT format is disabled for arbitrarily subset tables"
            _kw = dict(columns="|".join(context.data_columns))
            raise GeneFabFormatException(msg, **_kw)
        else:
            combined.constrain_columns(context=context)
            return combined.get(context=context)
    elif context.data_columns or context.data_comparisons:
        raise GeneFabFileException(
            "Column operations on non-table data objects are not supported",
            columns=context.data_columns, comparisons=context.data_comparisons,
        )
    else:
        return combined


def combined_data(descriptors, n_descriptors, context, mongo_collections, sqlite_dbs, adapter):
    """Patch through to cached data for each file and combine them"""
    getset = lru_cache(maxsize=None)(lambda *keys: set(
        reduce(lambda d, k: d.get(k, {}), keys, d) or None for d in descriptors
    ))
    if n_descriptors > 1:
        fail_if_files_not_joinable(getset)
    if getset("file", "cacheable") != {True}:
        msg = "Data marked as non-cacheable, cannot be returned in this format"
        sug = "Use 'format=raw'"
        raise GeneFabFileException(msg, suggestion=sug, format=context.format)
    _types = getset("file", "type")
    if _types == {"table"}:
        sqlite_db, CachedFile = sqlite_dbs.tables["db"], CachedTableFile
        identifier_prefix = "TABLE"
        maxdbsize = sqlite_dbs.tables["maxsize"]
        _kws = dict(maxdbsize=maxdbsize, index_col=0, INPLACE_process=True)
    elif len(_types) == 1:
        sqlite_db, CachedFile = sqlite_dbs.blobs["db"], CachedBinaryFile
        identifier_prefix, _kws = "BLOB", {}
    else:
        raise NotImplementedError(f"Joining data of types {_types}")
    _sort_key = lambda d: (d.get("accession"), d.get("assay name"))
    data = combine_objects(context=context, objects=[
        get_formatted_data(
            descriptor, mongo_collections, sqlite_db, CachedFile, adapter,
            identifier_prefix, _kws,
        )
        for descriptor in natsorted(descriptors, key=_sort_key)
    ])
    if data is None:
        raise GeneFabDatabaseException("No data found in database")
    else:
        data.datatypes = getset("file", "datatype")
        data.gct_validity_set = getset("file", "gct_valid")
        return data


def get(*, mongo_collections, locale, context, sqlite_dbs, adapter):
    """Return data corresponding to search parameters; merged if multiple underlying files are same type and joinable"""
    descriptors = PhoenixIterator(aggregate_file_descriptors_by_context(
        mongo_collections.metadata, locale=locale, context=context,
    ))
    n_descriptors = 0
    for n_descriptors, d in enumerate(descriptors, 1):
        if ("file" in d) and (not isinstance(d["file"], dict)):
            msg = "Query did not result in an unambiguous target file"
            raise GeneFabDatabaseException(msg, debug_info=d)
        elif ("file" not in d) or ("filename" not in d["file"]):
            msg = "File information missing for entry"
            raise GeneFabDatabaseException(msg, entry=d)
    if n_descriptors == 0:
        msg = "No file found matching specified constraints"
        raise HTTPError(context.full_path, 404, msg, hdrs=None, fp=None)
    elif context.format == "raw":
        if n_descriptors == 1:
            return file_redirect(next(descriptors))
        else:
            raise GeneFabFileException(
                ("Multiple files match query; " +
                "with format 'raw', only one file can be requested"),
                format="raw",
                files={d["file"]["filename"] for d in descriptors},
            )
    else:
        return combined_data(
            descriptors, n_descriptors,
            context, mongo_collections, sqlite_dbs, adapter,
        )
