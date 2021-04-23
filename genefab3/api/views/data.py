from genefab3.db.mongo.utils import retrieve_by_context
from genefab3.db.mongo.utils import match_sample_names_to_file_descriptor
from functools import lru_cache, reduce, partial
from genefab3.common.utils import pick_reachable_url
from flask import redirect, Response
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDataManagerException
from pandas import MultiIndex
from genefab3.db.sql.types import OndemandSQLiteDataFrame
from genefab3.db.sql.types import CachedTableFile, CachedBinaryFile
from natsort import natsorted
from genefab3.common.exceptions import GeneFabDatabaseException
from urllib.error import HTTPError


TECH_TYPE_LOCATOR = "investigation.study assays", "study assay technology type"


def get_file_descriptors(mongo_collections, *, locale, context):
    """Return DataFrame of file descriptors that match user query"""
    context.projection["file"] = True
    context.update(".".join(TECH_TYPE_LOCATOR), auto_reduce=True)
    descriptors, _ = retrieve_by_context(
        mongo_collections.metadata, locale=locale, context=context,
        id_fields=["accession", "assay", "sample name"], postprocess=[
            {"$group": {
                "_id": {
                    "accession": "$id.accession",
                    "assay": "$id.assay",
                    "technology type": "$"+".".join(TECH_TYPE_LOCATOR),
                    "file": "$file",
                },
                "sample name": {"$push": "$id.sample name"},
            }},
            {"$addFields": {"_id.sample name": "$sample name"}},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ],
    )
    return list(descriptors)


def validate_joinable_files(descriptors):
    """Check for ability to join data from requested files"""
    getset = lru_cache(maxsize=None)(lambda *keys: set(
        reduce(lambda d, k: d.get(k, {}), keys, d) or None for d in descriptors
    ))
    if len(getset("file", "datatype")) > 1:
        msg = "Cannot combine data of multiple datatypes"
        return msg, dict(datatypes=getset("file", "datatype"))
    elif len(getset("technology type")) > 1:
        msg = "Cannot combine data for multiple technology types"
        return msg, dict(technology_types=getset("technology type"))
    elif getset("file", "joinable") != {True}:
        return "Cannot combine multiple files of this datatype", dict(
            datatype=getset("file", "datatype").pop(),
            filenames=getset("file", "filename"),
        )
    elif getset("file", "type") != {"table"}:
        msg = "Cannot combine non-table files"
        return msg, dict(types=getset("file", "type"))
    elif len(getset("file", "index_name")) > 1:
        msg = "Cannot combine tables with conflicting index names"
        return msg, dict(index_names=getset("file", "index_name"))
    else:
        return None, {}


def file_redirect(descriptors):
    """Redirect to file at original URL, without caching or interpreting"""
    if len(descriptors) == 1:
        name = descriptors[0]["file"]["filename"]
        urls = descriptors[0]["file"].get("urls", ())
        with pick_reachable_url(urls, name=name) as url:
            return redirect(url, code=303, Response=Response)
    else:
        raise GeneFabFileException(
            ("Multiple files match query; " +
            "with format 'raw', only one file can be requested"),
            format="raw", files={d["file"]["filename"] for d in descriptors},
        )


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
    column_order, harmonized_column_order = harmonize_columns(
        dataframe, descriptor, all_sample_names, best_sample_name_matches,
    )
    dataframe = dataframe[column_order]
    dataframe.columns = harmonized_column_order


def get_formatted_data(descriptor, mongo_collections, sqlite_db, CachedFile, adapter, _kws):
    """Instantiate and initialize CachedFile object; post-process its data; select only the columns in passed annotation"""
    try:
        accession, assay = descriptor["accession"], descriptor["assay"]
        filename = descriptor["file"]["filename"]
    except (KeyError, TypeError, IndexError):
        msg = "File descriptor missing 'accession', 'assay', or 'filename'"
        raise GeneFabDatabaseException(msg, descriptor=descriptor)
    else:
        identifier = f"{accession}/File/{assay}/{filename}"
    if "INPLACE_process" in _kws:
        _kws = {**_kws, "INPLACE_process": partial(
            INPLACE_process_dataframe, descriptor=descriptor,
            best_sample_name_matches=adapter.best_sample_name_matches,
            mongo_collections=mongo_collections,
        )}
    file = CachedFile(
        identifier=identifier,
        name=filename, urls=descriptor["file"].get("urls", ()),
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
            (accession, assay, column) for column in harmonized_column_order
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
        raise NotImplementedError("Merging non-dataframe data objects")
    if isinstance(combined, OndemandSQLiteDataFrame):
        data = combined.get( # TODO: get() arguments
            limit=1 if (context.schema == "1") else limit,
        )
        if data.index.name is None:
            data.index.name = "index" # best we can do
        data.reset_index(inplace=True, col_level=-1, col_fill="*")
        return data
    else:
        return combined


def combined_data(descriptors, context, mongo_collections, sqlite_dbs, adapter):
    """Patch through to cached data for each file and combine them"""
    if len(descriptors) > 1:
        msg, _kw = validate_joinable_files(descriptors)
        if msg:
            raise GeneFabFileException(msg, **_kw)
    getset = lru_cache(maxsize=None)(lambda *keys: set(
        reduce(lambda d, k: d.get(k, {}), keys, d) or None for d in descriptors
    ))
    if getset("file", "cacheable") != {True}:
        msg = "Cannot combine these data as they were not marked cacheable"
        raise NotImplementedError(msg)
    types = getset("file", "type")
    if types == {"table"}:
        sqlite_db, CachedFile = sqlite_dbs.tables, CachedTableFile
        _kws = dict(index_col=0, INPLACE_process=True)
    elif len(types) == 1:
        sqlite_db, CachedFile, _kws = sqlite_dbs.blobs, CachedBinaryFile, {}
    else:
        raise NotImplementedError(f"Joining data of types {types}")
    data = combine_objects(context=context, objects=[
        get_formatted_data(
            descriptor, mongo_collections, sqlite_db, CachedFile, adapter, _kws,
        )
        for descriptor in natsorted(
            descriptors, key=lambda d: (d.get("accession"), d.get("assay")),
        )
    ])
    _dtp = getset("file", "datatype")
    data.datatypes = _dtp
    return data


def get(mongo_collections, *, locale, context, sqlite_dbs, adapter):
    """Return data corresponding to search parameters; merged if multiple underlying files are same type and joinable"""
    descriptors = get_file_descriptors(
        mongo_collections, locale=locale, context=context,
    )
    for d in descriptors:
        if ("file" not in d) or ("filename" not in d["file"]):
            msg = "File information missing for entry"
            raise GeneFabDatabaseException(msg, entry=d)
    if not len(descriptors):
        msg = "No file found matching specified constraints"
        raise HTTPError(context.full_path, 404, msg, hdrs=None, fp=None)
    elif context.format == "raw":
        return file_redirect(descriptors)
    else:
        return combined_data(
            descriptors, context, mongo_collections, sqlite_dbs, adapter,
        )
