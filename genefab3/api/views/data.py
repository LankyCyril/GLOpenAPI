from genefab3.db.mongo.utils import retrieve_by_context
from functools import lru_cache, reduce, partial
from genefab3.common.utils import pick_reachable_url, set_attributes
from flask import redirect, Response
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDataManagerException
from pandas import MultiIndex
from genefab3.db.sql.types import OndemandSQLiteDataFrame
from genefab3.db.sql.types import CachedTableFile, CachedBinaryFile
from natsort import natsorted
from genefab3.common.exceptions import GeneFabDatabaseException


TECH_TYPE_LOCATOR = "investigation.study assays", "study assay technology type"


def get_file_descriptors(mongo_collections, *, locale, context):
    """Return DataFrame of file descriptors that match user query"""
    context.projection["file"] = True
    context.update(".".join(TECH_TYPE_LOCATOR))
    descriptors, _ = retrieve_by_context(
        mongo_collections.metadata, locale=locale, context=context,
        include={"id.sample name"}, postprocess=[
            {"$group": {
                "_id": {
                    "accession": "$id.accession",
                    "assay": "$id.assay",
                    "technology type": "$"+".".join(TECH_TYPE_LOCATOR),
                    "file": "$file",
                },
                "sample name": {"$addToSet": "$id.sample name"},
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


def INPLACE_process_dataframe(dataframe, *, descriptor, best_sample_name_matches):
    """Harmonize index name, column names, reorder sample columns to match annotation"""
    if not dataframe.index.name:
        dataframe.index.name = descriptor["file"].get("index_name", "index")
    sample_names = natsorted(descriptor.get("sample name", ()))
    harmonized_columns, harmonized_positions = [], []
    for i, c in enumerate(dataframe.columns):
        hcs, ps = best_sample_name_matches(
            c, sample_names, return_positions=True,
        )
        if len(hcs) == 0:
            harmonized_columns.append(c)
        elif len(hcs) == 1:
            harmonized_columns.append(hcs[0])
            harmonized_positions.append((ps[0], i))
        else:
            msg = "Column name matches multiple sample names"
            filename = descriptor["file"].get("filename")
            _kws = dict(filename=filename, column=c, sample_names=hcs)
            raise GeneFabDataManagerException(msg, **_kws)
    if descriptor.get("column_subset") == "sample name":
        if len(harmonized_positions) != len(sample_names):
            msg = "Data columns do not match sample names 1-to-1"
            _kws_a = dict(filename=descriptor["file"].get("filename"))
            _kws_b = dict(columns=harmonized_columns, sample_names=sample_names)
            raise GeneFabDataManagerException(msg, **_kws_a, **_kws_b)
    unordered = harmonized_columns[:]
    current_positions = [i for p, i in harmonized_positions]
    target_positions = [i for p, i in sorted(harmonized_positions)]
    for cp, tp in zip(current_positions, target_positions):
        harmonized_columns[tp] = unordered[cp]
    dataframe.columns = harmonized_columns


def get_formatted_data(descriptor, sqlite_db, CachedFile, adapter, _kws):
    """Instantiate and initialize CachedFile object; post-process its data"""
    accession = descriptor.get("accession", "NO_ACCESSION")
    assay = descriptor.get("assay", "NO_ASSAY")
    name = descriptor["file"]["filename"]
    identifier = f"{accession}/File/{assay}/{name}"
    if "INPLACE_process" in _kws:
        _kws = {**_kws, "INPLACE_process": partial(
            INPLACE_process_dataframe, descriptor=descriptor,
            best_sample_name_matches=adapter.best_sample_name_matches,
        )}
    file = CachedFile(
        identifier=identifier,
        name=name, urls=descriptor["file"].get("urls", ()),
        timestamp=descriptor["file"].get("timestamp", -1),
        sqlite_db=sqlite_db, **_kws,
    )
    data = file.data
    if isinstance(data, OndemandSQLiteDataFrame):
        data.columns = MultiIndex.from_tuples((
            (accession, assay, column) for column in data.columns
        ))
    return data


def combine_objects(objects):
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
        data = combined.get() # TODO: get() arguments
        if data.index.name is None:
            data.index.name = "index" # best we can do
        data.reset_index(inplace=True, col_level=-1, col_fill="*")
        set_attributes(data, object_type="datatable")
        return data
    else:
        return combined


def combined_data(descriptors, sqlite_dbs, adapter):
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
    data = combine_objects([
        get_formatted_data(d, sqlite_db, CachedFile, adapter, _kws)
        for d in natsorted(
            descriptors, key=lambda d: (d.get("accession"), d.get("assay")),
        )
    ])
    set_attributes(
        data, datatypes=getset("file", "datatype"),
        accessions=getset("accession"),
    )
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
        raise FileNotFoundError("No file found matching specified constraints")
    elif context.format == "raw":
        return file_redirect(descriptors)
    else:
        return combined_data(descriptors, sqlite_dbs, adapter)
