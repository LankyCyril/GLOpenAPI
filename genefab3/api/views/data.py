from genefab3.db.mongo.utils import retrieve_by_context
from functools import lru_cache, reduce
from genefab3.db.sql.types import CachedBinaryFile, CachedTableFile
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDatabaseException
from pandas import DataFrame, MultiIndex, concat
from genefab3.common.utils import pick_reachable_url
from flask import redirect, Response


TECH_TYPE_LOCATOR = "investigation.study assays", "study assay technology type"


def get_file_descriptors(mongo_collections, *, locale, context):
    """Return DataFrame of file descriptors that match user query"""
    context.projection["file"] = True
    context.update(".".join(TECH_TYPE_LOCATOR))
    descriptors, _ = retrieve_by_context(
        mongo_collections.metadata, locale=locale, context=context,
        include={"info.sample name"}, postprocess=[
            {"$group": {
                "_id": {
                    "accession": "$info.accession",
                    "assay": "$info.assay",
                    "technology type": "$"+".".join(TECH_TYPE_LOCATOR),
                    "file": "$file",
                },
                "sample name": {"$addToSet": "$info.sample name"},
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


def get_formatted_data(descriptor, sqlite_db, CachedFile, _kws):
    """Instantiate and initialize CachedFile object; post-process its data"""
    accession = descriptor.get("accession", "NO_ACCESSION")
    assay = descriptor.get("assay", "NO_ASSAY")
    name = descriptor["file"]["filename"]
    identifier = f"{accession}/File/{assay}/{name}"
    file = CachedFile(
        identifier=identifier,
        name=name, urls=descriptor["file"].get("urls", ()),
        timestamp=descriptor["file"].get("timestamp", -1),
        sqlite_db=sqlite_db, **_kws,
    )
    data = file.data
    if isinstance(data, DataFrame):
        if not data.index.name:
            data.index.name = descriptor["file"].get("index_name", "index")
        data.columns = MultiIndex.from_tuples((
            (accession, assay, column) for column in data.columns
            # TODO: infer correct sample names here
        ))
    return data


def combine_dataframes(dataframes):
    """Combine dataframes in-memory; this faster than sqlite3: wesmckinney.com/blog/high-performance-database-joins-with-pandas-dataframe-more-benchmarks"""
    dataframe = concat(dataframes, axis=1, sort=False)
    dataframe.reset_index(inplace=True, col_level=-1, col_fill="*")
    return dataframe


def combined_data(descriptors, sqlite_dbs):
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
        sqlite_db = sqlite_dbs.tables
        CachedFile, _kws = CachedTableFile, dict(index_col=0)
        combine = combine_dataframes
    elif len(types) == 1:
        sqlite_db, CachedFile, _kws = sqlite_dbs.blobs, CachedBinaryFile, {}
        combine = next
    else:
        raise NotImplementedError(f"Joining data of types {types}")
    return combine(
        get_formatted_data(d, sqlite_db, CachedFile, _kws)
        for d in descriptors
    )


def get(mongo_collections, *, locale, context, sqlite_dbs):
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
        return combined_data(descriptors, sqlite_dbs)
