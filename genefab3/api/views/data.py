from genefab3.db.mongo.utils import retrieve_by_context
from functools import lru_cache, reduce
from genefab3.db.sql.types import CachedBinaryFile, CachedTableFile
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDatabaseException
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
        CachedFile = CachedTableFile
    elif len(types) == 1:
        CachedFile = CachedBinaryFile
    else:
        raise NotImplementedError(f"Joining data of types {types}")
    objects = []
    for d in descriptors:
        name = d["file"]["filename"]
        urls = d["file"].get("urls", ())
        with pick_reachable_url(urls, name=name) as url:
            objects.append(
                #CachedFile(
                dict(
                    name=name, sqlite_db=sqlite_dbs.blobs,
                    url=url, timestamp=d["file"].get("timestamp", -1),
                )
            )
    print(objects)
    return objects
"""
            if descriptor.get("cacheable") == True:
                cached_object_kwargs = dict(
                    name=descriptor["filename"], sqlite_db=sqlite_dbs.blobs,
                    url=url, timestamp=descriptor.get("timestamp", -1),
                )
                if descriptor.get("type") == "table":
                    file = CachedTableFile(**cached_object_kwargs)
                else:
                    file = CachedBinaryFile(**cached_object_kwargs)
            else:
"""


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
    elif context.format == "html": # TODO temporary
        from json import dumps
        return "<pre>"+dumps(descriptors, sort_keys=True, indent=4)
    elif context.format == "raw":
        return file_redirect(descriptors)
    else:
        return combined_data(descriptors, sqlite_dbs)
