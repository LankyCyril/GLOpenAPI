from functools import lru_cache
from pandas import Series
from genefab3.db.sql.types import CachedBinaryFile, CachedTableFile
from genefab3.common.exceptions import GeneFabParserException
from genefab3.api.views.file import get_descriptor_dataframe
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.utils import pick_reachable_url
from flask import redirect, Response


TECH_TYPE_LOCATOR = "investigation.study assays", "study assay technology type"


def validate_joinable_files(descriptor_dataframe):
    """Check for ability to join data from requested files"""
    getset = lru_cache(maxsize=None)(lambda col, default=None:
        set(descriptor_dataframe.get(col, Series(default)).drop_duplicates())
    )
    if len(getset(("datatype"))) > 1:
        msg = "Cannot combine data of multiple datatypes"
        return msg, dict(datatypes=getset(("datatype")))
    elif len(getset(TECH_TYPE_LOCATOR)) > 1:
        msg = "Cannot combine data for multiple technology types"
        return msg, dict(technology_types=getset(TECH_TYPE_LOCATOR))
    elif getset(("joinable")) != {True}:
        return "Cannot combine multiple files of this datatype", dict(
            datatype=getset(("datatype")).pop(),
            filenames=getset(("filename")),
        )
    elif getset(("type")) != {"table"}:
        msg = "Cannot combine non-table files"
        return msg, dict(types=getset(("type")))
    elif len(getset(("index_name"))) > 1:
        msg = "Cannot combine tables with conflicting index names"
        return msg, dict(index_names=getset(("index_name")))
    else:
        return None, {}


def get_sql_manager(files, context):
    """Select an SQLiteObject type suitable for passed files (CachedTableFile for tables, CachedBinaryFile for everything else)"""
    types = {f.get("type") for f in files}
    if types == {"table"}:
        if context.format != "raw":
            return CachedTableFile
        else:
            msg = "Cannot represent table data in format 'raw'"
            _kw = {"type": "table", "format": context.format}
            raise GeneFabParserException(msg, **_kw)
    elif len(types) == 1:
        if context.format == "raw":
            return CachedBinaryFile
        else:
            msg = "Cannot represent non-table data in non-raw format"
            _kw = {"type": "table", "format": context.format}
            raise GeneFabParserException(msg, **_kw)
    else:
        raise NotImplementedError("Joining data of type(s)", types=types)


def combined_cached_data(files, context):
    """Patch through to cached data for each file and combine them"""
    CachedFile = get_sql_manager(files, context)
    objects = [
    ]


def squash_sample_names(descriptor_dataframe):
    """Collapse sample names belonging to same assay and file into lists"""
    cols = [c for c in descriptor_dataframe if c != ("info", "sample name")]
    squashed = descriptor_dataframe.groupby(cols, as_index=False).agg(list)
    squashed.columns = [c[1] if c[0] == "file" else c for c in squashed]
    return squashed


def get(mongo_collections, *, locale, context, sqlite_dbs):
    """Return data corresponding to search parameters; merged if multiple underlying files are same type and joinable"""
    descriptor_dataframe = squash_sample_names(
        get_descriptor_dataframe(
            mongo_collections, locale=locale, context=context,
            project_info=True, include={TECH_TYPE_LOCATOR},
        ),
    )
    print(descriptor_dataframe.T)
    if descriptor_dataframe.empty:
        raise FileNotFoundError("No file found matching specified constraints")
    elif "filename" not in descriptor_dataframe:
        raise FileNotFoundError("No file found matching specified constraints")
    elif len(descriptor_dataframe) > 1:
        msg, _kw = validate_joinable_files(descriptor_dataframe)
        if msg:
            raise GeneFabFileException(msg, **_kw)
    files = descriptor_dataframe.to_dict(orient="records")
    if {f.get("cacheable") for f in files} != {True}:
        if len(files) == 1:
            if context.format == "raw":
                urls = files[0].get("urls", ())
                with pick_reachable_url(urls, name=files[0]["filename"]) as url:
                    return redirect(url, code=303, Response=Response)
            else:
                msg = "Cannot represent non-cacheable file in format '{}'"
                raise NotImplementedError(msg.format(context.format))
        else:
            msg = "Cannot combine these data as they were not marked cacheable"
            raise NotImplementedError(msg)
    else:
        return combined_cached_data(files, context)
    return repr(files)

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
